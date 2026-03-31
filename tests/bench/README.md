# CDC Replication Benchmark: Postgres → Iceberg

Measures sustained CDC replication throughput from PostgreSQL to Apache Iceberg using native v2 equality deletes and RowDelta commits.

## Architecture

```
┌──────────────┐     WAL      ┌──────────────────┐    Parquet     ┌─────────┐
│   Postgres   │ ──────────── │  SinkReplication  │ ────────────── │  MinIO  │
│  (source)    │   CDC events │  (transferia)     │   data files   │  (S3)   │
└──────────────┘              └──────────────────┘                └─────────┘
       ▲                              │                               │
       │                              │ RowDelta commit               │
  Load Generator                      ▼                               │
  (Go goroutines)             ┌──────────────────┐                    │
  INSERT/UPDATE/DELETE        │  Iceberg REST     │◄───────────────────┘
                              │  Catalog          │   manifest + metadata
                              └──────────────────┘
```

All services run via `docker-compose` (see `recipe/docker-compose.yml`).

## Load Profiles

| Profile | INSERT | UPDATE | DELETE | Use case |
|---|---|---|---|---|
| **InsertOnly** | 100% | 0% | 0% | Pure append throughput (streaming/logging) |
| **InsertHeavy** | 90% | 5% | 5% | Event-driven CDC with corrections |
| **Balanced** | 60% | 30% | 10% | OLTP order management system |

Each profile uses the same ramp-up strategy:
- **Start**: 1,000 rows/sec
- **Ramp**: Linear increase over 2.5 minutes
- **Peak**: 10,000 rows/sec
- **Hold**: Sustained at peak for 2.5 minutes
- **Total duration**: ~5 minutes of load + 30s drain

## Table Schema

Simulates a realistic OLTP orders table:

```sql
CREATE TABLE bench_orders (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL,
    status     VARCHAR(32) NOT NULL,    -- pending/confirmed/shipped/delivered/cancelled
    amount     NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

## Metrics Collected

Every 10 seconds during the benchmark:

| Metric | Description |
|---|---|
| `PG rows written` | Cumulative DML operations executed on Postgres |
| `Iceberg rows` | Current row count in Iceberg table (via table scan) |
| `Lag (rows)` | `PG ops - Iceberg rows` (approximate for mixed DML) |
| `Rate (rows/sec)` | Current write throughput on Postgres |

Final summary includes peak rate, average rate, and total counts broken down by INSERT/UPDATE/DELETE.

## How to Run

### Prerequisites

```bash
# Start infrastructure (Postgres + Iceberg REST + MinIO)
make recipe
```

### Quick smoke test (~30 seconds)

```bash
AWS_S3_ENDPOINT=http://localhost:9000 \
CATALOG_ENDPOINT=http://localhost:8181 \
  go test -run TestBenchmarkSmoke -timeout=5m -v ./tests/bench/
```

### Single profile (~6 minutes)

```bash
AWS_S3_ENDPOINT=http://localhost:9000 \
CATALOG_ENDPOINT=http://localhost:8181 \
  go test -run TestBenchmarkInsertHeavy -timeout=30m -v ./tests/bench/
```

### All profiles (~20 minutes)

```bash
AWS_S3_ENDPOINT=http://localhost:9000 \
CATALOG_ENDPOINT=http://localhost:8181 \
  go test -run TestBenchmarkAll -timeout=60m -v ./tests/bench/
```

### Custom configuration

Edit `DefaultConfig()` in `load_generator.go` to adjust:
- `RateStart` / `RateEnd` — write rate bounds
- `RampDuration` / `HoldDuration` — timing
- `Workers` — concurrent PG writer goroutines

## Benchmark Results (Apple M1 Pro, Docker/Colima, local MinIO)

### Smoke test (InsertOnly, 100→500 rows/sec, 20s)

```
[10s] PG:2969  Iceberg:1667  Lag:1302  Rate:296/s
[20s] PG:7896  Iceberg:6403  Lag:1493  Rate:492/s
[30s] PG:7896  Iceberg:7896  Lag:0     Rate:0/s

=== Benchmark Results ===
Profile:           InsertOnly
Duration:          20s
PG rows written:   7,896 (I:7,896 U:0 D:0)
Iceberg rows:      7,896
Replication lag:   0 rows
Peak write rate:   492 rows/sec
Avg write rate:    394 rows/sec
========================
```

Key observations:
- **Zero data loss**: Iceberg rows match PG rows exactly (7,896 = 7,896)
- **Lag catches up**: ~1,500 row lag during load, drops to 0 within 10s after load stops
- **Commit interval**: 5s flush interval produces ~4 commits during 20s load window

## What to Look For

- **Lag stability**: Lag should stay bounded, not grow unboundedly. If lag keeps growing, the sink can't keep up.
- **File count**: Check MinIO console (http://localhost:9001, admin/password) for the `warehouse` bucket. Too many small files indicates commit interval is too aggressive.
- **Memory**: Monitor Go process memory. The 64MB buffer cap should prevent OOM.
- **Equality delete ratio**: For Balanced profile, ~40% of operations produce delete files. Watch for delete file accumulation (indicates need for compaction).
