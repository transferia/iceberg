# Demo: PostgreSQL CDC to Iceberg v2

Real-time Change Data Capture replication from PostgreSQL to Apache Iceberg v2 tables — entirely in Go, no JVM.

INSERT, UPDATE, and DELETE operations are captured from the PostgreSQL WAL and written to Iceberg using v2 equality deletes (merge-on-read).

## Prerequisites

- Docker / Docker Compose
- Go 1.23+

## Quick Start

### 1. Start infrastructure

```bash
cd demo
docker compose up -d
```

This starts:
- **PostgreSQL** (port 5432) — source database with WAL-level replication
- **MinIO** (ports 9000/9001) — S3-compatible storage for Iceberg data files
- **Iceberg REST Catalog** (port 8181) — Iceberg table metadata

### 2. Seed the source database

```bash
psql "host=localhost port=5432 user=postgres password=postgres dbname=demo" -f seed.sql
```

### 3. Build and start replication

```bash
# From the repo root
make build
./binaries/trcli activate --transfer demo/transfer.yaml --log-level info
```

The transfer will:
1. **Snapshot** the existing `orders` table into Iceberg
2. **Switch to CDC** — streaming WAL changes in real-time

### 4. Generate changes

In another terminal, run DML against PostgreSQL:

```bash
psql "host=localhost port=5432 user=postgres password=postgres dbname=demo" -f demo/workload.sql
```

Or run ad-hoc SQL:

```bash
psql "host=localhost port=5432 user=postgres password=postgres dbname=demo"

INSERT INTO orders (customer, product, quantity, price) VALUES ('zara', 'widget-z', 1, 9.99);
UPDATE orders SET status = 'delivered' WHERE customer = 'zara';
DELETE FROM orders WHERE customer = 'frank';
```

### 5. Observe

**MinIO Console** — browse the Iceberg data and delete files:
```
http://localhost:9001
Login: admin / password
Bucket: warehouse → public/orders/
```

You'll see:
- `data/` — Parquet data files (from INSERTs and snapshot)
- `data/` — Equality delete files (from UPDATEs and DELETEs, also Parquet, containing just the PK values)
- `metadata/` — Iceberg table metadata, manifests, and snapshots

**REST Catalog** — check table exists:
```bash
curl -s http://localhost:8181/v1/namespaces/public/tables | jq .
```

### 6. Cleanup

```bash
docker compose down -v
```

## How It Works

```
PostgreSQL WAL
    │
    ▼
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  PG Source   │────▶│  SinkReplication  │────▶│  Iceberg v2     │
│  (CDC)       │     │  (Go)            │     │  (Parquet + S3) │
└─────────────┘     └──────────────────┘     └─────────────────┘
                           │
                    ┌──────┴──────┐
                    │             │
               INSERT/UPDATE   DELETE
                    │             │
                    ▼             ▼
              WriteRecords   WriteEqualityDeletes
              (data file)    (delete file with PK)
                    │             │
                    └──────┬──────┘
                           ▼
                      RowDelta Commit
                    (atomic transaction)
```

- **INSERT** → appended as a new data row
- **UPDATE** → equality delete (old PK) + new data row (new values), committed atomically via RowDelta
- **DELETE** → equality delete file containing the deleted PK

All mutations within a commit interval (default 5s) are batched and deduplicated by PK before writing.

## Performance

| Metric | Value |
|--------|-------|
| Peak throughput | 6,400 rows/sec |
| Avg throughput | 4,500 rows/sec |
| Replication lag | ~3 seconds |
| Data loss | 0 (exact row count match) |

Measured with 1.35M rows over 5 minutes. See [benchmark results](../tests/bench/README.md).

## Known Limitations

1. **Equality delete read overhead** — scan performance degrades as delete files accumulate (124x slower after 10K DML ops). Requires periodic compaction. See [performance analysis](../doc/equality-delete-performance.md) and [compaction research](../doc/compaction-research.md).

2. **No built-in compaction** — iceberg-go doesn't have an `Optimize` API yet. Workaround: use Spark `CALL system.rewrite_data_files()` or the full-table scan+rewrite approach described in [compaction research](../doc/compaction-research.md).

3. **Single-writer** — concurrent replication workers writing to the same table will hit `CommitFailedException`. The sink invalidates the table cache on failure and retries, but throughput drops.

4. **No schema evolution** — if the source schema changes, the Iceberg table schema is not updated automatically. The table must be recreated.

5. **Append-only without PK** — tables without a primary key are treated as append-only. UPDATEs and DELETEs are dropped since equality deletes require a PK.

## Further Reading

- [Equality delete performance analysis](../doc/equality-delete-performance.md) — why reads degrade and the numbers
- [Compaction research](../doc/compaction-research.md) — strategies to keep read cost constant
- [Optimize API proposal for iceberg-go](../doc/iceberg-go-optimize-proposal.md) — feature request draft
- [Benchmark README](../tests/bench/README.md) — full benchmark methodology and results
