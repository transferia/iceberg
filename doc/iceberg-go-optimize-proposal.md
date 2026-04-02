# Feature Request: `Optimize` / Table Compaction API for iceberg-go

## Summary

Request a high-level `Optimize` (or `RewriteDataFiles`) API that compacts Iceberg tables by rewriting data files to physically remove rows matched by equality delete files. This is critical for CDC (Change Data Capture) workloads where equality deletes accumulate and degrade read performance.

## Problem

Equality deletes (merge-on-read) are the correct write strategy for CDC — they're fast and don't rewrite data files on every UPDATE/DELETE. However, **read cost grows unboundedly** as delete files accumulate:

| State | Data Files | Delete Files | Eq Deletes | `COUNT(*)` Scan | Slowdown |
|---|---|---|---|---|---|
| After snapshot (clean) | 1 | 0 | 0 | **41ms** | 1x |
| After 1K DML | 2 | 1 | 200 | 230ms | 5.6x |
| After 5K DML | 3 | 2 | 500 | 165ms | 4x |
| After 10K DML | 4 | 3 | 1,000 | **5,087ms** | **124x** |

The merge-on-read overhead is O(data_files × delete_files × rows_per_file) because equality delete files must be matched against **every** data file.

### File Statistics

Both data sources already track the necessary metrics:

**Snapshot Summary** (from `Snapshot.Summary.Properties`):
```
total-data-files:       4
total-delete-files:     3
total-equality-deletes: 1000
total-records:          1900
```

**PlanFiles** (from `Scan().PlanFiles()`):
```
FileScanTask{
    File:                <data file>,
    EqualityDeleteFiles: [<delete file 1>, <delete file 2>],  // "dirty" if non-empty
}
```

After 1K DML on a 1K-row table: **75% of data files are dirty** (have associated equality delete files).

## Proposed API

### Option A: High-level `Optimize`

```go
// Optimize rewrites data files to remove rows matched by equality/position deletes.
// After optimization, the table has no delete files and reads are at baseline speed.
func (t *Transaction) Optimize(ctx context.Context, opts ...OptimizeOption) error

// Options
func WithTargetFileSizeBytes(size int64) OptimizeOption  // target output file size
func WithOptimizeConcurrency(n int) OptimizeOption       // parallel file rewriting
func WithPartitionFilter(filter BooleanExpression) OptimizeOption // only compact matching partitions
```

Usage:
```go
tx := tbl.NewTransaction()
err := tx.Optimize(ctx, WithTargetFileSizeBytes(128*1024*1024))
_, err = tx.Commit(ctx)
```

### Option B: Lower-level building blocks (if high-level is too opinionated)

The primitives already exist — what's missing is the glue:

1. **`PlanFiles()`** — identifies dirty files ✅ exists
2. **`ToArrowRecords()`** — reads with merge-on-read (clean output) ✅ exists
3. **`WriteRecords()`** — writes new Parquet data files ✅ exists
4. **`ReplaceDataFilesWithDataFiles()`** — atomic file swap ✅ exists

What's missing:
- **Per-file scan with delete application**: Read a specific data file and apply its equality deletes, yielding clean records. Currently `ToArrowRecords()` scans the whole table.
- **`TableStats()` or `FileGroupStats()`**: Quick summary of dirty files without calling `PlanFiles()` (which reads manifests).

### Option C: `RewriteDataFiles` (matching Spark's API)

```go
// RewriteDataFiles compacts data files, optionally filtering by partition.
// Equivalent to Spark's `CALL system.rewrite_data_files(table => '...')`
func (t *Transaction) RewriteDataFiles(ctx context.Context, opts ...RewriteOption) (RewriteResult, error)

type RewriteResult struct {
    RewrittenDataFiles  int
    AddedDataFiles      int  
    RemovedDeleteFiles  int
    RemovedEqDeletes    int64
}
```

## What I've Built So Far (workaround)

Using existing iceberg-go APIs, I can implement compaction as:

```go
func CompactTable(ctx context.Context, tbl *table.Table) error {
    // 1. Check if compaction is needed
    tasks, _ := tbl.Scan().PlanFiles(ctx)
    var filesToDelete []iceberg.DataFile
    needsCompaction := false
    for _, task := range tasks {
        filesToDelete = append(filesToDelete, task.File)
        if len(task.EqualityDeleteFiles) > 0 {
            needsCompaction = true
        }
    }
    if !needsCompaction {
        return nil
    }

    // 2. Full scan with merge-on-read → clean records
    schema, records, _ := tbl.Scan().ToArrowRecords(ctx)

    // 3. Write clean records as new data files
    newFiles := table.WriteRecords(ctx, tbl, schema, records)

    // 4. Atomic replace
    tx := tbl.NewTransaction()
    tx.ReplaceDataFilesWithDataFiles(ctx, filesToDelete, newFiles, nil)
    _, err := tx.Commit(ctx)
    return err
}
```

**Limitation**: This rewrites the entire table. For large tables, we need per-file or per-partition compaction — which requires reading individual data files with their associated delete files applied, not the full table scan.

## Use Case

**CDC (Change Data Capture) replication from PostgreSQL to Iceberg**

- PostgreSQL WAL produces INSERT/UPDATE/DELETE events
- UPDATEs → equality delete + new data row (RowDelta)
- DELETEs → equality delete only
- At 5K+ rows/sec with 10% UPDATE/DELETE rate, delete files accumulate to ~50+ in 5 minutes
- Read queries (dashboards, analytics) become unusably slow

**Expected outcome after `Optimize`**: Scan time returns to baseline regardless of how many mutations occurred since last compaction.

## Environment

- iceberg-go version: latest (commit from `main` as of 2026-04-01)
- Format version: 2
- Catalog: REST
- Storage: S3 (MinIO)
- Test repo with benchmarks: https://github.com/transferia/iceberg (branch `feat/cdc-replication-sink`)
  - `tests/bench/equality_delete_perf_test.go` — reproduces the degradation
  - `tests/bench/table_stats_test.go` — demonstrates file stats collection
  - `doc/equality-delete-performance.md` — full analysis
  - `doc/compaction-research.md` — strategies comparison
