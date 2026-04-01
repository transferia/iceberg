# Equality Delete Performance Analysis

## Context

The CDC replication sink uses Iceberg v2 **equality deletes** (merge-on-read) for UPDATE and DELETE operations. This avoids rewriting data files on every mutation — deletes are written as small delete files that are applied at read time by the scanner.

The trade-off: writes are fast, but **reads degrade as delete files accumulate**.

This document measures the degradation and discusses mitigation strategies.

## Test Setup

- **Table**: 10,000 rows, 3 columns (`id BIGINT PK, value INT, label VARCHAR`)
- **Workload**: 10 rounds, each round = 500 UPDATEs + 500 DELETEs (with re-INSERTs to keep row count stable)
- **Commit interval**: 2 seconds (each round produces multiple commits)
- **Measurement**: Full table scan via `Scan().ToArrowRecords()` — equivalent to `SELECT COUNT(*)` with merge-on-read
- **Environment**: Apple M1 Pro, Docker/Colima, local MinIO + REST catalog

## Results

| Round | Cumulative DML | Scan Time | Slowdown vs Baseline |
|-------|----------------|-----------|----------------------|
| 0     | 0              | **41ms**  | 1.0x                 |
| 1     | 1,000          | 230ms     | 5.6x                 |
| 2     | 2,000          | 171ms     | 4.2x                 |
| 3     | 3,000          | 147ms     | 3.6x                 |
| 4     | 4,000          | 125ms     | 3.0x                 |
| 5     | 5,000          | 165ms     | 4.0x                 |
| 6     | 6,000          | 207ms     | 5.0x                 |
| 7     | 7,000          | 1,157ms   | 28.2x                |
| 8     | 8,000          | 190ms     | 4.6x                 |
| 9     | 9,000          | 988ms     | 24.1x                |
| 10    | 10,000         | **5,087ms** | **124x**           |

## Analysis

### Why the non-linear degradation?

The scan overhead is not strictly proportional to the number of delete files. The spikes at rounds 7, 9, and 10 correlate with:

1. **Number of equality delete files**: Each commit with deletes produces a separate delete file. After 10 rounds with 2s commit interval, there are ~50+ delete files.

2. **Cross-file matching**: Each equality delete file must be matched against **every data file** in the table. With N data files and M delete files, the merge cost is O(N × M × rows_per_file).

3. **No file-level pruning for equality deletes**: Unlike position deletes (which reference specific files), equality deletes require checking every data file because any file might contain the deleted key.

### What happens during a scan

```
For each data file:
  1. Read all rows from the Parquet file
  2. For each equality delete file with overlapping partition:
     a. Read all delete keys
     b. For each row in the data file:
        - Check if PK matches any delete key
        - If yes, filter out the row
  3. Yield remaining rows
```

At 10K DML ops, this means:
- ~10 data files (from commits) × ~50 delete files × 10K rows = **5M comparisons**

### Comparison: Write vs Read overhead

| Operation | Without equality deletes | With equality deletes |
|-----------|------------------------|-----------------------|
| **INSERT 10K rows** | WriteRecords → 1 Parquet file | Same |
| **UPDATE 1K rows** | N/A | WriteRecords + WriteEqualityDeletes → 2 files per commit |
| **DELETE 1K rows** | N/A | WriteEqualityDeletes → 1 delete file per commit |
| **COUNT(*)** | Scan ~10 data files: **41ms** | Scan ~10 data + ~50 delete files: **5,087ms** |

## Mitigation: Compaction

The standard solution is **compaction** — periodically rewriting data files to physically remove rows matched by equality deletes.

### How compaction helps

```
Before compaction:
  10 data files + 50 equality delete files
  Scan cost: O(data_files × delete_files × rows)

After compaction:
  1 data file + 0 delete files
  Scan cost: O(rows)
```

### Compaction strategies

1. **Time-based**: Compact every N minutes or when delete file count exceeds a threshold
2. **File-count-based**: Trigger when `num_delete_files / num_data_files > ratio`
3. **Read-time-based**: Compact when scan latency exceeds a target SLA
4. **External**: Use Spark/Trino `CALL rewrite_data_files()` as a scheduled job

### Compaction in iceberg-go

As of the current iceberg-go version, compaction is not yet implemented in the Go library. Options:

- **Spark**: `CALL iceberg.system.rewrite_data_files(table => 'public.my_table')`
- **Trino**: `ALTER TABLE my_table EXECUTE optimize`
- **Custom Go**: Read all files, merge/filter, write new data files, commit via `ReplaceDataFilesWithDataFiles`

### Recommended approach for transferia-iceberg

1. **Short-term**: Run Spark/Trino compaction as a scheduled job (every 15-30 min for CDC tables)
2. **Medium-term**: Add a `CompactTable` method to the sink that runs after N commits
3. **Long-term**: Implement native Go compaction using `tx.ReplaceDataFilesWithDataFiles()`

## Reproducing

```bash
make recipe
bash tests/run_repl_test.sh TestEqualityDeleteReadPerf ./tests/bench/ 30m
```

The test is in `tests/bench/equality_delete_perf_test.go`.
