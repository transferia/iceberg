# Compaction Research: Constant-Time Reads with CDC

## Problem Statement

After 10K DML operations on a 10K-row table, `COUNT(*)` scan degrades from **41ms to 5,087ms** (124x slower). The cost of reading grows with the number of accumulated equality delete files. For production CDC workloads, read cost must be roughly constant regardless of how many mutations have occurred.

## Root Cause

Merge-on-read equality deletes work as follows:

```
For each data file:
  For each equality delete file:
    For each row in data file:
      Check if row PK matches any delete key
      If match → filter out
```

Cost: **O(data_files × delete_files × rows_per_file)**

After N commits with deletes, there are ~N delete files. Each scan must process all of them against all data files. This is fundamentally unbounded.

## Solution: Compaction

Compaction rewrites data files to physically remove deleted rows, eliminating the delete files.

```
Before: 10 data files + 50 delete files → scan in 5s
After:  1 data file + 0 delete files   → scan in 41ms
```

## Available APIs in iceberg-go

### 1. `tx.ReplaceDataFilesWithDataFiles(ctx, filesToDelete, filesToAdd, props)`

**The primary compaction API.** Atomically replaces a set of data files with new ones.

```go
// Read current files + equality deletes → write compacted files → replace
tx := tbl.NewTransaction()
err := tx.ReplaceDataFilesWithDataFiles(ctx, oldDataFiles, newDataFiles, props)
_, err = tx.Commit(ctx)
```

### 2. `scan.PlanFiles(ctx) → []FileScanTask`

Returns file scan tasks with both data files and their associated delete files:

```go
type FileScanTask struct {
    File                iceberg.DataFile   // the data file
    DeleteFiles         []iceberg.DataFile // position delete files
    EqualityDeleteFiles []iceberg.DataFile // equality delete files
}
```

Files with non-empty `EqualityDeleteFiles` are candidates for compaction.

### 3. `scan.ToArrowRecords(ctx)` 

Full merge-on-read scan that applies equality deletes. The output is clean data with deletes already applied — exactly what we need to write back as compacted files.

### 4. `table.WriteRecords(ctx, tbl, schema, records)` 

Writes Arrow records to new Parquet data files. Combined with scan, this gives us the compaction pipeline.

### 5. `tx.Delete(ctx, filter)` with copy-on-write mode

Alternative to equality deletes: `write.delete.mode = "copy-on-write"` rewrites data files on every DELETE/UPDATE. No delete files accumulate, reads stay constant. But writes are 10-100x slower.

## Compaction Strategies

### Strategy 1: Periodic Full Compaction (Simplest)

Scan the entire table with merge-on-read, write new data files, replace all old files.

```go
func CompactTable(ctx context.Context, tbl *table.Table) error {
    // 1. Plan files to identify which have delete files
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
        return nil // nothing to compact
    }
    
    // 2. Scan with merge-on-read → clean records
    schema, records, _ := tbl.Scan().ToArrowRecords(ctx)
    
    // 3. Write clean records as new data files
    var newFiles []iceberg.DataFile
    for df, err := range table.WriteRecords(ctx, tbl, schema, records) {
        newFiles = append(newFiles, df)
    }
    
    // 4. Atomic replace: old files → new files
    tx := tbl.NewTransaction()
    tx.ReplaceDataFilesWithDataFiles(ctx, filesToDelete, newFiles, nil)
    _, err := tx.Commit(ctx)
    return err
}
```

**Pros:** Simple, guaranteed constant read time after compaction.  
**Cons:** Rewrites entire table — expensive for large tables. Blocks during rewrite.

### Strategy 2: Incremental Compaction (Smart)

Only compact data files that have associated equality delete files.

```go
func CompactIncremental(ctx context.Context, tbl *table.Table) error {
    tasks, _ := tbl.Scan().PlanFiles(ctx)
    
    // Only compact files with delete files
    var dirtyTasks []FileScanTask
    for _, task := range tasks {
        if len(task.EqualityDeleteFiles) > 0 || len(task.DeleteFiles) > 0 {
            dirtyTasks = append(dirtyTasks, task)
        }
    }
    if len(dirtyTasks) == 0 {
        return nil
    }
    
    // Read only dirty files (with delete filtering applied)
    // Write compacted versions
    // Replace only those files
}
```

**Pros:** Only rewrites affected files — much cheaper.  
**Cons:** Harder to implement — need per-file scan with delete application.

### Strategy 3: Threshold-Based Auto-Compaction in Sink

Run compaction inside `SinkReplication` when delete file count exceeds a threshold.

```go
func (s *SinkReplication) maybeCompact(ctx context.Context, tbl *table.Table) {
    tasks, _ := tbl.Scan().PlanFiles(ctx)
    deleteFileCount := 0
    for _, task := range tasks {
        deleteFileCount += len(task.EqualityDeleteFiles)
    }
    if deleteFileCount > s.cfg.CompactionThreshold { // e.g., 20
        CompactTable(ctx, tbl)
    }
}
```

**Pros:** Automatic, no external scheduler needed.  
**Cons:** Compaction during replication adds latency to commits.

### Strategy 4: Copy-on-Write Mode (No Compaction Needed)

Switch from equality deletes to copy-on-write: set `write.delete.mode = "copy-on-write"` on the table.

In copy-on-write, every UPDATE/DELETE **rewrites the affected data files immediately**. No delete files are ever created. Reads are always fast.

```go
// Table property
catalog.CreateTable(ctx, ident, schema, catalog.WithProperties(iceberg.Properties{
    "format-version":    "2",
    "write.delete.mode": "copy-on-write",
}))
```

Then use `tx.Delete(ctx, filter)` instead of `tx.WriteEqualityDeletes` + `RowDelta`.

**Pros:** Read performance is constant. No compaction needed.  
**Cons:** Writes are much slower — must read + filter + rewrite entire data files per DELETE. Not suitable for high-throughput CDC.

### Strategy 5: Hybrid — Equality Deletes + Background Compaction

Use equality deletes for writes (fast), run compaction in a background goroutine.

```go
func (s *SinkReplication) startCompactionLoop(interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        for range ticker.C {
            for _, tbl := range s.tableCache {
                CompactTable(s.ctx, tbl)
            }
        }
    }()
}
```

**Pros:** Best of both worlds — fast writes, bounded read cost.  
**Cons:** Background compaction competes for I/O with the replication pipeline.

## Recommendation

For transferia-iceberg CDC:

| Approach | Write Speed | Read Speed | Complexity | Recommendation |
|----------|-------------|------------|------------|----------------|
| Equality deletes only | Fast | Degrades | Low | Current (not sustainable) |
| Copy-on-write | Slow | Constant | Low | Not for high-throughput CDC |
| **Equality deletes + periodic compaction** | Fast | Bounded | Medium | **Recommended** |
| Equality deletes + auto-compaction in sink | Fast | Bounded | High | Future optimization |

### Implementation Plan

**Phase 1: External compaction command**

Add `CompactTable` to the iceberg package, callable from CLI:
```bash
trcli compact --table public.orders
```

**Phase 2: Threshold-based trigger in SinkReplication**

After each flush, check delete file count. If over threshold, schedule compaction:
```go
if deleteFileCount > cfg.CompactionThreshold {
    go s.compactTable(ctx, tbl)
}
```

**Phase 3: Smart incremental compaction**

Only rewrite files that have associated delete files, preserving clean files.

## Key Numbers

| Scenario | Delete files | Scan (10K rows) | Relative |
|----------|-------------|------------------|----------|
| Clean table | 0 | 41ms | 1x |
| After 1K DML | ~5 | 170ms | 4x |
| After 5K DML | ~25 | 165ms | 4x |
| After 10K DML | ~50 | 5,087ms | 124x |
| **After compaction** | **0** | **~41ms** | **1x** |
