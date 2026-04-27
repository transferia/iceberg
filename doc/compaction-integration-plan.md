# Compaction Integration Plan

iceberg-go now has full compaction support. This document plans the integration into transferia-iceberg's CDC replication sink.

## Available iceberg-go APIs

### Planner (`table/compaction` package)

```go
type Config struct {
    TargetFileSizeBytes int64   // default: 512 MB
    MinFileSizeBytes    int64   // default: 75% of target
    MaxFileSizeBytes    int64   // default: 180% of target
    MinInputFiles       uint    // default: 5 — drop groups smaller than this
    DeleteFileThreshold int     // default: 5 — files with ≥N deletes are forced into compaction
    PackingLookback     uint    // default: 128
}

func (cfg Config) PlanCompaction(tasks []table.FileScanTask) (Plan, error)

type Plan struct {
    Groups          []Group
    SkippedFiles    int
    TotalInputFiles int
    TotalInputBytes int64
    EstOutputFiles  int
    EstOutputBytes  int64
}
```

### Executor (`table` package)

```go
func (t *Transaction) RewriteDataFiles(
    ctx context.Context,
    groups []CompactionTaskGroup,
    partialProgress bool,
    snapshotProps iceberg.Properties,
) (*RewriteResult, error)

type RewriteResult struct {
    RewrittenGroups    int
    AddedDataFiles     int
    RemovedDataFiles   int
    RemovedDeleteFiles int
    BytesBefore        int64
    BytesAfter         int64
}
```

### Important caveat

> **Equality delete files are intentionally preserved** — they may apply to data files outside the compaction scope. Removal of equality deletes requires verifying that ALL data files in the partition are being rewritten.

This means single-partition full compaction works, but partial compaction won't reduce equality delete count. For our CDC tables with no partitioning, this should mostly be fine since all files are in the unpartitioned bucket.

## Integration Strategy

### Phase 1: Bump iceberg-go and add a `Compact` method

**Goal**: standalone compaction callable from tests and CLI.

```go
// sink_replication.go (or a new compact.go)
func (s *SinkReplication) Compact(ctx context.Context, tableID abstract.TableID) (*table.RewriteResult, error) {
    tbl, err := s.catalog.LoadTable(ctx, table.Identifier{tableID.Namespace, tableID.Name})
    if err != nil {
        return nil, err
    }

    tasks, err := tbl.Scan().PlanFiles(ctx)
    if err != nil {
        return nil, err
    }

    cfg := compaction.DefaultConfig()
    cfg.MinInputFiles = 2 // CDC produces small files; lower threshold
    cfg.DeleteFileThreshold = 3 // aggressive: rewrite files with ≥3 deletes

    plan, err := cfg.PlanCompaction(tasks)
    if err != nil {
        return nil, err
    }
    if len(plan.Groups) == 0 {
        return &table.RewriteResult{}, nil // nothing to do
    }

    groups := make([]table.CompactionTaskGroup, len(plan.Groups))
    for i, g := range plan.Groups {
        groups[i] = table.CompactionTaskGroup{
            PartitionKey:   g.PartitionKey,
            Tasks:          g.Tasks,
            TotalSizeBytes: g.TotalSizeBytes,
        }
    }

    tx := tbl.NewTransaction()
    result, err := tx.RewriteDataFiles(ctx, groups, false, nil)
    if err != nil {
        return result, err
    }
    if _, err := tx.Commit(ctx); err != nil {
        return result, err
    }

    // Invalidate cached table — next replication flush reloads fresh metadata.
    s.mu.Lock()
    delete(s.tableCache, tableCacheKey(table.Identifier{tableID.Namespace, tableID.Name}))
    s.mu.Unlock()

    return result, nil
}
```

**Tests:**
- Add `TestCompactionAfterDML` — run 1K DML ops, call `Compact()`, verify scan time returns to baseline.
- Modify `TestEqualityDeleteReadPerf` to include compaction step at round 5 and confirm the slope flattens.

### Phase 2: Threshold-based auto-compaction in the sink

**Goal**: trigger compaction automatically when delete files exceed a threshold.

Add config knobs to `Destination`:
```go
type Destination struct {
    // ... existing fields ...

    // CompactionEnabled triggers automatic compaction when delete file count exceeds threshold.
    CompactionEnabled bool

    // CompactionDeleteFileThreshold triggers compaction when this many delete files accumulate per table.
    // Default: 20.
    CompactionDeleteFileThreshold int

    // CompactionMinInterval is the minimum time between compactions for the same table.
    // Default: 5 minutes. Prevents hot-loop compaction under heavy write load.
    CompactionMinInterval time.Duration
}
```

Hook it into the flush path. After a successful flush:
```go
func (s *SinkReplication) maybeCompact(ctx context.Context, tableID abstract.TableID) {
    if !s.cfg.CompactionEnabled {
        return
    }
    if time.Since(s.lastCompaction[tableCacheKey]) < s.cfg.CompactionMinInterval {
        return
    }

    // Cheap check: snapshot summary tells us delete file count without reading manifests.
    tbl := s.tableCache[tableCacheKey]
    if snap := tbl.CurrentSnapshot(); snap != nil && snap.Summary != nil {
        deleteFiles, _ := strconv.Atoi(snap.Summary.Properties["total-delete-files"])
        if deleteFiles < s.cfg.CompactionDeleteFileThreshold {
            return
        }
    }

    // Run compaction in a goroutine to avoid blocking replication.
    go func() {
        result, err := s.Compact(ctx, tableID)
        if err != nil {
            s.lgr.Warn("compaction failed", log.Error(err))
            return
        }
        s.lgr.Info("compaction complete",
            log.Int("rewritten_groups", result.RewrittenGroups),
            log.Int("removed_delete_files", result.RemovedDeleteFiles))
    }()
}
```

**Concurrency concerns:**
- Background compaction commits a snapshot. If the foreground replication tries to commit at the same time, we get `CommitFailedException`. We already invalidate the cache on failure and retry, so this should self-heal — but it costs latency.
- Simpler alternative: serialize compaction with replication flushes via the existing mutex. Compaction runs synchronously between flushes when triggered. This adds latency to one flush but avoids retry storms.

Recommendation: **start with synchronous in-flush compaction** (simpler, more predictable). If latency becomes a problem, move to background goroutine with retry handling.

### Phase 3: Re-enable benchmarks in CI

Once compaction is wired up:
- Re-enable `TestBenchmarkInsertHeavy` and `TestBenchmarkBalanced` in CI (drop `skipBenchInCI`).
- Add a final assertion: lag at end of run < 5 seconds (currently fails because reads time out).

## Tunable Defaults for CDC

CDC produces many small files (one per commit interval). Different defaults than the iceberg-go ones:

| Setting | iceberg-go default | CDC override | Why |
|---------|-------------------|--------------|-----|
| `TargetFileSizeBytes` | 512 MB | **64 MB** | Balance file size vs commit frequency |
| `MinFileSizeBytes` | 384 MB (75%) | **8 MB** | Most CDC files are tiny |
| `MinInputFiles` | 5 | **2** | Compact even small groups |
| `DeleteFileThreshold` | 5 | **3** | Aggressive: dirty files compact often |
| `CompactionDeleteFileThreshold` | — | **20** | Trigger when 20+ delete files exist |
| `CompactionMinInterval` | — | **5 min** | Prevent hot loops |

## Testing Plan

1. **Local benchmark** (already passing) — re-run InsertHeavy with compaction enabled
   - Expected: replication lag stays under 10s, no read timeouts, end-of-run drain < 30s
2. **Unit tests for `Compact()`** — table-driven cases:
   - No delete files → no-op, returns empty result
   - 1 file, 5 deletes → 1 rewrite, all deletes removed
   - Multiple files, partial dirty → only dirty files rewritten
3. **Long-running soak test** (manual) — 30 min Balanced workload with auto-compaction
   - Expected: no monotonic growth in scan time, snapshot count grows linearly

## Open Questions

1. **Equality delete cleanup**: iceberg-go currently preserves equality deletes during compaction. For our unpartitioned CDC tables, when we rewrite ALL files in the unpartitioned bucket, can we manually clear them? Need to follow up with iceberg-go community.

2. **Partial progress mode**: `RewriteDataFiles(ctx, groups, partialProgress=true, ...)` commits each group separately. For long-running compaction, this avoids losing all work on one failure — but produces more snapshots. Default to `false` for atomic compaction; expose as a knob.

3. **Compaction during replication failover**: if the worker crashes mid-compaction, the snapshot is either committed or not (atomic). No data loss, but partial work is lost. Acceptable.

## Implementation Order

1. Bump `iceberg-go` to a version that includes RewriteDataFiles + compaction package
2. Add `SinkReplication.Compact(ctx, tableID)` method
3. Add unit tests
4. Re-run `TestEqualityDeleteReadPerf` with compaction at midpoint to demonstrate constant-time reads
5. Add config knobs to `Destination` for auto-compaction
6. Wire into flush path (synchronous mode)
7. Re-enable full benchmarks in CI
8. Update README/demo with compaction story
