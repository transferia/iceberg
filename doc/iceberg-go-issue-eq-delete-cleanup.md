# GitHub Issue Draft: `apache/iceberg-go`

## Title

`feat(table): drop dangling equality delete files during RewriteDataFiles when all referenced data files are rewritten`

## Body

### Summary

When `Transaction.RewriteDataFiles` compacts data files, position delete files are removed from the new snapshot if they pointed at rewritten files (good!). Equality delete files are intentionally preserved (`collectSafePositionDeletes`, [`rewrite_data_files.go:201-235`](https://github.com/apache/iceberg-go/blob/main/table/rewrite_data_files.go#L201-L235)).

For high-churn CDC workloads this leaves dangling equality delete entries in every subsequent snapshot's manifests forever. Compaction restores read perf in the short term (the seq-number filter at [`scanner.go:372-398`](https://github.com/apache/iceberg-go/blob/main/table/scanner.go#L372-L398) skips them), but the manifest read overhead grows linearly with the number of preserved eq-delete entries. Over days of replication this becomes the dominant scan cost.

I'd like to propose extending the cleanup to also drop equality deletes that are provably dead after compaction.

### Why this matters

Workload: PostgreSQL → Iceberg v2 CDC replication, ~one snapshot every 5 seconds, ~1 equality delete file per snapshot for UPDATE/DELETE operations.

Empirical numbers from a small repro on a 10K-row table (Apple M1 Pro, local REST + MinIO):

| State | Files (data / eq-delete) | `COUNT(*)` scan |
|---|---|---|
| Clean | 1 / 0 | ~30ms (baseline) |
| 10K DML, no compaction | 16 / 120 | ~217ms (peak 382ms) |
| **After `iceberg compact run`** | **1 / 120** | **~50ms** |

So compaction is effective — 16 → 1 data file, scan time drops from 217ms to 50ms. But the 120 dangling eq-delete files now sit in every future snapshot's manifests with no purpose.

Extrapolating sustained CDC at one eq-delete per snapshot:

| Time running | Dangling eq-deletes | Estimated overhead |
|---|---|---|
| 1 hour | ~720 | ~120ms per scan |
| 1 day | ~17,000 | ~3 sec per scan |
| 1 week | ~120,000 | ~20 sec per scan |

`ExpireSnapshots` doesn't help here, because the **current** snapshot's manifests still reference the dead eq-delete files. The cleanup has to happen during the rewrite commit itself.

### When is an equality delete safe to drop?

The Iceberg v2 spec says an equality delete file E applies to a data file D iff `E.seq > D.seq` (and partitions match). After `RewriteDataFiles` rewrites a set of files in a partition, the new files have a sequence number strictly greater than every existing file in that partition.

So an equality delete file E is **provably dead** after compaction when:

1. Every data file with `data.seq < E.seq` in E's partition was in the rewrite set, AND
2. `E.seq < new_compacted_file.seq` (always true — compaction commit gets the new highest seq)

In other words: if compaction processed every data file E could have applied to, E will never apply to anything again.

The simplest case — and the one most CDC sinks hit — is **unpartitioned full-partition compaction**: when every data file in the (only) partition is being rewritten, every equality delete file in that partition becomes dead.

### Proposal

Extend `collectSafePositionDeletes` (or add a sibling) to also collect equality deletes that are provably dead given the rewrite scope.

Sketch:

```go
// collectSafeEqualityDeletes returns equality delete files that are safe
// to remove given the set of data files being rewritten in this group.
//
// An equality delete file E is safe to remove when, within E's partition,
// every data file with sequence number < E.seq is in the rewrite set.
// After commit, E has no remaining data files it could apply to.
func collectSafeEqualityDeletes(
    tasks []FileScanTask,
    allTasksInPartition map[string][]FileScanTask, // by partition key
) []iceberg.DataFile {
    rewrittenInPartition := groupRewrittenByPartition(tasks)

    candidates := map[string]iceberg.DataFile{} // path → file
    for _, task := range tasks {
        for _, df := range task.EqualityDeleteFiles {
            candidates[df.FilePath()] = df
        }
    }

    var safe []iceberg.DataFile
    for _, eqDelete := range candidates {
        partKey := partitionKey(eqDelete)
        // Find every data file with seq < eqDelete.seq in this partition.
        // If all of them are being rewritten, eqDelete is dead.
        allDead := true
        for _, t := range allTasksInPartition[partKey] {
            if t.File.SequenceNumber() >= eqDelete.SequenceNumber() {
                continue // higher-seq files don't matter to this delete
            }
            if !rewrittenInPartition[partKey][t.File.FilePath()] {
                allDead = false
                break
            }
        }
        if allDead {
            safe = append(safe, eqDelete)
        }
    }
    return safe
}
```

The check requires knowing the full set of data files in each partition, not just the rewrite set — so the planner ([`compaction/compaction.go`](https://github.com/apache/iceberg-go/blob/main/table/compaction/compaction.go)) needs to surface that, or `RewriteDataFiles` needs to re-plan over all partitions involved in the rewrite scope.

### Why this is safe

- The Iceberg v2 spec uses sequence numbers exactly for this purpose. An eq-delete with no data file at lower sequence can never logically apply.
- For partitioned tables we still need partition matching — that's `len(delPartition) > 0 && partitionsMatch(...)` in the existing scanner code.
- The conflict validator framework added in #928 ([`feat(table): add conflict validation framework`](https://github.com/apache/iceberg-go/pull/928)) plus the rewrite validator at `rewrite_data_files.go:191` already protect us against concurrent writers adding new data files at lower seq during the rewrite.

### Scope (what I'm NOT asking for)

To keep the change small:

- **Cross-partition logic**: leave as-is. Eq-deletes on a partition that's only partially rewritten remain preserved (today's behavior).
- **Standalone "expire dead deletes" routine**: separate followup. This issue is just about fixing the cleanup that already runs inside `RewriteDataFiles`.
- **Deletion vectors (v3)**: separate, deferred until DV read support lands.

### Reproducing

Repro repo: https://github.com/transferia/iceberg (branch `feat/cdc-replication-sink`)

```bash
# Start infra
docker-compose -f recipe/docker-compose.yml up -d

# Accumulate garbage: 16 data files + 120 eq deletes
go test -run TestEqualityDeleteReadPerf -timeout=10m ./tests/bench/

# Build CLI from this repo
go build -o /tmp/iceberg-cli ./cmd/iceberg/

# Plan and run compaction
AWS_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=password \
  /tmp/iceberg-cli --uri http://localhost:8181 --catalog rest \
  compact analyze public.eq_delete_perf
# Plan: 16 files scanned, 16 to rewrite, 1 group, 120 delete files

AWS_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=password \
  /tmp/iceberg-cli --uri http://localhost:8181 --catalog rest \
  compact run public.eq_delete_perf
# Done. Rewrote 16 -> 1 files. Removed 0 delete files.   ← this "0" is what I'd like to fix

# Inspect: the new snapshot's manifests still list 120 eq-delete files
/tmp/iceberg-cli ... files public.eq_delete_perf
```

### Happy to help

I can take a stab at the implementation if there's interest — wanted to confirm the design direction first since the existing comment at `rewrite_data_files.go:209-213` explicitly notes this was deferred. Is the partition-scope check the blocker, or are there other concerns I'm missing?
