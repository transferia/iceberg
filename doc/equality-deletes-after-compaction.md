# Equality Deletes After Compaction: Behavior Analysis

This document expands on the equality delete preservation in `table.RewriteDataFiles`. Reading the code precisely changes my picture of how bad this actually is.

## What `RewriteDataFiles` does today

Source: `iceberg-go/table/rewrite_data_files.go:201-235`

```go
// collectSafePositionDeletes returns position delete files from the given
// tasks that are safe to remove during compaction.
//
// Only position deletes (EntryContentPosDeletes) are considered.
// Equality deletes and deletion vectors are intentionally excluded:
//   - Equality deletes may apply to data files outside the compaction
//     scope. Removing them requires verifying the entire partition is
//     being rewritten.
```

So **equality delete files are intentionally left in the table metadata** after compaction. They are not removed even when every data file they referenced has been rewritten.

## Does this break correctness?

**No.** The scan-time matching rule saves us.

Source: `iceberg-go/table/scanner.go:372-398` (`matchEqualityDeletesToData`):

```
An equality delete file applies to a data file only if:
  del.SequenceNum() > data.SequenceNum()
```

When compaction writes new data files, they get the compaction snapshot's sequence number — which is **higher** than every pre-existing equality delete file. So every old equality delete file fails the `seq > seq` check against the new data file and is skipped at scan time.

**Concrete example:**

```
seq 1: data file A (10K rows, INSERT)
seq 2: eq delete file D1 (deletes 200 PKs from A)
seq 3: eq delete file D2 (deletes 300 PKs from A)
seq 4: data file B (compacted A with D1+D2 applied → 9,500 rows)

After compaction, the table has: B, D1, D2
Scan plan for B:
  - matchEqualityDeletesToData(B, [D1, D2])
  - D1.seq (2) > B.seq (4)? NO → skip
  - D2.seq (3) > B.seq (4)? NO → skip
  - B is read with no delete filtering. Correct.
```

So scans return correct results after compaction. ✅

## What's the actual cost then?

Three things, none of them correctness:

### 1. Manifest read overhead

Every scan still reads the equality delete manifests to build the eq-delete entry list. Cost is per-scan, proportional to the number of preserved eq-delete files. For our case (~50 eq-deletes after 5 min CDC), this is the manifest read for each — small but non-zero.

### 2. Per-file matching loop

`PlanFiles` loops over all eq-delete entries for every data file in `matchEqualityDeletesToData`. The seq-number check is O(1) per pair, but it's still O(N×M) work where N=data files, M=eq-delete files. After compaction with 1 data file and 50 stale eq-deletes, that's 50 fast comparisons per scan. Negligible.

### 3. Storage waste

The Parquet files for the dead eq deletes stay in S3 forever (until `ExpireSnapshots` + `DeleteOrphanFiles` cleans them up). Each file is small (a few KB of PKs) but they accumulate.

## So what's the actual blocker?

Looking at our 124x slowdown reproduction (`TestEqualityDeleteReadPerf`), the read cost grows because **every data file** has to apply **every relevant eq-delete file**, where "relevant" means `seq > dataSeq`. As long as we keep writing new data files (CDC) AND the eq-deletes have higher seqs than those data files, the merge-on-read still happens.

After compaction, the situation changes:
- All compacted data files have the highest sequence number.
- Old eq-deletes are lower-seq than the new data files → they don't apply.
- New eq-deletes (post-compaction) WILL apply to the new data files until the next compaction.

So compaction **does** fix the read cost — the merge work goes from "N data files × M deletes" to "1 compacted file × deletes_since_last_compaction". The dead eq-delete manifests are loaded but their entries are filtered out cheaply.

## What I want from iceberg-go

Three asks, in increasing scope:

### Ask 1 (easy): Remove eq-deletes when ALL referenced data files are rewritten

When compaction rewrites every data file that an eq-delete file targets (because their seq < compaction seq), that eq-delete is provably dead. Detection:

```
for each eq_delete_file D in scope:
    if every data_file with data.seq < D.seq is in the rewrite set:
        D is safe to remove
```

For an unpartitioned full-table compaction, this collapses to "are we rewriting every data file?" which is straightforward.

This is a strict superset of the position-delete cleanup that already exists.

### Ask 2 (harder): Remove dead eq-deletes during planning

Eq-delete files where every potentially-applicable data file has a higher sequence number are dead. They will never apply to anything. A standalone "expire dead delete files" routine could remove them without rewriting any data — pure metadata operation.

### Ask 3 (largest): VACUUM / expire-snapshots integration

Standard Iceberg has `expire_snapshots` + `remove_orphan_files`. iceberg-go has `ExpireSnapshots` already. The orphan file removal step should pick up the dead eq-delete data files once they're no longer referenced by any live snapshot.

## Practical impact for our CDC sink

Given the seq-number filter saves correctness, the practical impact of skipping equality deletes during compaction is small:

| Concern | Impact |
|---------|--------|
| Read correctness | None |
| Read latency | Manifest load + cheap seq check. Small fixed cost per scan. |
| Storage | Dead eq-delete files persist until snapshot expiration |
| Compaction effectiveness | Full — read cost still drops to baseline after compaction |

**Conclusion**: we can ship Phase 1 (`SinkReplication.Compact()`) without waiting for upstream changes. The dead eq-delete files are wasteful but not harmful. We file Ask 1 with iceberg-go as a follow-up — it's a clear, bounded enhancement.

## Followup plan

1. Verify empirically in our `TestEqualityDeleteReadPerf` test — add a midpoint `Compact()` call and confirm scan time returns to baseline despite preserved eq-deletes.
2. If empirically fine, file Ask 1 as a GitHub issue on `apache/iceberg-go`. Reference the pos-delete cleanup at `rewrite_data_files.go:201` as the blueprint.
3. Track storage growth in the long-running soak test — if it gets meaningful, investigate `ExpireSnapshots` integration.
