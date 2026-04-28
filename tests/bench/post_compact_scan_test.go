package bench

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
)

// TestPostCompactionScanPerf measures scan latency on a table that has been
// compacted via the iceberg CLI. Run as a manual sequence:
//   1. TestEqualityDeleteReadPerf — produce 16 data files + 120 eq deletes
//   2. iceberg compact run public.eq_delete_perf — bin-pack into 1 file
//   3. TestPostCompactionScanPerf — measure scan time
//
// Empirical result (Apple M1 Pro, local stack):
//   Before compaction (16 files + 120 eq deletes): scan ~217ms (peak 382ms)
//   After compaction (1 file + 120 dangling deletes): scan ~50ms warm
//
// The dangling equality delete files have a lower sequence number than the
// new compacted data file, so they are filtered out at scan plan time
// (matchEqualityDeletesToData). Read perf returns to near-baseline despite
// the preserved files. Confirms our analysis in
// doc/equality-deletes-after-compaction.md.
func TestPostCompactionScanPerf(t *testing.T) {
	if os.Getenv("CATALOG_ENDPOINT") == "" {
		t.Skip("CATALOG_ENDPOINT not set")
	}

	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)

	const iters = 5
	for i := range iters {
		start := time.Now()
		rows, err := iceberg.DestinationRowCount(target, "public", "eq_delete_perf")
		elapsed := time.Since(start)
		require.NoError(t, err)
		t.Logf("scan %d: %d rows in %s", i, rows, elapsed)
	}
}
