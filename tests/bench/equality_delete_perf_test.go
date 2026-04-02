package bench

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

// TestEqualityDeleteReadPerf measures how scan-based COUNT(*) degrades
// as equality delete files accumulate from UPDATE/DELETE operations.
//
// It runs N rounds of DML (each round = batch of UPDATEs + DELETEs),
// and after each round measures the time to count rows via full scan.
//
// This reproduces the merge-on-read overhead: each scan must apply
// all accumulated equality delete files against all data files.
func TestEqualityDeleteReadPerf(t *testing.T) {
	if os.Getenv("CATALOG_ENDPOINT") == "" {
		t.Skip("CATALOG_ENDPOINT not set; start infra with 'make recipe'")
	}
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		t.Skip("skipped in CI; run locally")
	}

	ctx := context.Background()

	// --- Setup ---
	source := pgrecipe.RecipeSource(pgrecipe.WithoutPgDump())
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	iceberg.CleanupTable(target, "public", "eq_delete_perf")

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		source.Hosts[0], source.Port, source.User, string(source.Password), source.Database)
	pool, err := pgxpool.Connect(ctx, connStr)
	require.NoError(t, err)
	defer pool.Close()

	// Create table with 10K initial rows
	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS eq_delete_perf;
		CREATE TABLE eq_delete_perf (
			id     BIGINT PRIMARY KEY,
			value  INT NOT NULL,
			label  VARCHAR(64) NOT NULL
		);
		INSERT INTO eq_delete_perf (id, value, label)
		SELECT g, g % 1000, 'initial_' || g
		FROM generate_series(1, 10000) g;
	`)
	require.NoError(t, err)

	// --- Start transfer ---
	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// Wait for snapshot
	time.Sleep(10 * time.Second)

	initialCount, err := iceberg.DestinationRowCount(target, "public", "eq_delete_perf")
	require.NoError(t, err)
	t.Logf("Initial snapshot: %d rows", initialCount)

	// --- Measure read perf as delete files accumulate ---
	type measurement struct {
		Round       int
		TotalDML    int
		CountResult uint64
		ScanTime    time.Duration
	}
	var measurements []measurement

	// Measure baseline scan time (no equality deletes yet)
	start := time.Now()
	count, err := iceberg.DestinationRowCount(target, "public", "eq_delete_perf")
	baselineScan := time.Since(start)
	require.NoError(t, err)
	t.Logf("Baseline scan: %d rows in %s", count, baselineScan)
	measurements = append(measurements, measurement{Round: 0, TotalDML: 0, CountResult: count, ScanTime: baselineScan})

	rounds := 10
	dmlPerRound := 1000 // 500 UPDATEs + 500 DELETEs per round

	for round := 1; round <= rounds; round++ {
		// Generate UPDATEs: update random rows
		offset := (round - 1) * dmlPerRound
		for i := 0; i < dmlPerRound/2; i++ {
			id := (offset+i)%10000 + 1
			_, err = pool.Exec(ctx,
				"UPDATE eq_delete_perf SET value = $1, label = $2 WHERE id = $3",
				round*1000+i, fmt.Sprintf("updated_r%d_%d", round, i), id,
			)
			require.NoError(t, err)
		}

		// Generate DELETEs + re-INSERTs (to keep row count stable)
		for i := dmlPerRound / 2; i < dmlPerRound; i++ {
			id := (offset+i)%10000 + 1
			_, err = pool.Exec(ctx, "DELETE FROM eq_delete_perf WHERE id = $1", id)
			require.NoError(t, err)
			_, err = pool.Exec(ctx,
				"INSERT INTO eq_delete_perf (id, value, label) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET value = $2, label = $3",
				id, round*2000+i, fmt.Sprintf("reinserted_r%d_%d", round, i),
			)
			require.NoError(t, err)
		}

		// Wait for replication to flush (commit interval = 2s, give extra time)
		time.Sleep(5 * time.Second)

		// Measure scan time
		start := time.Now()
		count, err := iceberg.DestinationRowCount(target, "public", "eq_delete_perf")
		scanTime := time.Since(start)
		if err != nil {
			t.Logf("Round %d: scan error (expected with heavy deletes): %v", round, err)
			measurements = append(measurements, measurement{Round: round, TotalDML: round * dmlPerRound, ScanTime: scanTime})
			continue
		}

		measurements = append(measurements, measurement{Round: round, TotalDML: round * dmlPerRound, CountResult: count, ScanTime: scanTime})
		t.Logf("Round %2d: %5d cumulative DML ops → scan: %d rows in %s",
			round, round*dmlPerRound, count, scanTime)
	}

	// --- Print summary ---
	t.Log("\n=== Equality Delete Read Performance ===")
	t.Log("Round | Cumulative DML | Rows  | Scan Time | Slowdown vs Baseline")
	t.Log("------|----------------|-------|-----------|---------------------")
	for _, m := range measurements {
		slowdown := float64(1.0)
		if baselineScan > 0 {
			slowdown = float64(m.ScanTime) / float64(baselineScan)
		}
		t.Logf("%5d | %14d | %5d | %9s | %.1fx",
			m.Round, m.TotalDML, m.CountResult, m.ScanTime.Truncate(time.Millisecond), slowdown)
	}
	t.Log("========================================")
}
