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

// TestTableFileStats demonstrates how to get dirty file statistics
// from an Iceberg table after CDC operations with equality deletes.
func TestTableFileStats(t *testing.T) {
	if os.Getenv("CATALOG_ENDPOINT") == "" {
		t.Skip("CATALOG_ENDPOINT not set; start infra with 'make recipe'")
	}

	ctx := context.Background()

	source := pgrecipe.RecipeSource(pgrecipe.WithoutPgDump())
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	iceberg.CleanupTable(target, "public", "file_stats_test")

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		source.Hosts[0], source.Port, source.User, string(source.Password), source.Database)
	pool, err := pgxpool.Connect(ctx, connStr)
	require.NoError(t, err)
	defer pool.Close()

	// Create table with 1K rows
	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS file_stats_test;
		CREATE TABLE file_stats_test (
			id    BIGINT PRIMARY KEY,
			value INT NOT NULL
		);
		INSERT INTO file_stats_test (id, value)
		SELECT g, g FROM generate_series(1, 1000) g;
	`)
	require.NoError(t, err)

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(8 * time.Second)

	// Helper: load table and print stats
	printStats := func(label string) {
		tbl, err := iceberg.LoadTable(target, "public", "file_stats_test")
		require.NoError(t, err)

		// --- Source 1: Snapshot Summary ---
		snap := tbl.CurrentSnapshot()
		if snap != nil && snap.Summary != nil {
			t.Logf("[%s] Snapshot Summary:", label)
			t.Logf("  total-data-files:       %s", snap.Summary.Properties["total-data-files"])
			t.Logf("  total-delete-files:      %s", snap.Summary.Properties["total-delete-files"])
			t.Logf("  total-equality-deletes:  %s", snap.Summary.Properties["total-equality-deletes"])
			t.Logf("  total-records:           %s", snap.Summary.Properties["total-records"])
		}

		// --- Source 2: PlanFiles ---
		tasks, err := tbl.Scan().PlanFiles(ctx)
		require.NoError(t, err)

		totalDataFiles := len(tasks)
		dirtyDataFiles := 0
		totalEqDeleteFiles := 0
		for _, task := range tasks {
			if len(task.EqualityDeleteFiles) > 0 {
				dirtyDataFiles++
				totalEqDeleteFiles += len(task.EqualityDeleteFiles)
			}
		}
		t.Logf("[%s] PlanFiles:", label)
		t.Logf("  data files:              %d", totalDataFiles)
		t.Logf("  dirty data files:        %d (have eq delete files)", dirtyDataFiles)
		t.Logf("  equality delete files:   %d (total associations)", totalEqDeleteFiles)
		t.Logf("  dirty ratio:             %.0f%%", float64(dirtyDataFiles)/float64(max(totalDataFiles, 1))*100)
	}

	printStats("after snapshot")

	// Round 1: 200 UPDATEs
	for i := 1; i <= 200; i++ {
		_, err = pool.Exec(ctx, "UPDATE file_stats_test SET value = $1 WHERE id = $2", i+10000, i)
		require.NoError(t, err)
	}
	time.Sleep(5 * time.Second)
	printStats("after 200 UPDATEs")

	// Round 2: 200 more UPDATEs + 100 DELETEs
	for i := 201; i <= 400; i++ {
		_, err = pool.Exec(ctx, "UPDATE file_stats_test SET value = $1 WHERE id = $2", i+20000, i)
		require.NoError(t, err)
	}
	for i := 901; i <= 1000; i++ {
		_, err = pool.Exec(ctx, "DELETE FROM file_stats_test WHERE id = $1", i)
		require.NoError(t, err)
	}
	time.Sleep(5 * time.Second)
	printStats("after 500 DML total")

	// Round 3: 500 more UPDATEs
	for i := 401; i <= 900; i++ {
		_, err = pool.Exec(ctx, "UPDATE file_stats_test SET value = $1 WHERE id = $2", i+30000, i)
		require.NoError(t, err)
	}
	time.Sleep(5 * time.Second)
	printStats("after 1000 DML total")
}

