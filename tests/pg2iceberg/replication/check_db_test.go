// Skip in CI: substrait-go v7.6.0 panics during init() on linux/amd64
// due to a go-yaml bug (strings.Repeat with negative count).
// Run locally with: go test -tags cdc_replication ./tests/pg2iceberg/replication/
//go:build cdc_replication

package replication

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

func dumpDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "dump", "pg")
}

// TestSnapshotAndReplication tests the full CDC flow:
// 1. Snapshot the initial table state (3 rows)
// 2. Start replication
// 3. Perform INSERT, UPDATE, DELETE via SQL
// 4. Verify the Iceberg table reflects all changes
func TestSnapshotAndReplication(t *testing.T) {
	source := pgrecipe.RecipeSource(
		pgrecipe.WithInitDir(dumpDir()),
		pgrecipe.WithoutPgDump(),
	)
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	// Clean up any leftover Iceberg table from previous runs
	iceberg.CleanupTable(target, "public", "cdc_test")

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// Wait for snapshot to complete, flush, and replication to start
	time.Sleep(15 * time.Second)

	// Verify snapshot landed: 3 rows
	rowCount, err := iceberg.DestinationRowCount(target, "public", "cdc_test")
	require.NoError(t, err, "table should exist in Iceberg after snapshot")
	require.Equal(t, uint64(3), rowCount, "snapshot should land 3 rows")

	// Perform CDC operations on the source
	conn := pgConnect(t, source)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "INSERT INTO cdc_test (id, name, val) VALUES (4, 'dave', 400)")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "UPDATE cdc_test SET name = 'alice_updated', val = 150 WHERE id = 1")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "DELETE FROM cdc_test WHERE id = 3")
	require.NoError(t, err)

	// Wait for replication flush
	time.Sleep(15 * time.Second)

	// Verify final state: 3 original + 1 insert - 1 delete = 3 rows
	rowCount, err = iceberg.DestinationRowCount(target, "public", "cdc_test")
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount, "after CDC: 3 + 1 insert - 1 delete = 3")
}

// TestReplicationOnly tests CDC replication without an initial snapshot.
func TestReplicationOnly(t *testing.T) {


	source := pgrecipe.RecipeSource(
		pgrecipe.WithInitDir(dumpDir()),
		pgrecipe.WithoutPgDump(),
	)
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(3 * time.Second)

	conn := pgConnect(t, source)
	defer conn.Close(context.Background())

	for i := 10; i < 15; i++ {
		_, err = conn.Exec(context.Background(),
			fmt.Sprintf("INSERT INTO cdc_test (id, name, val) VALUES (%d, 'user_%d', %d)", i, i, i*100))
		require.NoError(t, err)
	}

	time.Sleep(15 * time.Second)

	rowCount, err := iceberg.DestinationRowCount(target, "public", "cdc_test")
	require.NoError(t, err)
	require.True(t, rowCount >= 5, "expected at least 5 rows, got %d", rowCount)
}

func pgConnect(t *testing.T, source *provider_postgres.PgSource) *pgx.Conn {
	t.Helper()
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		source.Hosts[0], source.Port, source.User, string(source.Password), source.Database,
	)
	conn, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	return conn
}
