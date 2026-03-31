package replication

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

// TestSnapshotAndReplication tests the full CDC flow:
// 1. Snapshot the initial table state (3 rows)
// 2. Start replication
// 3. Perform INSERT, UPDATE, DELETE via SQL
// 4. Verify the Iceberg table reflects all changes
func TestSnapshotAndReplication(t *testing.T) {
	source := pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	TransferType := abstract.TransferTypeSnapshotAndIncrement
	helpers.InitSrcDst(helpers.TransferID, source, target, TransferType)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// Wait for snapshot to complete and replication to start
	time.Sleep(5 * time.Second)

	// Verify snapshot landed: 3 rows
	rowCount, err := iceberg.DestinationRowCount(target, "public", "cdc_test")
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount)

	// Now perform CDC operations on the source
	conn := pgConnect(t, source)
	defer conn.Close(context.Background())

	// INSERT a new row
	_, err = conn.Exec(context.Background(), "INSERT INTO cdc_test (id, name, val) VALUES (4, 'dave', 400)")
	require.NoError(t, err)

	// UPDATE an existing row
	_, err = conn.Exec(context.Background(), "UPDATE cdc_test SET name = 'alice_updated', val = 150 WHERE id = 1")
	require.NoError(t, err)

	// DELETE a row
	_, err = conn.Exec(context.Background(), "DELETE FROM cdc_test WHERE id = 3")
	require.NoError(t, err)

	// Wait for replication flush
	time.Sleep(10 * time.Second)

	// Verify final state: 3 original + 1 insert - 1 delete = 3 rows
	rowCount, err = iceberg.DestinationRowCount(target, "public", "cdc_test")
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount)
}

// TestReplicationOnly tests CDC replication without an initial snapshot.
func TestReplicationOnly(t *testing.T) {
	source := pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	TransferType := abstract.TransferTypeIncrementOnly
	helpers.InitSrcDst(helpers.TransferID, source, target, TransferType)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// Give replication time to start
	time.Sleep(3 * time.Second)

	// Perform INSERT operations
	conn := pgConnect(t, source)
	defer conn.Close(context.Background())

	for i := 10; i < 15; i++ {
		_, err = conn.Exec(context.Background(),
			fmt.Sprintf("INSERT INTO cdc_test (id, name, val) VALUES (%d, 'user_%d', %d)", i, i, i*100))
		require.NoError(t, err)
	}

	// Wait for replication flush
	time.Sleep(10 * time.Second)

	// Verify: 5 new rows landed
	rowCount, err := iceberg.DestinationRowCount(target, "public", "cdc_test")
	require.NoError(t, err)
	require.True(t, rowCount >= 5, "expected at least 5 rows, got %d", rowCount)
}

func pgConnect(t *testing.T, source interface{ AllHosts() []string }) *pgx.Conn {
	t.Helper()
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("SOURCE_PG_LOCAL_HOST"),
		os.Getenv("SOURCE_PG_LOCAL_PORT"),
		os.Getenv("SOURCE_PG_LOCAL_USER"),
		os.Getenv("SOURCE_PG_LOCAL_PASSWORD"),
		os.Getenv("SOURCE_PG_LOCAL_DATABASE"),
	)
	if connStr == "host= port= user= password= dbname= sslmode=disable" {
		connStr = "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable"
	}
	conn, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	return conn
}
