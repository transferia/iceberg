package replication

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

// TestSnapshotAndReplication tests snapshot + CDC replication for MySQL.
func TestSnapshotAndReplication(t *testing.T) {
	source := mysqlrecipe.RecipeMysqlSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	TransferType := abstract.TransferTypeSnapshotAndIncrement
	helpers.InitSrcDst(helpers.TransferID, source, target, TransferType)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MySQL source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// Wait for snapshot to complete
	time.Sleep(5 * time.Second)

	// Verify snapshot: 3 rows
	rowCount, err := iceberg.DestinationRowCount(target, source.Database, "cdc_test")
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount)

	// CDC operations
	db := mysqlConnect(t)
	defer db.Close()

	_, err = db.Exec("INSERT INTO cdc_test (id, name, val) VALUES (4, 'dave', 400)")
	require.NoError(t, err)

	_, err = db.Exec("UPDATE cdc_test SET name = 'alice_updated', val = 150 WHERE id = 1")
	require.NoError(t, err)

	_, err = db.Exec("DELETE FROM cdc_test WHERE id = 3")
	require.NoError(t, err)

	// Wait for replication flush
	time.Sleep(10 * time.Second)

	// Verify: 3 original + 1 insert - 1 delete = 3
	rowCount, err = iceberg.DestinationRowCount(target, source.Database, "cdc_test")
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount)
}

// TestReplicationOnly tests CDC-only replication for MySQL.
func TestReplicationOnly(t *testing.T) {
	source := mysqlrecipe.RecipeMysqlSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	TransferType := abstract.TransferTypeIncrementOnly
	helpers.InitSrcDst(helpers.TransferID, source, target, TransferType)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MySQL source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(3 * time.Second)

	db := mysqlConnect(t)
	defer db.Close()

	for i := 10; i < 15; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO cdc_test (id, name, val) VALUES (%d, 'user_%d', %d)", i, i, i*100))
		require.NoError(t, err)
	}

	time.Sleep(10 * time.Second)

	rowCount, err := iceberg.DestinationRowCount(target, source.Database, "cdc_test")
	require.NoError(t, err)
	require.True(t, rowCount >= 5, "expected at least 5 rows, got %d", rowCount)
}

func mysqlConnect(t *testing.T) *sql.DB {
	t.Helper()
	host := os.Getenv("SOURCE_MYSQL_LOCAL_HOST")
	port := os.Getenv("SOURCE_MYSQL_LOCAL_PORT")
	user := os.Getenv("SOURCE_MYSQL_LOCAL_USER")
	pass := os.Getenv("SOURCE_MYSQL_LOCAL_PASSWORD")
	dbname := os.Getenv("SOURCE_MYSQL_LOCAL_DATABASE")
	if host == "" {
		host = "localhost"
		port = "3306"
		user = "root"
		pass = ""
		dbname = "source"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, dbname)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}
