package replication

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

func TestSnapshotAndReplication(t *testing.T) {
	source := mysqlrecipe.RecipeMysqlSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	iceberg.CleanupTable(target, source.Database, "cdc_test")

	helpers.InitSrcDst(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MySQL source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(10 * time.Second)

	rowCount, err := iceberg.DestinationRowCount(target, source.Database, "cdc_test")
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount, "snapshot should land 3 rows")

	db := mysqlConnect(t, source)
	defer db.Close()

	_, err = db.Exec("INSERT INTO cdc_test (id, name, val) VALUES (4, 'dave', 400)")
	require.NoError(t, err)

	_, err = db.Exec("UPDATE cdc_test SET name = 'alice_updated', val = 150 WHERE id = 1")
	require.NoError(t, err)

	_, err = db.Exec("DELETE FROM cdc_test WHERE id = 3")
	require.NoError(t, err)

	time.Sleep(15 * time.Second)

	rowCount, err = iceberg.DestinationRowCount(target, source.Database, "cdc_test")
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount, "after CDC: 3 + 1 insert - 1 delete = 3")
}

func TestReplicationOnly(t *testing.T) {
	source := mysqlrecipe.RecipeMysqlSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	iceberg.CleanupTable(target, source.Database, "cdc_test")

	helpers.InitSrcDst(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MySQL source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(3 * time.Second)

	db := mysqlConnect(t, source)
	defer db.Close()

	for i := 10; i < 15; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO cdc_test (id, name, val) VALUES (%d, 'user_%d', %d)", i, i, i*100))
		require.NoError(t, err)
	}

	time.Sleep(15 * time.Second)

	rowCount, err := iceberg.DestinationRowCount(target, source.Database, "cdc_test")
	require.NoError(t, err)
	require.True(t, rowCount >= 5, "expected at least 5 rows, got %d", rowCount)
}

func mysqlConnect(t *testing.T, source *provider_mysql.MysqlSource) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		source.User, string(source.Password),
		source.Host, source.Port, source.Database,
	)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}
