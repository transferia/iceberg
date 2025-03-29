package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

func TestSnapshot(t *testing.T) {
	TransferType := abstract.TransferTypeSnapshotOnly
	source := mysqlrecipe.RecipeMysqlSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	helpers.InitSrcDst(helpers.TransferID, source, target, TransferType)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MySQL source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, TransferType)
	_ = helpers.Activate(t, transfer)

	rowsInDst, err := iceberg.DestinationRowCount(target, source.Database, "mysql_snapshot")
	require.NoError(t, err)
	require.Equal(t, rowsInDst, uint64(3))
}
