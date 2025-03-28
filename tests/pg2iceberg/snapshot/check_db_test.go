package snapshot

import (
	"context"
	"testing"

	"github.com/transferia/iceberg"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	client2 "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

func TestSnapshot(t *testing.T) {
	var (
		TransferType = abstract.TransferTypeSnapshotOnly
		source       = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	)
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)

	helpers.InitSrcDst(helpers.TransferID, source, target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, TransferType)
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(client2.NewStatefulFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)
	//require.NoError(t, helpers.CompareStorages(t, source, target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
}
