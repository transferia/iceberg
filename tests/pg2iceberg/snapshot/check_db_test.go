package snapshot

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/transferia/iceberg"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

func dumpDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "dump", "pg")
}

func TestSnapshot(t *testing.T) {
	var (
		TransferType = abstract.TransferTypeSnapshotOnly
		source       = pgrecipe.RecipeSource(pgrecipe.WithInitDir(dumpDir()), pgrecipe.WithoutPgDump())
	)
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)

	iceberg.CleanupTable(target, "public", "__test")

	helpers.InitSrcDst(helpers.TransferID, source, target, TransferType)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, TransferType)
	require.NoError(t, err)
	require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, coordinator.NewStatefulFakeClient(), *transfer, helpers.EmptyRegistry()))

	rowsInSrc, err := iceberg.DestinationRowCount(target, "public", "__test")
	require.NoError(t, err)
	require.Equal(t, uint64(16), rowsInSrc)
}
