package snapshot

import (
	"context"
	"github.com/transferia/iceberg/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"testing"

	"github.com/transferia/iceberg"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
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

	helpers.InitSrcDst(helpers.TransferID, source, target, TransferType)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, TransferType)
	require.NoError(t, err)
	require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, coordinator.NewStatefulFakeClient(), *transfer, helpers.EmptyRegistry()))

	src := &iceberg.Source{
		Properties:  target.Properties,
		CatalogType: target.CatalogType,
		CatalogURI:  target.CatalogURI,
		Schema:      "public",
	}
	storage, err := iceberg.NewStorage(src, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	rowsInSrc, err := storage.ExactTableRowsCount(abstract.TableID{
		Namespace: "public",
		Name:      "__test",
	})
	require.NoError(t, err)
	require.Equal(t, rowsInSrc, uint64(16))
}
