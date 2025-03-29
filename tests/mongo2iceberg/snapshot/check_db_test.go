package snapshot

import (
	"context"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract/model"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	mongocommon "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/tests/canon/mongo"
	"github.com/transferia/transferia/tests/helpers"
)

const databaseName string = "db"

func TestGroup(t *testing.T) {
	source := mongocommon.RecipeSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: source.Port},
		))
	}()
	source.Collections = []mongocommon.MongoCollection{
		{DatabaseName: databaseName, CollectionName: "test_data"},
	}

	require.NoError(t, mongo.InsertDocs(
		context.Background(),
		source,
		databaseName,
		"test_data",
		mongo.SnapshotDocuments...,
	))

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotOnly)
	transfer.TypeSystemVersion = model.LatestVersion
	_ = helpers.Activate(t, transfer)

	rowsInDst, err := iceberg.DestinationRowCount(target, databaseName, "test_data")
	require.NoError(t, err)
	require.Equal(t, rowsInDst, uint64(21))
}
