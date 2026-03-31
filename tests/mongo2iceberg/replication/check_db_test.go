package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	mongocommon "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/tests/canon/mongo"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

const databaseName = "cdc_db"
const collectionName = "cdc_test"

func TestSnapshotAndReplication(t *testing.T) {
	source := mongocommon.RecipeSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	iceberg.CleanupTable(target, databaseName, collectionName)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: source.Port},
		))
	}()

	source.Collections = []mongocommon.MongoCollection{
		{DatabaseName: databaseName, CollectionName: collectionName},
	}

	require.NoError(t, mongo.InsertDocs(
		context.Background(),
		source,
		databaseName,
		collectionName,
		bson.D{{"_id", "1"}, {"name", "alice"}, {"val", 100}},
		bson.D{{"_id", "2"}, {"name", "bob"}, {"val", 200}},
		bson.D{{"_id", "3"}, {"name", "charlie"}, {"val", 300}},
	))

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(10 * time.Second)

	rowCount, err := iceberg.DestinationRowCount(target, databaseName, collectionName)
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount, "snapshot should land 3 docs")
}

func TestReplicationOnly(t *testing.T) {
	source := mongocommon.RecipeSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	iceberg.CleanupTable(target, databaseName, collectionName)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: source.Port},
		))
	}()

	source.Collections = []mongocommon.MongoCollection{
		{DatabaseName: databaseName, CollectionName: collectionName},
	}

	// Insert initial data for the replication slot to start from
	require.NoError(t, mongo.InsertDocs(
		context.Background(),
		source,
		databaseName,
		collectionName,
		bson.D{{"_id", "seed"}, {"name", "seed"}, {"val", 0}},
	))

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(3 * time.Second)

	for i := 10; i < 15; i++ {
		require.NoError(t, mongo.InsertDocs(
			context.Background(),
			source,
			databaseName,
			collectionName,
			bson.D{{"_id", i}, {"name", "user"}, {"val", i * 100}},
		))
	}

	time.Sleep(15 * time.Second)

	rowCount, err := iceberg.DestinationRowCount(target, databaseName, collectionName)
	require.NoError(t, err)
	require.True(t, rowCount >= 5, "expected at least 5 rows, got %d", rowCount)
}
