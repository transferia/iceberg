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
		bson.D{{Key: "_id", Value: "1"}, {Key: "name", Value: "alice"}, {Key: "val", Value: 100}},
		bson.D{{Key: "_id", Value: "2"}, {Key: "name", Value: "bob"}, {Key: "val", Value: 200}},
		bson.D{{Key: "_id", Value: "3"}, {Key: "name", Value: "charlie"}, {Key: "val", Value: 300}},
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
		bson.D{{Key: "_id", Value: "seed"}, {Key: "name", Value: "seed"}, {Key: "val", Value: 0}},
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
			bson.D{{Key: "_id", Value: i}, {Key: "name", Value: "user"}, {Key: "val", Value: i * 100}},
		))
	}

	time.Sleep(15 * time.Second)

	rowCount, err := iceberg.DestinationRowCount(target, databaseName, collectionName)
	require.NoError(t, err)
	require.True(t, rowCount >= 5, "expected at least 5 rows, got %d", rowCount)
}
