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
	mgdrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const databaseName = "cdc_db"
const collectionName = "cdc_test"

// TestSnapshotAndReplication tests snapshot + CDC replication for MongoDB.
func TestSnapshotAndReplication(t *testing.T) {
	source := mongocommon.RecipeSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: source.Port},
		))
	}()

	source.Collections = []mongocommon.MongoCollection{
		{DatabaseName: databaseName, CollectionName: collectionName},
	}

	// Insert initial documents
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

	// Wait for snapshot
	time.Sleep(5 * time.Second)

	// Verify snapshot: 3 docs
	rowCount, err := iceberg.DestinationRowCount(target, databaseName, collectionName)
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount)

	// CDC: insert, update, delete via MongoDB driver
	client := mongoConnect(t, source)
	defer client.Disconnect(context.Background())
	coll := client.Database(databaseName).Collection(collectionName)

	_, err = coll.InsertOne(context.Background(), bson.D{{"_id", "4"}, {"name", "dave"}, {"val", 400}})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(),
		bson.D{{"_id", "1"}},
		bson.D{{"$set", bson.D{{"name", "alice_updated"}, {"val", 150}}}},
	)
	require.NoError(t, err)

	_, err = coll.DeleteOne(context.Background(), bson.D{{"_id", "3"}})
	require.NoError(t, err)

	// Wait for replication flush
	time.Sleep(10 * time.Second)

	// Verify: 3 + 1 insert - 1 delete = 3
	rowCount, err = iceberg.DestinationRowCount(target, databaseName, collectionName)
	require.NoError(t, err)
	require.Equal(t, uint64(3), rowCount)
}

// TestReplicationOnly tests CDC-only replication for MongoDB.
func TestReplicationOnly(t *testing.T) {
	source := mongocommon.RecipeSource()
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: source.Port},
		))
	}()

	source.Collections = []mongocommon.MongoCollection{
		{DatabaseName: databaseName, CollectionName: collectionName},
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(3 * time.Second)

	client := mongoConnect(t, source)
	defer client.Disconnect(context.Background())
	coll := client.Database(databaseName).Collection(collectionName)

	for i := 10; i < 15; i++ {
		_, err = coll.InsertOne(context.Background(), bson.D{
			{"_id", i},
			{"name", "user"},
			{"val", i * 100},
		})
		require.NoError(t, err)
	}

	time.Sleep(10 * time.Second)

	rowCount, err := iceberg.DestinationRowCount(target, databaseName, collectionName)
	require.NoError(t, err)
	require.True(t, rowCount >= 5, "expected at least 5 rows, got %d", rowCount)
}

func mongoConnect(t *testing.T, source *mongocommon.MongoSource) *mgdrv.Client {
	t.Helper()
	connStr := mongocommon.ConnectionString(source.Hosts, source.Port, source.User, string(source.Password), source.ReplicaSet)
	client, err := mgdrv.Connect(context.Background(), options.Client().ApplyURI(connStr))
	require.NoError(t, err)
	require.NoError(t, client.Ping(context.Background(), nil))
	return client
}
