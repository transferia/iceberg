package snapshot

import (
	"context"
	"fmt"
	"github.com/transferia/iceberg"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	mongocommon "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/clickhouse"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/tests/canon/mongo"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
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
		{DatabaseName: databaseName, CollectionName: "test_data2"},
	}

	doc := `{
  "_id": "D1AAD9AB",
  "floors": [
    {
      "currency": "EUR",
      "value": 0.2,
      "countryIds": [
        "IT"
      ]
    },
    {
      "currency": "EUR",
      "value": 0.3,
      "countryIds": [
        "FR",
        "GB"
      ]
    }
  ]
}`
	var masterDoc bson.D
	require.NoError(t, bson.UnmarshalExtJSON([]byte(doc), false, &masterDoc))

	require.NoError(t, mongo.InsertDocs(
		context.Background(),
		source,
		databaseName,
		"test_data2",
		masterDoc,
	))

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotOnly)
	transfer.TypeSystemVersion = 7
	transfer.Transformation = &dp_model.Transformation{Transformers: &transformer.Transformers{
		DebugMode: false,
		Transformers: []transformer.Transformer{{
			clickhouse.Type: clickhouse.Config{
				Tables: filter.Tables{
					IncludeTables: []string{fmt.Sprintf("%s.%s", databaseName, "test_data2")},
				},
				Query: `
SELECT _id,
	JSONExtractArrayRaw(document,'floors') as floors_as_string_array,
	arrayMap(x -> JSONExtractFloat(x, 'value'), JSONExtractArrayRaw(document,'floors')) as value_from_floors,
	arrayMap(x -> JSONExtractString(x, 'currency'), JSONExtractArrayRaw(document,'floors')) as currency_from_floors,
	JSONExtractRaw(assumeNotNull(document),'floors') AS floors_as_string
FROM table
SETTINGS
    function_json_value_return_type_allow_nullable = true,
    function_json_value_return_type_allow_complex = true
`,
			},
		}},
		ErrorsOutput: nil,
	}}
	_ = helpers.Activate(t, transfer)
}
