package main

import (
	"fmt"
	"github.com/transferia/transferia/tests/helpers"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/iceberg/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	kafkasink "github.com/transferia/transferia/pkg/providers/kafka"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestReplication(t *testing.T) {
	var (
		kafkaTopic  = "topic1"
		source      = kafkasink.MustSourceRecipe()
		target, err = iceberg.DestinationRecipe()
	)
	require.NoError(t, err)
	target.CommitInterval = time.Second
	target.DefaultNamespace = "streaming"
	// prepare source

	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "level", DataType: ytschema.TypeString.String()},
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	source.ParserConfig = parserConfigMap
	source.Topic = kafkaTopic

	// write to source topic

	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection: source.Connection,
			Auth:       source.Auth,
			Topic:      source.Topic,
			FormatSettings: model.SerializationFormat{
				Name: model.SerializationFormatJSON,
				BatchingSettings: &model.Batching{
					Enabled:        false,
					Interval:       0,
					MaxChangeItems: 0,
					MaxMessageSize: 0,
				},
			},
			ParralelWriterCount: 10,
		},
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
	)
	require.NoError(t, err)

	// activate transfer
	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	for i := range 5 {
		k := []byte(fmt.Sprintf(`any_key_%v`, i))
		v := []byte(fmt.Sprintf(`{"id": "%v", "level": "my_level", "caller": "my_caller", "msg": "my_msg"}`, i))

		err = srcSink.Push([]abstract.ChangeItem{
			kafkasink.MakeKafkaRawMessage(
				source.Topic,
				time.Time{},
				source.Topic,
				0,
				0,
				k,
				v,
			),
		})
		require.NoError(t, err)
	}

	// check results
	time.Sleep(time.Second)
	rowsInSrc, err := iceberg.DestinationRowCount(target, target.DefaultNamespace, source.Topic)
	require.NoError(t, err)
	require.True(t, rowsInSrc > 0)
}
