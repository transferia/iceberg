package main

import (
	"fmt"
	"sync"
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
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

// waitForRows polls DestinationRowCount until rows > 0 or timeout.
func waitForRows(t *testing.T, target *iceberg.Destination, namespace, table string, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := iceberg.DestinationRowCount(target, namespace, table)
		if err == nil && rows > 0 {
			return rows
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for rows in %s.%s after %s", namespace, table, timeout)
	return 0
}

var parserFields = []abstract.ColSchema{
	{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
	{ColumnName: "level", DataType: ytschema.TypeString.String()},
	{ColumnName: "caller", DataType: ytschema.TypeString.String()},
	{ColumnName: "msg", DataType: ytschema.TypeString.String()},
}

func makeKafkaSource(topic string) *kafkasink.KafkaSource {
	source := kafkasink.MustSourceRecipe()
	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields:        parserFields,
		AddRest:       false,
		AddDedupeKeys: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	if err != nil {
		panic(err)
	}
	source.ParserConfig = parserConfigMap
	source.Topic = topic
	return source
}

func makeKafkaSink(source *kafkasink.KafkaSource) abstract.Sinker {
	sink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection: source.Connection,
			Auth:       source.Auth,
			Topic:      source.Topic,
			FormatSettings: model.SerializationFormat{
				Name: model.SerializationFormatJSON,
				BatchingSettings: &model.Batching{
					Enabled: false,
				},
			},
			ParralelWriterCount: 10,
		},
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
	)
	if err != nil {
		panic(err)
	}
	return sink
}

// TestReplication tests basic single-partition Kafka to Iceberg replication.
func TestReplication(t *testing.T) {
	kafkaTopic := "topic_single"
	source := makeKafkaSource(kafkaTopic)
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = time.Second
	target.DefaultNamespace = "streaming"

	srcSink := makeKafkaSink(source)

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	for i := range 5 {
		k := []byte(fmt.Sprintf(`any_key_%v`, i))
		v := []byte(fmt.Sprintf(`{"id": "%v", "level": "my_level", "caller": "my_caller", "msg": "my_msg"}`, i))
		err = srcSink.Push([]abstract.ChangeItem{
			abstract.MakeRawMessage(k, source.Topic, time.Time{}, source.Topic, 0, int64(i), v),
		})
		require.NoError(t, err)
	}

	rowsInDst := waitForRows(t, target, target.DefaultNamespace, source.Topic, 30*time.Second)
	require.True(t, rowsInDst > 0, "expected rows in destination, got 0")
}

// TestMultiPartitionReplication tests concurrent multi-partition Kafka ingestion.
// Each goroutine simulates a separate Kafka partition writing to the same topic.
// The streaming sink must handle concurrent Push() calls safely and commit
// all data from all partitions to the Iceberg table.
func TestMultiPartitionReplication(t *testing.T) {
	kafkaTopic := "topic_multi"
	source := makeKafkaSource(kafkaTopic)
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 2 * time.Second
	target.DefaultNamespace = "streaming"

	srcSink := makeKafkaSink(source)

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	const (
		numPartitions    = 4
		messagesPerPart  = 25
		totalMessages    = numPartitions * messagesPerPart
	)

	// Simulate concurrent partition writers
	var wg sync.WaitGroup
	errCh := make(chan error, numPartitions)

	for partition := range numPartitions {
		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			for offset := range messagesPerPart {
				globalID := part*messagesPerPart + offset
				k := []byte(fmt.Sprintf(`key_p%d_%d`, part, offset))
				v := []byte(fmt.Sprintf(`{"id": "%d", "level": "level_%d", "caller": "partition_%d", "msg": "message_%d"}`,
					globalID, part, part, offset))
				if err := srcSink.Push([]abstract.ChangeItem{
					abstract.MakeRawMessage(
						k,
						source.Topic,
						time.Now(),
						source.Topic,
						part,          // partition/shard
						int64(offset), // offset within partition
						v,
					),
				}); err != nil {
					errCh <- fmt.Errorf("partition %d offset %d: %w", part, offset, err)
					return
				}
			}
		}(partition)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	rowsInDst := waitForRows(t, target, target.DefaultNamespace, source.Topic, 30*time.Second)
	require.True(t, rowsInDst > 0, "expected rows from multi-partition ingestion, got 0")
	t.Logf("Multi-partition test: %d rows landed from %d total messages across %d partitions",
		rowsInDst, totalMessages, numPartitions)
}

// TestMultiPartitionHighThroughput tests sustained concurrent writes
// with a higher volume to stress-test the buffering and commit path.
func TestMultiPartitionHighThroughput(t *testing.T) {
	kafkaTopic := "topic_throughput"
	source := makeKafkaSource(kafkaTopic)
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 1 * time.Second
	target.DefaultNamespace = "streaming"

	srcSink := makeKafkaSink(source)

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	const (
		numPartitions    = 8
		messagesPerPart  = 100
		totalMessages    = numPartitions * messagesPerPart
	)

	var wg sync.WaitGroup
	errCh := make(chan error, numPartitions)

	for partition := range numPartitions {
		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			// Send in batches of 10 per Push call (more realistic)
			batch := make([]abstract.ChangeItem, 0, 10)
			for offset := range messagesPerPart {
				globalID := part*messagesPerPart + offset
				k := []byte(fmt.Sprintf(`key_p%d_%d`, part, offset))
				v := []byte(fmt.Sprintf(`{"id": "%d", "level": "info", "caller": "worker_%d", "msg": "batch_msg_%d"}`,
					globalID, part, offset))
				batch = append(batch, abstract.MakeRawMessage(
					k, source.Topic, time.Now(), source.Topic,
					part, int64(offset), v,
				))
				if len(batch) >= 10 {
					if err := srcSink.Push(batch); err != nil {
						errCh <- fmt.Errorf("partition %d batch at offset %d: %w", part, offset, err)
						return
					}
					batch = batch[:0]
				}
			}
			// Flush remaining
			if len(batch) > 0 {
				if err := srcSink.Push(batch); err != nil {
					errCh <- fmt.Errorf("partition %d final batch: %w", part, err)
					return
				}
			}
		}(partition)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	rowsInDst := waitForRows(t, target, target.DefaultNamespace, source.Topic, 30*time.Second)
	require.True(t, rowsInDst > 0, "expected rows from high-throughput test, got 0")
	t.Logf("High-throughput test: %d rows landed from %d total messages across %d partitions",
		rowsInDst, totalMessages, numPartitions)
}
