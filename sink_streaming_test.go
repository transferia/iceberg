package iceberg

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func TestStreamingSink(t *testing.T) {
	// Setup coordinator
	if os.Getenv("LOCAL") != "true" {
		t.Skip()
	}
	cp := coordinator.NewStatefulFakeClient()

	// Create destination
	dst, err := DestinationRecipe()
	require.NoError(t, err)
	dst.CommitInterval = 500 * time.Millisecond // Short interval for testing

	// Create transfer
	transfer := &model.Transfer{
		ID: uuid.New().String(),
	}

	// Create streaming sink
	sink, err := NewSinkStreaming(dst, cp, transfer, dst.CommitInterval)
	require.NoError(t, err)
	defer sink.Close()

	// Create schema columns
	cols := []abstract.ColSchema{
		{
			ColumnName: "id",
			DataType:   "INT64",
			Required:   true,
			PrimaryKey: true,
		},
		{
			ColumnName: "name",
			DataType:   "STRING",
			Required:   true,
		},
	}

	// Create table schema using helper function
	tableSchema := abstract.NewTableSchema(cols)

	// Create change items
	items := []abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Schema:       "public",
			Table:        "streaming_test",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"id", "name"},
			ColumnValues: []interface{}{int64(1), "test1"},
			Size: changeitem.EventSize{
				Read:   0,
				Values: 0,
			},
		},
		{
			Kind:         abstract.InsertKind,
			Schema:       "public",
			Table:        "streaming_test",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"id", "name"},
			ColumnValues: []interface{}{int64(2), "test2"},
			Size: changeitem.EventSize{
				Read:   0,
				Values: 0,
			},
		},
	}

	// Push data
	err = sink.Push(items)
	require.NoError(t, err)

	// Wait for commit to happen (slightly more than the interval)
	time.Sleep(time.Second)

	// Verify data was committed
	// This would normally query the actual Iceberg table
	// In a real test with a real database connection

	// Verify files were cleared after commit
	state, err := cp.GetTransferState(transfer.ID)
	require.NoError(t, err)

	// All files should have been cleared after commit
	empty := true
	for _, v := range state {
		if files, ok := v.Generic.([]string); ok {
			if len(files) > 0 {
				empty = false
				break
			}
		}
	}

	assert.True(t, empty, "Files should have been cleared after commit")
}
