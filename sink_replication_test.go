package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

func makeTableSchema() *abstract.TableSchema {
	return abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: "INT64", Required: true, PrimaryKey: true},
		{ColumnName: "name", DataType: "STRING", Required: false},
		{ColumnName: "value", DataType: "INT64", Required: false},
	})
}

func makeInsert(id int64, name string) abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		Schema:       "public",
		Table:        "test_table",
		TableSchema:  makeTableSchema(),
		ColumnNames:  []string{"id", "name", "value"},
		ColumnValues: []interface{}{id, name, int64(100)},
		OldKeys: changeitem.OldKeysType{
			KeyNames: []string{"id"},
		},
	}
}

func makeDelete(id int64) abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:        abstract.DeleteKind,
		Schema:      "public",
		Table:       "test_table",
		TableSchema: makeTableSchema(),
		ColumnNames: []string{"id", "name", "value"},
		OldKeys: changeitem.OldKeysType{
			KeyNames:  []string{"id"},
			KeyTypes:  []string{"INT64"},
			KeyValues: []interface{}{id},
		},
	}
}

func makeUpdate(oldID, newID int64, newName string) abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:         abstract.UpdateKind,
		Schema:       "public",
		Table:        "test_table",
		TableSchema:  makeTableSchema(),
		ColumnNames:  []string{"id", "name", "value"},
		ColumnValues: []interface{}{newID, newName, int64(200)},
		OldKeys: changeitem.OldKeysType{
			KeyNames:  []string{"id"},
			KeyTypes:  []string{"INT64"},
			KeyValues: []interface{}{oldID},
		},
	}
}

func TestPrepareChanges_InsertOnly(t *testing.T) {
	items := []abstract.ChangeItem{
		makeInsert(1, "alice"),
		makeInsert(2, "bob"),
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 2)
	assert.Len(t, deletes, 0)
}

func TestPrepareChanges_DeleteOnly(t *testing.T) {
	items := []abstract.ChangeItem{
		makeDelete(1),
		makeDelete(2),
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 0)
	assert.Len(t, deletes, 2)
}

func TestPrepareChanges_UpdatePKUnchanged(t *testing.T) {
	items := []abstract.ChangeItem{
		makeUpdate(1, 1, "alice_updated"),
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 1, "should have insert for the new row value")
	assert.Len(t, deletes, 1, "should have delete for the old row")

	// The insert should have the updated name
	assert.Equal(t, "alice_updated", inserts[0].ColumnValues[1])
}

func TestPrepareChanges_UpdatePKChanged(t *testing.T) {
	items := []abstract.ChangeItem{
		makeUpdate(1, 2, "alice_moved"),
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 1, "should have insert for new PK")
	assert.Len(t, deletes, 1, "should have delete for old PK")

	// Verify the insert has the new PK
	assert.Equal(t, int64(2), inserts[0].ColumnValues[0])
	// Verify the delete has the old PK
	assert.Equal(t, int64(1), deletes[0].OldKeys.KeyValues[0])
}

func TestPrepareChanges_InsertThenDeleteSamePK(t *testing.T) {
	items := []abstract.ChangeItem{
		makeInsert(1, "alice"),
		makeDelete(1),
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 0, "INSERT then DELETE should cancel out")
	assert.Len(t, deletes, 0, "INSERT then DELETE should cancel out")
}

func TestPrepareChanges_DeleteThenInsertSamePK(t *testing.T) {
	items := []abstract.ChangeItem{
		makeDelete(1),
		makeInsert(1, "alice_new"),
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 1, "should insert the new row")
	assert.Len(t, deletes, 1, "should delete the old row")

	assert.Equal(t, "alice_new", inserts[0].ColumnValues[1])
}

func TestPrepareChanges_InsertThenUpdateSamePK(t *testing.T) {
	items := []abstract.ChangeItem{
		makeInsert(1, "alice"),
		makeUpdate(1, 1, "alice_updated"),
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 1, "should collapse to single insert with updated values")
	assert.Len(t, deletes, 0, "no delete needed since row was inserted in this batch")

	// The insert should have the updated values
	assert.Equal(t, "alice_updated", inserts[0].ColumnValues[1])
}

func TestPrepareChanges_MultipleUpdatesOnSamePK(t *testing.T) {
	items := []abstract.ChangeItem{
		makeUpdate(1, 1, "v1"),
		makeUpdate(1, 1, "v2"),
		makeUpdate(1, 1, "v3"),
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 1, "should collapse to single insert")
	assert.Len(t, deletes, 1, "should have one delete for the original row")

	// Should have the last update's values
	assert.Equal(t, "v3", inserts[0].ColumnValues[1])
}

func TestPrepareChanges_MixedOperations(t *testing.T) {
	items := []abstract.ChangeItem{
		makeInsert(1, "alice"),
		makeInsert(2, "bob"),
		makeDelete(3),          // delete existing row 3
		makeUpdate(2, 2, "bob_updated"), // update bob
		makeInsert(4, "charlie"),
		makeDelete(1),          // delete alice (inserted in this batch)
	}

	inserts, deletes := prepareChanges(items)

	// Row 1: INSERT then DELETE -> cancel out
	// Row 2: INSERT then UPDATE (same PK) -> single INSERT with updated values
	// Row 3: DELETE -> delete
	// Row 4: INSERT -> insert
	assert.Len(t, inserts, 2, "should have bob_updated and charlie")
	assert.Len(t, deletes, 1, "should have delete for row 3")
}

func TestPrepareChanges_NoPrimaryKey(t *testing.T) {
	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: "INT64", Required: false, PrimaryKey: false},
		{ColumnName: "name", DataType: "STRING", Required: false},
	})

	items := []abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Schema:       "public",
			Table:        "test_table",
			TableSchema:  schema,
			ColumnNames:  []string{"id", "name"},
			ColumnValues: []interface{}{int64(1), "alice"},
			OldKeys:      changeitem.OldKeysType{},
		},
	}

	inserts, deletes := prepareChanges(items)
	assert.Len(t, inserts, 1, "no PK -> append-only fallback")
	assert.Len(t, deletes, 0, "no PK -> no deletes possible")
}

func TestPrepareChanges_EmptyBatch(t *testing.T) {
	inserts, deletes := prepareChanges(nil)
	assert.Nil(t, inserts)
	assert.Nil(t, deletes)
}

// --- Buffer tests ---

func TestTableBuffer_Basic(t *testing.T) {
	buf := newTableBuffer()
	assert.Equal(t, 0, buf.Len())
	assert.Equal(t, int64(0), buf.SizeBytes())

	item := makeInsert(1, "alice")
	item.LSN = 100
	buf.Append(item)

	assert.Equal(t, 1, buf.Len())
	assert.Greater(t, buf.SizeBytes(), int64(0))

	item2 := makeInsert(2, "bob")
	item2.LSN = 200
	buf.Append(item2)

	assert.Equal(t, 2, buf.Len())

	items, schema, maxLSN := buf.DrainAndReset()
	assert.Len(t, items, 2)
	assert.NotNil(t, schema)
	assert.Equal(t, uint64(200), maxLSN)

	// Buffer should be empty after drain
	assert.Equal(t, 0, buf.Len())
	assert.Equal(t, int64(0), buf.SizeBytes())
}

func TestTableBuffer_SizeEstimate(t *testing.T) {
	buf := newTableBuffer()

	item := makeInsert(1, "alice")
	item.Size = changeitem.EventSize{Values: 1024}
	buf.Append(item)

	assert.Equal(t, int64(1024), buf.SizeBytes())
}

// --- PK extraction tests ---

func TestExtractPKNames_FromOldKeys(t *testing.T) {
	items := []abstract.ChangeItem{
		makeDelete(1),
	}
	pks := extractPKNames(items)
	require.Len(t, pks, 1)
	assert.Equal(t, "id", pks[0])
}

func TestExtractPKNames_FromTableSchema(t *testing.T) {
	items := []abstract.ChangeItem{
		makeInsert(1, "alice"),
	}
	// makeInsert has empty OldKeys.KeyNames for the key itself but
	// still has KeyNames: []string{"id"} set. Let's test with truly empty.
	items[0].OldKeys = changeitem.OldKeysType{}

	pks := extractPKNames(items)
	require.Len(t, pks, 1)
	assert.Equal(t, "id", pks[0])
}

func TestPkKeyFromColumnValues(t *testing.T) {
	item := makeInsert(42, "alice")
	key := pkKeyFromColumnValues(&item, []string{"id"})
	assert.Equal(t, "42", key)
}

func TestPkKeyFromOldKeys(t *testing.T) {
	item := makeDelete(42)
	key := pkKeyFromOldKeys(&item, []string{"id"})
	assert.Equal(t, "42", key)
}
