package iceberg

import (
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/pkg/abstract"
)

// pkFieldIDs returns the identifier (primary key) field IDs from the Iceberg schema.
func pkFieldIDs(schema *iceberg.Schema) []int {
	return schema.IdentifierFieldIDs
}

// deleteArrowSchema builds the Arrow schema for equality delete records.
// It contains PK columns and, for partitioned tables, partition source columns.
func deleteArrowSchema(tbl *table.Table) (*arrow.Schema, error) {
	iceSchema := tbl.Schema()
	ids := pkFieldIDs(iceSchema)
	if len(ids) == 0 {
		return nil, fmt.Errorf("table %v has no identifier field IDs (primary key)", tbl.Identifier())
	}

	// Collect names: PK columns + partition source columns (if not already in PK)
	seen := make(map[int]struct{}, len(ids))
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		name, ok := iceSchema.FindColumnName(id)
		if !ok {
			return nil, fmt.Errorf("field ID %d not found in schema", id)
		}
		seen[id] = struct{}{}
		names = append(names, name)
	}

	spec := tbl.Spec()
	for _, f := range spec.Fields() { //nolint:gosimple
		if _, ok := seen[f.SourceID]; ok {
			continue
		}
		name, ok := iceSchema.FindColumnName(f.SourceID)
		if !ok {
			return nil, fmt.Errorf("partition source field ID %d not found in schema", f.SourceID)
		}
		seen[f.SourceID] = struct{}{}
		names = append(names, name)
	}

	projected, err := iceSchema.Select(true, names...)
	if err != nil {
		return nil, fmt.Errorf("projecting delete schema: %w", err)
	}

	return table.SchemaToArrowSchema(projected, nil, true, false)
}

// toDeleteArrowRecord builds an Arrow record containing PK column values
// (and partition source columns for partitioned tables) from OldKeys of delete items.
func toDeleteArrowRecord(items []abstract.ChangeItem, tbl *table.Table, arrowSch *arrow.Schema) (arrow.RecordBatch, error) {
	if len(items) == 0 {
		return nil, nil
	}

	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, arrowSch)
	defer builder.Release()
	builder.Reserve(len(items))

	for fieldIdx, field := range arrowSch.Fields() {
		fb := builder.Field(fieldIdx)

		for _, item := range items {
			// Try to find value in OldKeys first (PK columns)
			val, found := findValueInOldKeys(item, field.Name)
			if !found {
				// For partition source columns not in PK, look in ColumnValues
				val, found = findValueInColumns(item, field.Name)
			}
			if !found {
				fb.AppendNull()
				continue
			}

			appendArrowValue(fb, field, val)
		}
	}

	return builder.NewRecord(), nil
}

// findValueInOldKeys looks for a column value in the item's OldKeys.
func findValueInOldKeys(item abstract.ChangeItem, colName string) (interface{}, bool) {
	for i, name := range item.OldKeys.KeyNames {
		if name == colName && i < len(item.OldKeys.KeyValues) {
			return item.OldKeys.KeyValues[i], true
		}
	}
	return nil, false
}

// findValueInColumns looks for a column value in the item's ColumnValues.
func findValueInColumns(item abstract.ChangeItem, colName string) (interface{}, bool) {
	for i, name := range item.ColumnNames {
		if name == colName && i < len(item.ColumnValues) {
			return item.ColumnValues[i], true
		}
	}
	return nil, false
}

// appendArrowValue appends a single value to an Arrow field builder with type conversion.
// Note: TypeAny (JSON) columns are not expected in delete keys (PK/partition columns),
// so no special JSON marshaling is needed here unlike ToArrowRows.
func appendArrowValue(fb array.Builder, field arrow.Field, value interface{}) {
	if value == nil {
		fb.AppendNull()
		return
	}

	switch field.Type.ID() {
	case arrow.INT8:
		fb.(*array.Int8Builder).Append(cast.ToInt8(value))
	case arrow.INT16:
		fb.(*array.Int16Builder).Append(cast.ToInt16(value))
	case arrow.INT32:
		fb.(*array.Int32Builder).Append(cast.ToInt32(value))
	case arrow.INT64:
		fb.(*array.Int64Builder).Append(cast.ToInt64(value))
	case arrow.UINT8:
		fb.(*array.Uint8Builder).Append(cast.ToUint8(value))
	case arrow.UINT16:
		fb.(*array.Uint16Builder).Append(cast.ToUint16(value))
	case arrow.UINT32:
		fb.(*array.Uint32Builder).Append(cast.ToUint32(value))
	case arrow.UINT64:
		fb.(*array.Uint64Builder).Append(cast.ToUint64(value))
	case arrow.FLOAT32:
		fb.(*array.Float32Builder).Append(cast.ToFloat32(value))
	case arrow.FLOAT64:
		fb.(*array.Float64Builder).Append(cast.ToFloat64(value))
	case arrow.BINARY:
		if b, ok := value.([]byte); ok {
			fb.(*array.BinaryBuilder).Append(b)
		} else {
			fb.AppendNull()
		}
	case arrow.STRING:
		fb.(*array.StringBuilder).Append(cast.ToString(value))
	case arrow.BOOL:
		fb.(*array.BooleanBuilder).Append(cast.ToBool(value))
	case arrow.DATE32:
		fb.(*array.Date32Builder).Append(arrow.Date32(ToDate(value)))
	case arrow.TIMESTAMP:
		fb.(*array.TimestampBuilder).Append(arrow.Timestamp(ToTimestamp(value)))
	default:
		fb.AppendNull()
	}
}

// toInsertIterator wraps insert ChangeItems as an iter.Seq2 of Arrow RecordBatch
// for consumption by table.WriteRecords.
func toInsertIterator(items []abstract.ChangeItem, schema *arrow.Schema) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		if len(items) == 0 {
			return
		}
		record := ToArrowRows(items, schema)
		if record == nil {
			return
		}
		// WriteRecords will call Release on the batch
		yield(record, nil)
	}
}

// toDeleteIterator wraps delete ChangeItems as an iter.Seq2 of Arrow RecordBatch
// for consumption by tx.WriteEqualityDeletes.
func toDeleteIterator(items []abstract.ChangeItem, tbl *table.Table, arrowSch *arrow.Schema) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		if len(items) == 0 {
			return
		}
		record, err := toDeleteArrowRecord(items, tbl, arrowSch)
		if err != nil {
			yield(nil, err)
			return
		}
		if record == nil {
			return
		}
		yield(record, nil)
	}
}
