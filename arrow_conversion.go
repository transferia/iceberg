package iceberg

import (
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cast"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/apache/iceberg-go"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

// ToArrowRows converts abstract.ChangeItem slice to Arrow Record
func ToArrowRows(items []abstract.ChangeItem, schema *arrow.Schema) arrow.Record {
	if len(items) == 0 {
		return nil
	}

	// Initialize memory allocator
	mem := memory.NewGoAllocator()

	// Create a record builder based on the schema
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Set the number of rows we're going to add
	builder.Reserve(len(items))

	// For each field in the schema, we need to populate the corresponding array
	for fieldIdx, field := range schema.Fields() {
		fieldBuilder := builder.Field(fieldIdx)

		// For each row, set the value for this field
		for _, item := range items {
			// Check if column name exists in item's column names
			colIdx := -1
			for i, colName := range item.ColumnNames {
				if colName == field.Name {
					colIdx = i
					break
				}
			}

			// If column not found or value is nil, append null
			if colIdx == -1 || colIdx >= len(item.ColumnValues) || item.ColumnValues[colIdx] == nil {
				fieldBuilder.AppendNull()
				continue
			}

			// Value exists, append it with correct type conversion
			value := item.ColumnValues[colIdx]
			switch field.Type.ID() {
			case arrow.INT8:
				builder.Field(fieldIdx).(*array.Int8Builder).Append(cast.ToInt8(value))
			case arrow.INT16:
				builder.Field(fieldIdx).(*array.Int16Builder).Append(cast.ToInt16(value))
			case arrow.INT32:
				builder.Field(fieldIdx).(*array.Int32Builder).Append(cast.ToInt32(value))
			case arrow.INT64:
				builder.Field(fieldIdx).(*array.Int64Builder).Append(cast.ToInt64(value))
			case arrow.UINT8:
				builder.Field(fieldIdx).(*array.Uint8Builder).Append(cast.ToUint8(value))
			case arrow.UINT16:
				builder.Field(fieldIdx).(*array.Uint16Builder).Append(cast.ToUint16(value))
			case arrow.UINT32:
				builder.Field(fieldIdx).(*array.Uint32Builder).Append(cast.ToUint32(value))
			case arrow.UINT64:
				builder.Field(fieldIdx).(*array.Uint64Builder).Append(cast.ToUint64(value))
			case arrow.FLOAT32:
				builder.Field(fieldIdx).(*array.Float32Builder).Append(cast.ToFloat32(value))
			case arrow.FLOAT64:
				builder.Field(fieldIdx).(*array.Float64Builder).Append(cast.ToFloat64(value))
			case arrow.BINARY:
				if b, ok := value.([]byte); ok {
					builder.Field(fieldIdx).(*array.BinaryBuilder).Append(b)
				} else {
					builder.Field(fieldIdx).(*array.BinaryBuilder).AppendNull()
				}
			case arrow.STRING:
				if item.TableSchema.Columns()[colIdx].DataType == yt_schema.TypeAny.String() {
					jsonV, _ := json.Marshal(value)
					builder.Field(fieldIdx).(*array.StringBuilder).Append(string(jsonV))
				} else {
					builder.Field(fieldIdx).(*array.StringBuilder).Append(cast.ToString(value))
				}
			case arrow.BOOL:
				builder.Field(fieldIdx).(*array.BooleanBuilder).Append(cast.ToBool(value))
			case arrow.DATE32:
				// Convert to days since Unix epoch
				builder.Field(fieldIdx).(*array.Date32Builder).Append(arrow.Date32(ToDate(value)))
			case arrow.TIMESTAMP:
				// Convert to milliseconds since Unix epoch
				builder.Field(fieldIdx).(*array.TimestampBuilder).Append(arrow.Timestamp(ToTimestamp(value)))
			default:
				// For unsupported types, append null
				fieldBuilder.AppendNull()
			}
		}
	}

	record := builder.NewRecord()
	return record
}

// ToDate converts various date representations to int32 days since epoch
func ToDate(v interface{}) int32 {
	// Implement date conversion logic
	// This is simplified - might need more complex logic depending on your data format
	switch value := v.(type) {
	case time.Time:
		// Convert time to days since Unix epoch (1970-01-01)
		return int32(value.Unix() / (24 * 60 * 60))
	case int64:
		// Assume value is already in days since epoch
		return int32(value)
	case string:
		t, err := time.Parse("2006-01-02", value)
		if err == nil {
			return int32(t.Unix() / (24 * 60 * 60))
		}
	}
	return 0
}

// ToTimestamp converts various time representations to int64 milliseconds since epoch
func ToTimestamp(v interface{}) int64 {
	// Implement timestamp conversion logic
	switch value := v.(type) {
	case time.Time:
		return value.UnixMilli()
	case int64:
		// Assume value is already in milliseconds since epoch
		return value
	case string:
		// Try different time formats
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			t, err := time.Parse(format, value)
			if err == nil {
				return t.UnixMilli()
			}
		}
	}
	return 0
}

// ConvertToIcebergSchema converts abstract.TableSchema to iceberg.Schema
func ConvertToIcebergSchema(schema *abstract.TableSchema) (*iceberg.Schema, error) {
	if schema == nil {
		return nil, xerrors.New("schema is nil")
	}

	var fields []iceberg.NestedField
	var identifierFieldIDs []int

	nextID := 1 // probably shall use schema registry

	for _, col := range schema.Columns() {
		var fieldType iceberg.Type
		switch col.DataType {
		case yt_schema.TypeInt64.String():
			fieldType = iceberg.PrimitiveTypes.Int64
		case yt_schema.TypeInt32.String():
			fieldType = iceberg.PrimitiveTypes.Int32
		case yt_schema.TypeInt16.String(), yt_schema.TypeInt8.String():
			fieldType = iceberg.PrimitiveTypes.Int32
		case yt_schema.TypeUint64.String(), yt_schema.TypeUint32.String():
			fieldType = iceberg.PrimitiveTypes.Int64
		case yt_schema.TypeUint16.String(), yt_schema.TypeUint8.String():
			fieldType = iceberg.PrimitiveTypes.Int32
		case yt_schema.TypeFloat32.String():
			fieldType = iceberg.PrimitiveTypes.Float32
		case yt_schema.TypeFloat64.String():
			fieldType = iceberg.PrimitiveTypes.Float64
		case yt_schema.TypeBytes.String():
			fieldType = iceberg.PrimitiveTypes.Binary
		case yt_schema.TypeString.String():
			fieldType = iceberg.PrimitiveTypes.String
		case yt_schema.TypeBoolean.String():
			fieldType = iceberg.PrimitiveTypes.Bool
		case yt_schema.TypeDate.String():
			fieldType = iceberg.PrimitiveTypes.Date
		case yt_schema.TypeDatetime.String(), yt_schema.TypeTimestamp.String():
			fieldType = iceberg.PrimitiveTypes.TimestampTz
		default:
			// JSON-based string
			fieldType = iceberg.PrimitiveTypes.String
		}

		field := iceberg.NestedField{
			ID:       nextID,
			Name:     col.ColumnName,
			Type:     fieldType,
			Required: col.Required,
		}

		// iceberg allows only required primary key
		if col.PrimaryKey && col.Required {
			identifierFieldIDs = append(identifierFieldIDs, nextID)
		}

		fields = append(fields, field)
		nextID++
	}

	// for now all schemas have version 1
	icebergSchema := iceberg.NewSchema(1, fields...)
	icebergSchema.IdentifierFieldIDs = identifierFieldIDs

	return icebergSchema, nil
}
