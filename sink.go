package iceberg

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/spf13/cast"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

// To verify providers contract implementation
var (
	_ abstract.Sinker = (*Sink)(nil)
)

type Sink struct {
	cfg        *Destination
	catalog    catalog.Catalog
	ctx        context.Context
	cancelFunc context.CancelFunc
	mu         sync.Mutex
	insertNum  int
	workerNum  int // TODO: pass worker index
	files      []string
	cp         coordinator.Coordinator
	transfer   *model.Transfer
}

// Close implements abstract.Sinker.
func (s *Sink) Close() error {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	return nil
}

// Push implements abstract.Sinker.
func (s *Sink) Push(items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}

	// Group items by table
	tableGroups := make(map[string][]abstract.ChangeItem)
	for _, item := range items {
		if !item.IsRowEvent() {
			continue
		}

		tableID := item.TableID().String()
		tableGroups[tableID] = append(tableGroups[tableID], item)
	}

	// Process each table
	for tableID, tableItems := range tableGroups {
		if err := s.processTable(tableItems); err != nil {
			return xerrors.Errorf("processing table %s: %w", tableID, err)
		}
	}

	return nil
}

func (s *Sink) processTable(items []abstract.ChangeItem) error {
	// Skip if no items
	if len(items) == 0 {
		return nil
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	// Handle table operations (create, drop, truncate)
	for _, item := range items {
		switch item.Kind {
		case abstract.DoneTableLoad:
			if err := s.cp.SetTransferState(s.transfer.ID, map[string]*coordinator.TransferStateData{
				fmt.Sprintf("files_for_%v", s.workerNum): {Generic: s.files},
			}); err != nil {
				return xerrors.Errorf("set transfer state: %w", err)
			}
			return nil
		case abstract.DoneShardedTableLoad:
			state, err := s.cp.GetTransferState(s.transfer.ID)
			if err != nil {
				return xerrors.Errorf("get transfer state: %w", err)
			}
			var files []string
			for k, v := range state {
				if !strings.Contains(k, "files_for_") {
					continue
				}
				files = append(files, v.Generic.([]string)...)
			}
			tbl, err := s.ensureTable(ctx, item)
			if err != nil {
				return xerrors.Errorf("ensure table: %w", err)
			}
			tx := tbl.NewTransaction()
			if err := tx.AddFiles(files, s.cfg.SnapshotProps, false); err != nil {
				return xerrors.Errorf("add files: %w", err)
			}

			if _, err := tx.Commit(ctx); err != nil {
				return xerrors.Errorf("commit transaction: %w", err)
			}
			return nil
		case abstract.DropTableKind, abstract.TruncateTableKind:
			tblIdent := s.createTableIdent(item)

			// load table to emulate check for existence
			_, err := s.catalog.LoadTable(ctx, tblIdent, iceberg.Properties{})
			if err != nil {
				// table exist, skip
				continue
			}

			// drop table
			if err := s.catalog.DropTable(ctx, tblIdent); err != nil {
				return xerrors.Errorf("drop table: %w", err)
			}

			// for TRUNCATE we do drop and create
			if item.Kind == abstract.TruncateTableKind {
				schema, err := convertToIcebergSchema(item.TableSchema)
				if err != nil {
					return xerrors.Errorf("convert schema for truncate: %w", err)
				}

				// create table
				_, err = s.catalog.CreateTable(ctx, tblIdent, schema)
				if err != nil {
					return xerrors.Errorf("recreate table after truncate: %w", err)
				}
			}

			return nil
		}
	}

	// Ensure the table exists
	tbl, err := s.ensureTable(ctx, items[0])
	if err != nil {
		return xerrors.Errorf("ensure table: %w", err)
	}

	// Convert data to Arrow format and write to parquet
	return s.writeDataToTable(ctx, tbl, items)
}

func (s *Sink) createTableIdent(item abstract.ChangeItem) table.Identifier {
	return table.Identifier{item.Schema, item.Table}
}

func (s *Sink) ensureTable(ctx context.Context, item abstract.ChangeItem) (*table.Table, error) {
	tbl := s.createTableIdent(item)

	existingTable, err := s.catalog.LoadTable(ctx, tbl, iceberg.Properties{})
	if err == nil {
		return existingTable, nil
	}

	schema, err := convertToIcebergSchema(item.TableSchema)
	if err != nil {
		return nil, xerrors.Errorf("converting to IcebergSchema: %w", err)
	}

	itable, err := s.catalog.CreateTable(ctx, tbl, schema)
	if err != nil {
		return nil, xerrors.Errorf("creating table: %w", err)
	}
	return itable, nil
}

func (s *Sink) writeDataToTable(ctx context.Context, tbl *table.Table, items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}
	return backoff.Retry(func() error {
		return s.writeBatch(ctx, tbl, items)
	}, backoff.NewExponentialBackOff())
}

func (s *Sink) writeBatch(ctx context.Context, tbl *table.Table, items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}
	fileIO, ok := tbl.FS().(io.WriteFileIO)
	if !ok {
		return xerrors.Errorf("%T does not implement io.WriteFileIO", tbl.FS())
	}
	fName := s.fileName(tbl)
	fw, err := fileIO.Create(fName)
	if err != nil {
		return xerrors.Errorf("create file writer: %w", err)
	}
	arrSchema, err := table.SchemaToArrowSchema(
		tbl.Schema(),
		map[string]string{},
		false,
		false,
	)
	if err != nil {
		return xerrors.Errorf("convert to ArrowSchema: %w", err)
	}
	pw, err := pqarrow.NewFileWriter(
		arrSchema,
		fw,
		nil,
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return xerrors.Errorf("create array writer: %w", err)
	}
	if err := pw.Write(toArrowRows(items, arrSchema)); err != nil {
		return xerrors.Errorf("write rows: %w", err)
	}
	s.storeFile(fName)
	return nil
}

func (s *Sink) fileName(tbl *table.Table) string {
	insertNum := s.loadInsertNum()
	return fmt.Sprintf(
		"%s/%s/%s/%05d-%d-%s-%d-%05d.parquet",
		s.cfg.Prefix,
		tbl.Identifier()[0],
		tbl.Identifier()[1],
		insertNum/10,
		insertNum%10,
		uuid.New().String(),
		s.workerNum/10000,
		s.workerNum%10000,
	)
}

func (s *Sink) loadInsertNum() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertNum++
	return s.insertNum
}

func (s *Sink) storeFile(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files = append(s.files, name)
}

func toArrowRows(items []abstract.ChangeItem, schema *arrow.Schema) arrow.Record {
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
				builder.Field(fieldIdx).(*array.Date32Builder).Append(arrow.Date32(toDate(value)))
			case arrow.TIMESTAMP:
				// Convert to milliseconds since Unix epoch
				builder.Field(fieldIdx).(*array.TimestampBuilder).Append(arrow.Timestamp(toTimestamp(value)))
			default:
				// For unsupported types, append null
				fieldBuilder.AppendNull()
			}
		}
	}

	record := builder.NewRecord()
	return record
}

func toDate(v interface{}) int32 {
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

func toTimestamp(v interface{}) int64 {
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

func convertToIcebergSchema(schema *abstract.TableSchema) (*iceberg.Schema, error) {
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

		if col.PrimaryKey {
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

func NewSink(cfg *Destination, cp coordinator.Coordinator, transfer *model.Transfer) (*Sink, error) {
	var cat catalog.Catalog
	if cfg.CatalogType == "rest" {
		var err error
		cat, err = rest.NewCatalog(context.Background(), cfg.CatalogType, cfg.CatalogURI)
		if err != nil {
			return nil, xerrors.Errorf("unable to init catalog: %w", err)
		}
	} else if cfg.CatalogType == "glue" {
		cat = glue.NewCatalog()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Sink{
		cfg:        cfg,
		catalog:    cat,
		ctx:        ctx,
		cancelFunc: cancel,
		mu:         sync.Mutex{},
		insertNum:  0,
		workerNum:  transfer.CurrentJobIndex(),
		files:      nil,
		cp:         cp,
		transfer:   transfer,
	}, nil
}
