package iceberg

import (
	"context"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
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
		if err := s.processTable(tableID, tableItems); err != nil {
			return xerrors.Errorf("processing table %s: %w", tableID, err)
		}
	}

	return nil
}

func (s *Sink) processTable(tableID string, items []abstract.ChangeItem) error {
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
				// Конвертируем схему
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

	// В минималистичной реализации просто возвращаем nil
	// TODO: Реализовать запись батча в таблицу

	return nil
}

func convertToIcebergSchema(schema *abstract.TableSchema) (*iceberg.Schema, error) {
	// TODO: Реализовать конвертацию схемы из abstract.TableSchema в iceberg.Schema
	// В минималистичной реализации просто возвращаем nil, nil
	return nil, nil
}

func NewSink(cfg *Destination) (*Sink, error) {
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
	}, nil
}
