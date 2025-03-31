package iceberg

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
)

// To verify providers contract implementation
var (
	_ abstract.Sinker = (*SinkSnapshot)(nil)
)

type SinkSnapshot struct {
	cfg        *Destination
	catalog    catalog.Catalog
	ctx        context.Context
	cancelFunc context.CancelFunc
	mu         sync.Mutex
	insertNum  int
	workerNum  int
	files      []string
	cp         coordinator.Coordinator
	transfer   *model.Transfer
}

// Close implements abstract.Sinker.
func (s *SinkSnapshot) Close() error {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	return nil
}

// Push implements abstract.Sinker.
func (s *SinkSnapshot) Push(items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}

	// Group items by table
	tableGroups := make(map[string][]abstract.ChangeItem)
	for _, item := range items {
		if !item.IsRowEvent() {
			if err := s.processControlEvent(item); err != nil {
				return xerrors.Errorf("processing control event: %w", err)
			}
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

func (s *SinkSnapshot) processControlEvent(item abstract.ChangeItem) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

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
		_, err := s.catalog.LoadTable(ctx, tblIdent, s.cfg.Properties)
		if err != nil {
			// table exist, skip
			return nil
		}

		// drop table
		if err := s.catalog.DropTable(ctx, tblIdent); err != nil {
			return xerrors.Errorf("drop table: %w", err)
		}

		// for TRUNCATE we do drop and create
		if item.Kind == abstract.TruncateTableKind {
			schema, err := ConvertToIcebergSchema(item.TableSchema)
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
	return nil
}

func (s *SinkSnapshot) processTable(items []abstract.ChangeItem) error {
	// Skip if no items
	if len(items) == 0 {
		return nil
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	// Ensure the table exists
	tbl, err := s.ensureTable(ctx, items[0])
	if err != nil {
		return xerrors.Errorf("ensure table: %w", err)
	}

	// Convert data to Arrow format and write to parquet
	return s.writeDataToTable(tbl, items)
}

func (s *SinkSnapshot) createTableIdent(item abstract.ChangeItem) table.Identifier {
	return table.Identifier{item.Schema, item.Table}
}

func (s *SinkSnapshot) ensureTable(ctx context.Context, item abstract.ChangeItem) (*table.Table, error) {
	tbl := s.createTableIdent(item)

	existingTable, err := s.catalog.LoadTable(ctx, tbl, s.cfg.Properties)
	if err == nil {
		return existingTable, nil
	}

	schema, err := ConvertToIcebergSchema(item.TableSchema)
	if err != nil {
		return nil, xerrors.Errorf("converting to IcebergSchema: %w", err)
	}

	itable, err := s.catalog.CreateTable(ctx, tbl, schema)
	if err != nil {
		return nil, xerrors.Errorf("creating table: %w", err)
	}
	return itable, nil
}

func (s *SinkSnapshot) writeDataToTable(tbl *table.Table, items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}
	return s.writeBatch(tbl, items)
}

func (s *SinkSnapshot) writeBatch(tbl *table.Table, items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}
	fName := fileName(s.cfg.Prefix, s.loadInsertNum(), s.workerNum, tbl)
	if err := writeFile(fName, tbl, items); err != nil {
		return xerrors.Errorf("write file %s: %w", fName, err)
	}
	s.storeFile(fName)
	return nil
}

func (s *SinkSnapshot) loadInsertNum() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertNum++
	return s.insertNum
}

func (s *SinkSnapshot) storeFile(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files = append(s.files, name)
}

func NewSinkSnapshot(cfg *Destination, cp coordinator.Coordinator, transfer *model.Transfer) (*SinkSnapshot, error) {
	var cat catalog.Catalog
	if cfg.CatalogType == "rest" {
		var err error
		cat, err = rest.NewCatalog(
			context.Background(),
			cfg.CatalogType,
			cfg.CatalogURI,
			rest.WithAdditionalProps(cfg.Properties),
		)
		if err != nil {
			return nil, xerrors.Errorf("unable to init catalog: %w", err)
		}
	} else if cfg.CatalogType == "glue" {
		cat = glue.NewCatalog()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &SinkSnapshot{
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
