package iceberg

import (
	"context"
	"fmt"
	"go.ytsaurus.tech/library/go/core/log"
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
	_ abstract.Sinker = (*SinkStreaming)(nil)
)

// SinkStreaming implements a streaming sink for Iceberg.
// Unlike SinkSnapshot, this sink:
// 1. Doesn't handle control events (no table drop/truncate)
// 2. Creates tables on first push
// 3. Commits files to tables at regular intervals
type SinkStreaming struct {
	cfg           *Destination
	catalog       catalog.Catalog
	ctx           context.Context
	cancelFunc    context.CancelFunc
	mu            sync.Mutex
	insertNum     int
	workerNum     int
	files         map[string][]string // Map of tableID -> file paths
	cp            coordinator.Coordinator
	transfer      *model.Transfer
	commitTicker  *time.Ticker
	commitDone    chan bool
	commitTimeout time.Duration
	lgr           log.Logger
}

// Close implements abstract.Sinker.
func (s *SinkStreaming) Close() error {
	if s.commitTicker != nil {
		s.commitTicker.Stop()
		s.commitDone <- true
	}
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	return nil
}

// Push implements abstract.Sinker.
func (s *SinkStreaming) Push(items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}

	// Group items by table
	tableGroups := make(map[string][]abstract.ChangeItem)
	for _, item := range items {
		// Skip non-row events in streaming mode
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

func (s *SinkStreaming) processTable(items []abstract.ChangeItem) error {
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
	return s.writeBatch(tbl, items)
}

func (s *SinkStreaming) createTableIdent(item abstract.ChangeItem) table.Identifier {
	schema := item.Schema
	if schema == "" {
		schema = s.cfg.DefaultNamespace
	}
	return table.Identifier{schema, item.Table}
}

func (s *SinkStreaming) ensureTable(ctx context.Context, item abstract.ChangeItem) (*table.Table, error) {
	tblIdent := s.createTableIdent(item)

	// Try to load existing table
	existingTable, err := s.catalog.LoadTable(ctx, tblIdent, s.cfg.Properties)
	if err == nil {
		s.lgr.Infof("table %s already exists: props: %v", tblIdent, s.cfg.Properties)
		return existingTable, nil
	}

	// Create new table
	schema, err := ConvertToIcebergSchema(item.TableSchema)
	if err != nil {
		return nil, xerrors.Errorf("converting to IcebergSchema: %w", err)
	}

	if _, err := s.catalog.CreateTable(ctx, tblIdent, schema); err != nil {
		return nil, xerrors.Errorf("creating table: %w", err)
	}

	return s.catalog.LoadTable(ctx, tblIdent, s.cfg.Properties)
}

func (s *SinkStreaming) writeBatch(tbl *table.Table, items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}

	tableID := items[0].TableID().String()

	fName := fileName(s.cfg.Prefix, s.loadInsertNum(), s.workerNum, tbl)
	if err := writeFile(fName, tbl, items); err != nil {
		return xerrors.Errorf("write file %s: %w", fName, err)
	}

	s.storeFile(tableID, fName)

	// Store files in coordinator
	if err := s.updateFilesInCoordinator(); err != nil {
		return xerrors.Errorf("update files in coordinator: %w", err)
	}

	return nil
}

func (s *SinkStreaming) loadInsertNum() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertNum++
	return s.insertNum
}

func (s *SinkStreaming) storeFile(tableID, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[tableID] = append(s.files[tableID], name)
}

func (s *SinkStreaming) updateFilesInCoordinator() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filesData := make(map[string]*coordinator.TransferStateData)
	for tableID, files := range s.files {
		if len(files) == 0 {
			continue
		}
		key := fmt.Sprintf("streaming_files_%s_%v", tableID, s.workerNum)
		filesData[key] = &coordinator.TransferStateData{Generic: files}
	}

	if len(filesData) == 0 {
		return nil
	}

	if err := s.cp.SetTransferState(s.transfer.ID, filesData); err != nil {
		return xerrors.Errorf("set transfer state: %w", err)
	}

	return nil
}

// startCommitScheduler starts a goroutine that periodically commits files to tables
func (s *SinkStreaming) startCommitScheduler() {
	s.commitTicker = time.NewTicker(s.commitTimeout)
	s.commitDone = make(chan bool)

	go func() {
		for {
			select {
			case <-s.commitTicker.C:
				if err := s.commitTables(); err != nil {
					// Log error but continue
					fmt.Printf("Error committing tables: %v\n", err)
				}
			case <-s.commitDone:
				// Final commit before exiting
				if err := s.commitTables(); err != nil {
					fmt.Printf("Error in final commit: %v\n", err)
				}
				return
			}
		}
	}()
}

// commitTables commits all pending files to their respective tables
func (s *SinkStreaming) commitTables() error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	// Get all files from coordinator
	state, err := s.cp.GetTransferState(s.transfer.ID)
	if err != nil {
		return xerrors.Errorf("get transfer state: %w", err)
	}

	// Group files by table
	tableFiles := make(map[string][]string)
	for key, value := range state {
		if files, ok := value.Generic.([]string); ok {
			for _, tableName := range s.getTableIDsFromKey(key) {
				tableFiles[tableName] = append(tableFiles[tableName], files...)
			}
		}
	}

	// Commit files for each table
	for tableID, files := range tableFiles {
		if len(files) == 0 {
			continue
		}

		// Extract schema and table name from tableID
		tid, _ := abstract.ParseTableID(tableID)
		if tid.Namespace == "" {
			tid.Namespace = s.cfg.DefaultNamespace
		}
		// Load table
		tblIdent := table.Identifier{tid.Namespace, tid.Name}
		tbl, err := s.catalog.LoadTable(ctx, tblIdent, s.cfg.Properties)
		if err != nil {
			continue
		}

		// Create transaction and add files
		tx := tbl.NewTransaction()
		if err := tx.AddFiles(files, s.cfg.SnapshotProps, false); err != nil {
			return xerrors.Errorf("add files for table %s: %w", tableID, err)
		}

		// Commit transaction
		if _, err := tx.Commit(ctx); err != nil {
			return xerrors.Errorf("commit transaction for table %s: %w", tableID, err)
		}

		// Clear committed files from coordinator
		if err := s.clearState(tableID); err != nil {
			return xerrors.Errorf("clear committed files for table %s: %w", tableID, err)
		}
	}

	return nil
}

// getTableIDsFromKey extracts table IDs from coordinator keys
func (s *SinkStreaming) getTableIDsFromKey(key string) []string {
	// If key is "streaming_files_{tableID}_{workerNum}", extract tableID
	var result []string

	if tableID := extractTableIDFromKey(key); tableID != "" {
		result = append(result, tableID)
	}

	return result
}

// extractTableIDFromKey extracts tableID from a key like "streaming_files_{tableID}_{workerNum}"
func extractTableIDFromKey(key string) string {
	// This implementation assumes key format: "streaming_files_{tableID}_{workerNum}"
	prefix := "streaming_files_"
	if !strings.HasPrefix(key, prefix) {
		return ""
	}

	// Remove prefix
	remaining := key[len(prefix):]

	// Find the last underscore which separates tableID from workerNum
	lastUnderscore := strings.LastIndex(remaining, "_")
	if lastUnderscore == -1 {
		return ""
	}

	// Extract tableID
	return remaining[:lastUnderscore]
}

// parseTableID splits a tableID string into schema and table name
func (s *SinkStreaming) parseTableID(tableID string) []string {
	// For TableID.String() that's formatted as "{schema}.{table}"
	parts := strings.Split(tableID, ".")
	if len(parts) == 2 {
		return parts
	}
	return []string{}
}

// clearState removes committed files from coordinator
func (s *SinkStreaming) clearState(tableID string) error {
	s.mu.Lock()
	// Get all keys for this table

	state, err := s.cp.GetTransferState(s.transfer.ID)
	if err != nil {
		return xerrors.Errorf("get transfer state: %w", err)
	}

	// Clear files for this table
	for key := range state {
		if extractTableIDFromKey(key) == tableID {
			// Set empty array for this key
			if err := s.cp.RemoveTransferState(s.transfer.ID, []string{key}); err != nil {
				return xerrors.Errorf("clear files for key %s: %w", key, err)
			}
		}
	}

	// Clear local files cache
	s.files[tableID] = []string{}
	s.mu.Unlock()

	return nil
}

// NewSinkStreaming creates a new streaming sink
func NewSinkStreaming(cfg *Destination, cp coordinator.Coordinator, transfer *model.Transfer, logger log.Logger) (*SinkStreaming, error) {
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

	// If commit timeout is not specified, use default of 1 minute
	commitTimeout := cfg.CommitInterval
	if commitTimeout <= 0 {
		commitTimeout = 1 * time.Minute
	}

	sink := &SinkStreaming{
		cfg:           cfg,
		catalog:       cat,
		ctx:           ctx,
		cancelFunc:    cancel,
		mu:            sync.Mutex{},
		insertNum:     0,
		workerNum:     transfer.CurrentJobIndex(),
		files:         make(map[string][]string),
		cp:            cp,
		transfer:      transfer,
		commitTimeout: commitTimeout,
		lgr:           logger,
	}

	// Start commit scheduler
	if transfer.IsMain() || transfer.CurrentJobIndex() == 0 {
		sink.startCommitScheduler()
	}

	return sink, nil
}
