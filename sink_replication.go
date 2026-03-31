package iceberg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/io/gocloud" // register S3/GCS/Azure IO schemes
	"github.com/apache/iceberg-go/table"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	defaultMaxBufferBytes   = 64 * 1024 * 1024 // 64 MB
	defaultReplicationFlush = 30 * time.Second
	snapshotPropLSN         = "transferia.source.lsn"
)

// To verify providers contract implementation
var _ abstract.Sinker = (*SinkReplication)(nil)

// SinkReplication implements a CDC replication sink for Iceberg.
// It handles INSERT/UPDATE/DELETE events using Iceberg v2 equality deletes
// and RowDelta commits for atomic row-level mutations.
type SinkReplication struct {
	cfg             *Destination
	catalog         catalog.Catalog
	isTransactional bool
	ctx             context.Context
	cancelFunc      context.CancelFunc
	mu              sync.Mutex
	buffers         map[string]*tableBuffer
	tableCache      map[string]*table.Table
	commitTicker    *time.Ticker
	commitDone      chan struct{}
	schedulerExited chan struct{} // closed when scheduler goroutine exits
	transfer        *model.Transfer
	cp              coordinator.Coordinator
	workerNum       int
	lgr             log.Logger
}

// NewSinkReplication creates a new CDC replication sink.
func NewSinkReplication(cfg *Destination, cp coordinator.Coordinator, transfer *model.Transfer, logger log.Logger) (*SinkReplication, error) {
	cat, err := cfg.NewCatalog()
	if err != nil {
		return nil, xerrors.Errorf("unable to init catalog: %w", err)
	}

	_, isTransactional := cat.(catalog.TransactionalCatalog)
	if !isTransactional {
		logger.Warn("Catalog does not support multi-table transactions; commits will be per-table")
	}

	ctx, cancel := context.WithCancel(context.Background())

	flushInterval := cfg.CommitInterval
	if flushInterval <= 0 {
		flushInterval = defaultReplicationFlush
	}

	sink := &SinkReplication{
		cfg:             cfg,
		catalog:         cat,
		isTransactional: isTransactional,
		ctx:             ctx,
		cancelFunc:      cancel,
		buffers:         make(map[string]*tableBuffer),
		tableCache:      make(map[string]*table.Table),
		transfer:        transfer,
		cp:              cp,
		workerNum:       transfer.CurrentJobIndex(),
		lgr:             logger,
	}

	// Lead worker runs the commit scheduler
	if transfer.IsMain() || transfer.CurrentJobIndex() == 0 {
		sink.startCommitScheduler(flushInterval)
	}

	return sink, nil
}

// Push implements abstract.Sinker.
func (s *SinkReplication) Push(items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	maxBuf := int64(defaultMaxBufferBytes)
	if s.cfg.MaxBufferBytes > 0 {
		maxBuf = s.cfg.MaxBufferBytes
	}

	for _, item := range items {
		if !item.IsRowEvent() {
			continue
		}

		tableID := item.TableID().String()
		buf, ok := s.buffers[tableID]
		if !ok {
			buf = newTableBuffer()
			s.buffers[tableID] = buf
		}
		buf.Append(item)
	}

	// Check if any buffer exceeds threshold — trigger immediate flush
	for _, buf := range s.buffers {
		if buf.SizeBytes() >= maxBuf {
			return s.flushLocked()
		}
	}

	return nil
}

// Close implements abstract.Sinker.
func (s *SinkReplication) Close() error {
	if s.commitDone != nil {
		s.commitTicker.Stop()
		close(s.commitDone)
		<-s.schedulerExited // wait for goroutine to finish
	}

	// Final flush
	s.mu.Lock()
	err := s.flushLocked()
	s.mu.Unlock()

	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	return err
}

// startCommitScheduler starts a goroutine that periodically flushes buffered items.
func (s *SinkReplication) startCommitScheduler(interval time.Duration) {
	s.commitTicker = time.NewTicker(interval)
	s.commitDone = make(chan struct{})
	s.schedulerExited = make(chan struct{})

	go func() {
		defer close(s.schedulerExited)
		for {
			select {
			case <-s.commitTicker.C:
				s.mu.Lock()
				if err := s.flushLocked(); err != nil {
					s.lgr.Error("replication flush error", log.Error(err))
				}
				s.mu.Unlock()
			case <-s.commitDone:
				return
			}
		}
	}()
}

// flushLocked processes all buffered items. Must be called with s.mu held.
func (s *SinkReplication) flushLocked() error {
	if len(s.buffers) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	// Drain all buffers
	type tableFlushData struct {
		items  []abstract.ChangeItem
		schema *abstract.TableSchema
		maxLSN uint64
	}
	tableBatches := make(map[string]*tableFlushData)
	for tableID, buf := range s.buffers {
		if buf.Len() == 0 {
			continue
		}
		items, schema, maxLSN := buf.DrainAndReset()
		tableBatches[tableID] = &tableFlushData{items: items, schema: schema, maxLSN: maxLSN}
	}

	if len(tableBatches) == 0 {
		return nil
	}

	// Process each table and collect commit data
	commitData := make(map[string]*tableCommitDataInternal)

	for tableID, batch := range tableBatches {
		tbl, err := s.ensureTable(ctx, batch.items[0])
		if err != nil {
			return xerrors.Errorf("ensure table %s: %w", tableID, err)
		}

		// Schema evolution: detect and apply new columns
		if batch.schema != nil {
			tbl, err = s.reconcileSchema(ctx, tbl, batch.schema)
			if err != nil {
				return xerrors.Errorf("reconcile schema %s: %w", tableID, err)
			}
		}

		// Deduplicate and split into inserts + deletes
		inserts, deletes := prepareChanges(batch.items)

		var dataFiles []iceberg.DataFile

		// Write insert data files (WriteRecords is table-level, no tx needed)
		if len(inserts) > 0 {
			arrowSch, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
			if err != nil {
				return xerrors.Errorf("arrow schema for %s: %w", tableID, err)
			}
			records := toInsertIterator(inserts, arrowSch)
			for df, err := range table.WriteRecords(ctx, tbl, arrowSch, records) {
				if err != nil {
					return xerrors.Errorf("write records for %s: %w", tableID, err)
				}
				dataFiles = append(dataFiles, df)
			}
		}

		if len(dataFiles) > 0 || len(deletes) > 0 {
			commitData[tableID] = &tableCommitDataInternal{
				tbl:       tbl,
				dataFiles: dataFiles,
				deletes:   deletes,
				maxLSN:    batch.maxLSN,
			}
		}
	}

	if len(commitData) == 0 {
		return nil
	}

	return s.commitAllInternal(ctx, commitData)
}

// ensureTable loads or creates the Iceberg table.
func (s *SinkReplication) ensureTable(ctx context.Context, item abstract.ChangeItem) (*table.Table, error) {
	ident := s.createTableIdent(item)
	cacheKey := tableCacheKey(ident)

	if tbl, ok := s.tableCache[cacheKey]; ok {
		return tbl, nil
	}

	tbl, err := s.catalog.LoadTable(ctx, ident)
	if err == nil {
		s.tableCache[cacheKey] = tbl
		return tbl, nil
	}

	// Table doesn't exist — ensure namespace exists, then create with format-version=2
	ns := table.Identifier{ident[0]}
	if exists, _ := s.catalog.CheckNamespaceExists(ctx, ns); !exists {
		_ = s.catalog.CreateNamespace(ctx, ns, nil)
	}

	schema, err := ConvertToIcebergSchema(item.TableSchema)
	if err != nil {
		return nil, xerrors.Errorf("converting schema: %w", err)
	}

	tbl, err = s.catalog.CreateTable(ctx, ident, schema,
		catalog.WithProperties(iceberg.Properties{
			"format-version": "2",
		}),
	)
	if err != nil {
		return nil, xerrors.Errorf("creating table: %w", err)
	}

	s.lgr.Info("Created Iceberg table", log.String("table", cacheKey))
	s.tableCache[cacheKey] = tbl
	return tbl, nil
}

// tableCacheKey returns a consistent cache key from a table identifier.
func tableCacheKey(ident table.Identifier) string {
	return strings.Join(ident, ".")
}

// createTableIdent builds a table identifier from a ChangeItem.
func (s *SinkReplication) createTableIdent(item abstract.ChangeItem) table.Identifier {
	schema := item.Schema
	if schema == "" {
		schema = s.cfg.DefaultNamespace
	}
	return table.Identifier{schema, item.Table}
}

// reconcileSchema evolves the Iceberg table schema if the incoming data has new columns.
func (s *SinkReplication) reconcileSchema(ctx context.Context, tbl *table.Table, incomingSchema *abstract.TableSchema) (*table.Table, error) {
	iceSchema := tbl.Schema()
	var newCols []abstract.ColSchema

	for _, col := range incomingSchema.Columns() {
		colName := col.ColumnName
		if colName == "_partition" {
			colName = "_partition_tr"
		}
		if _, ok := iceSchema.FindFieldByName(colName); !ok {
			newCols = append(newCols, col)
		}
	}

	if len(newCols) == 0 {
		return tbl, nil
	}

	tx := tbl.NewTransaction()
	us := tx.UpdateSchema(true, false)
	for _, col := range newCols {
		colName := col.ColumnName
		if colName == "_partition" {
			colName = "_partition_tr"
		}
		iceType := colTypeToIcebergType(col)
		us.AddColumn([]string{colName}, iceType, "", false, nil)
	}
	if err := us.Commit(); err != nil {
		return nil, xerrors.Errorf("update schema: %w", err)
	}

	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return nil, xerrors.Errorf("commit schema update: %w", err)
	}

	cacheKey := tableCacheKey(tbl.Identifier())
	s.tableCache[cacheKey] = newTbl
	s.lgr.Info("Evolved table schema", log.String("table", cacheKey), log.Int("new_columns", len(newCols)))
	return newTbl, nil
}

// commitAllInternal commits all table changes, using MultiTableTransaction if available.
func (s *SinkReplication) commitAllInternal(ctx context.Context, data map[string]*tableCommitDataInternal) error {
	if s.isTransactional && len(data) > 1 {
		return s.commitMultiTable(ctx, data)
	}
	return s.commitPerTable(ctx, data)
}

type tableCommitDataInternal struct {
	tbl       *table.Table
	dataFiles []iceberg.DataFile
	deletes   []abstract.ChangeItem // raw delete items; equality deletes written inside commit tx
	maxLSN    uint64
}

// writeDeletesAndBuildRowDelta writes equality delete files and builds a RowDelta
// within a single transaction so that delete files and their RowDelta commit share
// the same transaction context.
func (s *SinkReplication) writeDeletesAndBuildRowDelta(ctx context.Context, tableID string, d *tableCommitDataInternal) (*table.Transaction, error) {
	tx := d.tbl.NewTransaction()
	snapshotProps := s.buildSnapshotProps(d.maxLSN)
	rd := tx.NewRowDelta(snapshotProps)

	rd.AddRows(d.dataFiles...)

	if len(d.deletes) > 0 {
		ids := pkFieldIDs(d.tbl.Schema())
		if len(ids) == 0 {
			return nil, xerrors.Errorf("table %s has no primary key; cannot write equality deletes", tableID)
		}

		delArrowSch, err := deleteArrowSchema(d.tbl)
		if err != nil {
			return nil, xerrors.Errorf("delete arrow schema for %s: %w", tableID, err)
		}
		deleteRecords := toDeleteIterator(d.deletes, d.tbl, delArrowSch)

		deleteFiles, err := tx.WriteEqualityDeletes(ctx, ids, deleteRecords)
		if err != nil {
			return nil, xerrors.Errorf("write equality deletes for %s: %w", tableID, err)
		}
		rd.AddDeletes(deleteFiles...)
	}

	if err := rd.Commit(ctx); err != nil {
		return nil, xerrors.Errorf("row delta for %s: %w", tableID, err)
	}
	return tx, nil
}

func (s *SinkReplication) commitMultiTable(ctx context.Context, data map[string]*tableCommitDataInternal) error {
	mtx, err := catalog.NewMultiTableTransaction(s.catalog)
	if err != nil {
		s.lgr.Warn("Multi-table transaction failed to init, falling back to per-table", log.Error(err))
		return s.commitPerTable(ctx, data)
	}

	for tableID, d := range data {
		tx, err := s.writeDeletesAndBuildRowDelta(ctx, tableID, d)
		if err != nil {
			return err
		}
		if err := mtx.AddTransaction(tx); err != nil {
			return xerrors.Errorf("add transaction for %s: %w", tableID, err)
		}
	}

	tables, err := mtx.CommitAndReload(ctx)
	if err != nil {
		return xerrors.Errorf("multi-table commit: %w", err)
	}

	// Update table cache with reloaded tables
	for i, tbl := range tables {
		ident := tbl.Identifier()
		if len(ident) >= 2 {
			key := tableCacheKey(ident)
			s.tableCache[key] = tables[i]
		}
	}

	return nil
}

func (s *SinkReplication) commitPerTable(ctx context.Context, data map[string]*tableCommitDataInternal) error {
	for tableID, d := range data {
		tx, err := s.writeDeletesAndBuildRowDelta(ctx, tableID, d)
		if err != nil {
			return err
		}
		newTbl, err := tx.Commit(ctx)
		if err != nil {
			return xerrors.Errorf("commit for %s: %w", tableID, err)
		}
		s.tableCache[tableID] = newTbl
	}
	return nil
}

func (s *SinkReplication) buildSnapshotProps(maxLSN uint64) iceberg.Properties {
	props := make(iceberg.Properties)
	for k, v := range s.cfg.SnapshotProps {
		props[k] = v
	}
	if maxLSN > 0 {
		props[snapshotPropLSN] = strconv.FormatUint(maxLSN, 10)
	}
	return props
}

// prepareChanges deduplicates a batch of ChangeItems per primary key and splits
// them into insert items and delete items. See plan for dedup semantics.
func prepareChanges(items []abstract.ChangeItem) (inserts []abstract.ChangeItem, deletes []abstract.ChangeItem) {
	if len(items) == 0 {
		return nil, nil
	}

	// Determine PK column names from the first item with OldKeys or TableSchema
	pkNames := extractPKNames(items)
	if len(pkNames) == 0 {
		// No PK — treat everything as inserts (append-only fallback)
		for _, item := range items {
			if item.Kind == abstract.InsertKind || item.Kind == abstract.UpdateKind {
				inserts = append(inserts, item)
			}
		}
		return inserts, nil
	}

	type rowState struct {
		kind       abstract.Kind
		insertItem *abstract.ChangeItem // the "after" row for insert/update
		deleteItem *abstract.ChangeItem // for generating delete record from OldKeys
		seenInBatch bool                // true if this PK was inserted in this batch
	}

	states := make(map[string]*rowState)

	for i := range items {
		item := &items[i]
		switch item.Kind {
		case abstract.InsertKind:
			pk := pkKeyFromColumnValues(item, pkNames)
			st, exists := states[pk]
			if !exists {
				st = &rowState{}
				states[pk] = st
			}
			st.kind = abstract.InsertKind
			st.insertItem = item
			st.seenInBatch = true

		case abstract.DeleteKind:
			pk := pkKeyFromOldKeys(item, pkNames)
			st, exists := states[pk]
			if exists && st.seenInBatch {
				// INSERT then DELETE in same batch — cancel out
				delete(states, pk)
			} else {
				if !exists {
					st = &rowState{}
					states[pk] = st
				}
				st.kind = abstract.DeleteKind
				st.deleteItem = item
				st.insertItem = nil
			}

		case abstract.UpdateKind:
			oldPK := pkKeyFromOldKeys(item, pkNames)
			newPK := pkKeyFromColumnValues(item, pkNames)

			if oldPK == newPK {
				// PK unchanged — delete old + insert new under same key
				st, exists := states[oldPK]
				if !exists {
					st = &rowState{}
					states[oldPK] = st
				}
				if !st.seenInBatch {
					st.deleteItem = item // needs equality delete for old version
				}
				st.kind = abstract.InsertKind
				st.insertItem = item
				st.seenInBatch = true
			} else {
				// PK changed — delete old PK, insert new PK
				oldSt, exists := states[oldPK]
				if exists && oldSt.seenInBatch {
					// Was inserted in this batch with old PK — just remove it
					delete(states, oldPK)
				} else {
					if !exists {
						oldSt = &rowState{}
						states[oldPK] = oldSt
					}
					oldSt.kind = abstract.DeleteKind
					oldSt.deleteItem = item
					oldSt.insertItem = nil
				}

				newSt := &rowState{
					kind:        abstract.InsertKind,
					insertItem:  item,
					seenInBatch: true,
				}
				states[newPK] = newSt
			}
		}
	}

	// Emit final states
	for _, st := range states {
		if st.insertItem != nil {
			inserts = append(inserts, *st.insertItem)
		}
		if st.deleteItem != nil {
			deletes = append(deletes, *st.deleteItem)
		}
	}

	return inserts, deletes
}

// extractPKNames gets primary key column names from the items.
func extractPKNames(items []abstract.ChangeItem) []string {
	// First try OldKeys (available on UPDATE/DELETE)
	for _, item := range items {
		if len(item.OldKeys.KeyNames) > 0 {
			return item.OldKeys.KeyNames
		}
	}
	// Fall back to TableSchema PK columns
	for _, item := range items {
		if item.TableSchema != nil {
			var pks []string
			for _, col := range item.TableSchema.Columns() {
				if col.PrimaryKey {
					pks = append(pks, col.ColumnName)
				}
			}
			if len(pks) > 0 {
				return pks
			}
		}
	}
	return nil
}

// pkKeyFromColumnValues builds a string key from PK column values in ColumnValues.
func pkKeyFromColumnValues(item *abstract.ChangeItem, pkNames []string) string {
	vals := make([]string, len(pkNames))
	for idx, pk := range pkNames {
		vals[idx] = "\x01" // sentinel for missing column
		for i, name := range item.ColumnNames {
			if name == pk && i < len(item.ColumnValues) {
				vals[idx] = fmt.Sprintf("%v", item.ColumnValues[i])
				break
			}
		}
	}
	return strings.Join(vals, "\x00")
}

// pkKeyFromOldKeys builds a string key from PK column values in OldKeys.
func pkKeyFromOldKeys(item *abstract.ChangeItem, pkNames []string) string {
	vals := make([]string, len(pkNames))
	for idx, pk := range pkNames {
		vals[idx] = "\x01" // sentinel for missing column
		for i, name := range item.OldKeys.KeyNames {
			if name == pk && i < len(item.OldKeys.KeyValues) {
				vals[idx] = fmt.Sprintf("%v", item.OldKeys.KeyValues[i])
				break
			}
		}
	}
	return strings.Join(vals, "\x00")
}
