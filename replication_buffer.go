package iceberg

import (
	"github.com/transferia/transferia/pkg/abstract"
)

// tableBuffer accumulates ChangeItems for a single table between flush cycles.
// It tracks estimated memory usage and the maximum LSN seen for WAL position checkpointing.
type tableBuffer struct {
	items      []abstract.ChangeItem
	sizeBytes  int64
	lastSchema *abstract.TableSchema
	maxLSN     uint64
}

func newTableBuffer() *tableBuffer {
	return &tableBuffer{}
}

// Append adds an item to the buffer, updating size estimates and max LSN.
func (b *tableBuffer) Append(item abstract.ChangeItem) {
	b.items = append(b.items, item)
	b.sizeBytes += estimateItemSize(item)
	if item.LSN > b.maxLSN {
		b.maxLSN = item.LSN
	}
	if item.TableSchema != nil {
		b.lastSchema = item.TableSchema
	}
}

// DrainAndReset returns all buffered items and resets the buffer.
// Returns items, the last seen table schema, and the max LSN.
func (b *tableBuffer) DrainAndReset() ([]abstract.ChangeItem, *abstract.TableSchema, uint64) {
	items := b.items
	schema := b.lastSchema
	lsn := b.maxLSN

	b.items = nil
	b.sizeBytes = 0
	b.lastSchema = nil
	b.maxLSN = 0

	return items, schema, lsn
}

// SizeBytes returns the estimated memory usage of the buffer.
func (b *tableBuffer) SizeBytes() int64 {
	return b.sizeBytes
}

// Len returns the number of items in the buffer.
func (b *tableBuffer) Len() int {
	return len(b.items)
}

// estimateItemSize gives a rough byte estimate for a ChangeItem.
func estimateItemSize(item abstract.ChangeItem) int64 {
	if item.Size.Values > 0 {
		return int64(item.Size.Values)
	}
	// Rough estimate: 64 bytes per column value + 32 bytes overhead
	return int64(len(item.ColumnValues)*64 + 32)
}
