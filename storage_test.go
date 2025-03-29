package iceberg

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/transferia/iceberg/logger"

	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/util"
)

func TestStorage(t *testing.T) {
	t.Setenv("AWS_DEFAULT_CHECKSUM_VALIDATION", "0")
	src, err := SourceRecipe()
	if err != nil {
		t.Skip("No recipe defined")
	}
	storage, err := NewStorage(src, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	tables, err := storage.TableList(nil)
	require.NoError(t, err)
	require.True(t, len(tables) >= 19)
	for tid := range tables {
		t.Run(tid.String(), func(t *testing.T) {
			if tid.Namespace != "default" {
				t.Skip()
			}
			if tid.Name == "test_table_empty_list_and_map" {
				t.Skip()
			}
			_, err := storage.EstimateTableRowsCount(tid)
			require.NoError(t, err)
			st := time.Now()
			require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
				Name:   tid.Name,
				Schema: tid.Namespace,
				Filter: "",
				EtaRow: 0,
				Offset: 0,
			}, func(items []abstract.ChangeItem) error {
				totalSize := uint64(0)
				for _, r := range items {
					totalSize += util.DeepSizeof(r.ColumnValues)
				}
				logger.Log.Infof("%s-%v at %s, size: %s", tid.String(), len(items), time.Since(st), format.SizeUInt64(totalSize))
				st = time.Now()
				return nil
			}))
		})
	}
}
