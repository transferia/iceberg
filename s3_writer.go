package iceberg

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func fileName(prefix string, wNum, iNum int, tbl *table.Table) string {
	return fmt.Sprintf(
		"%s/%s/%s/data/%05d-%d-%s-%d-%05d.parquet",
		prefix,
		tbl.Identifier()[0],
		tbl.Identifier()[1],
		iNum/10,
		iNum%10,
		uuid.New().String(),
		wNum/10000,
		wNum%10000,
	)
}

func writeFile(fName string, tbl *table.Table, items []abstract.ChangeItem) error {
	if len(items) == 0 {
		return nil
	}
	fs, err := tbl.FS(context.Background())
	if err != nil {
		return xerrors.Errorf("get filesystem: %w", err)
	}
	fileIO, ok := fs.(io.WriteFileIO)
	if !ok {
		return xerrors.Errorf("%T does not implement io.WriteFileIO", fs)
	}
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
	record := ToArrowRows(items, arrSchema)
	defer record.Release()
	if err := pw.Write(record); err != nil {
		return xerrors.Errorf("write rows: %w", err)
	}
	if err := pw.Close(); err != nil {
		return xerrors.Errorf("close writer: %w", err)
	}
	return nil
}
