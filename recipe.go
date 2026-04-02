package iceberg

import (
	"context"
	"os"
	"time"

	"github.com/transferia/iceberg/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"

	go_iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	iceTable "github.com/apache/iceberg-go/table"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

func SourceRecipe() (*Source, error) {
	if _, ok := os.LookupEnv("AWS_S3_ENDPOINT"); ok {
		return &Source{
			Properties: go_iceberg.Properties{
				io.S3Region:          "us-east-1",
				io.S3AccessKeyID:     "admin",
				io.S3SecretAccessKey: "password",
				"type":               "rest",
			},
			CatalogType: "rest",
			CatalogURI:  os.Getenv("CATALOG_ENDPOINT"),
			Schema:      "default",
		}, nil
	}
	return nil, xerrors.New("recipe not supported")
}

func DestinationRecipe() (*Destination, error) {
	if _, ok := os.LookupEnv("AWS_S3_ENDPOINT"); ok {
		return &Destination{
			Properties: go_iceberg.Properties{
				io.S3Region:          "us-east-1",
				io.S3AccessKeyID:     "admin",
				io.S3SecretAccessKey: "password",
				"type":               "rest",
			},
			SnapshotProps:  nil,
			CatalogType:    "rest",
			CatalogURI:     os.Getenv("CATALOG_ENDPOINT"),
			Schema:         "default",
			Prefix:         "s3://warehouse",
			CommitInterval: 1 * time.Minute,
		}, nil
	}
	return nil, xerrors.New("recipe not supported")
}

// CleanupTable drops an Iceberg table if it exists (for test cleanup).
func CleanupTable(target *Destination, schema, tableName string) {
	cat, err := target.NewCatalog()
	if err != nil {
		return
	}
	ident := iceTable.Identifier{schema, tableName}
	_ = cat.DropTable(context.Background(), ident)
}

func DestinationRowCount(target *Destination, schema, table string) (uint64, error) {
	src := &Source{
		Properties:  target.Properties,
		CatalogType: target.CatalogType,
		CatalogURI:  target.CatalogURI,
		Schema:      "public",
	}
	storage, err := NewStorage(src, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	if err != nil {
		return 0, xerrors.Errorf("could not create storage: %w", err)
	}
	rowsInSrc, err := storage.ExactTableRowsCount(abstract.TableID{
		Namespace: schema,
		Name:      table,
	})
	if err != nil {
		return 0, xerrors.Errorf("could not get exact rows count: %w", err)
	}
	return rowsInSrc, nil
}

// LoadTable loads an Iceberg table by namespace and name. Useful for
// inspecting table metadata, snapshot summary, and file statistics.
// LoadTable loads an Iceberg table by namespace and name. Useful for
// inspecting table metadata, snapshot summary, and file statistics.
func LoadTable(target *Destination, namespace, tableName string) (*iceTable.Table, error) {
	cat, err := target.NewCatalog()
	if err != nil {
		return nil, xerrors.Errorf("unable to init catalog: %w", err)
	}
	tbl, err := cat.LoadTable(context.Background(), iceTable.Identifier{namespace, tableName})
	if err != nil {
		return nil, xerrors.Errorf("unable to load table: %w", err)
	}
	return tbl, nil
}
