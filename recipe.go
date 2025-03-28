package iceberg

import (
	"os"

	go_iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"

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
			SnapshotProps: nil,
			CatalogType:   "rest",
			CatalogURI:    os.Getenv("CATALOG_ENDPOINT"),
			Schema:        "default",
			Prefix:        "s3://warehouse",
		}, nil
	}
	return nil, xerrors.New("recipe not supported")
}
