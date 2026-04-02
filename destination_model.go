package iceberg

import (
	"context"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

// To verify providers contract implementation
var (
	_ model.LoggableDestination = (*Destination)(nil)
)

type Destination struct {
	Properties       iceberg.Properties
	SnapshotProps    iceberg.Properties
	CatalogType      string
	CatalogURI       string
	Schema           string
	Prefix           string
	CommitInterval   time.Duration // Interval for committing files in streaming/replication mode
	DefaultNamespace string
	MaxBufferBytes   int64 // Max in-memory buffer per sink before forced flush (default: 64MB)
}

// CleanupMode implements model.Destination.
func (i *Destination) CleanupMode() model.CleanupType {
	return model.Drop
}

// GetProviderType implements model.Destination.
func (i *Destination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

// IsDestination implements model.Destination.
func (i *Destination) IsDestination() {
}

// Validate implements model.Destination.
func (i *Destination) Validate() error {
	return nil
}

// WithDefaults implements model.Destination.
func (i *Destination) WithDefaults() {
}

func (i *Destination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("catalog_type", i.CatalogType)
	enc.AddString("catalog_uri", i.CatalogURI)
	return nil
}

// NewCatalog creates an Iceberg catalog from the destination config.
func (i *Destination) NewCatalog() (catalog.Catalog, error) {
	switch i.CatalogType {
	case "rest":
		return rest.NewCatalog(
			context.Background(),
			i.CatalogType,
			i.CatalogURI,
			rest.WithAdditionalProps(i.Properties),
		)
	case "glue":
		return glue.NewCatalog(), nil
	default:
		return nil, xerrors.Errorf("unsupported catalog type: %s", i.CatalogType)
	}
}
