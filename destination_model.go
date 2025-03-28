package iceberg

import (
	"github.com/apache/iceberg-go"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

// To verify providers contract implementation
var (
	_ model.Destination = (*Destination)(nil)
)

type Destination struct {
	Properties  iceberg.Properties
	CatalogType string
	CatalogURI  string
	Schema      string
}

// CleanupMode implements model.Destination.
func (i *Destination) CleanupMode() model.CleanupType {
	return model.DisabledCleanup
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
