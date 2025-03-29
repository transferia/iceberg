package iceberg

import (
	"github.com/apache/iceberg-go"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

// To verify providers contract implementation
var (
	_ model.Source = (*Source)(nil)
)

type Source struct {
	Properties  iceberg.Properties
	CatalogType string
	CatalogURI  string
	Schema      string
}

func (i *Source) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (i *Source) Validate() error {
	return nil
}

func (i *Source) WithDefaults() {
}

func (i *Source) IsSource() {
}
