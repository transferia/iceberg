package iceberg

import (
	"github.com/apache/iceberg-go"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

// To verify providers contract implementation
var (
	_ model.LoggableSource = (*Source)(nil)
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

func (i *Source) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("catalog_type", i.CatalogType)
	enc.AddString("catalog_uri", i.CatalogURI)
	return nil
}
