package iceberg

import (
	"encoding/gob"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

const ProviderType = abstract.ProviderType("iceberg")

// To verify providers contract implementation
var (
	_ providers.Snapshot = (*Provider)(nil)
	_ providers.Sinker   = (*Provider)(nil)
)

func init() {
	sourceFactory := func() model.Source {
		return new(Source)
	}

	gob.Register(new(Source))
	model.RegisterSource(ProviderType, sourceFactory)
	model.RegisterDestination(ProviderType, func() model.Destination {
		return new(Destination)
	})
	abstract.RegisterProviderName(ProviderType, "Iceberg")
}

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	transfer *model.Transfer
}

// Sink implements providers.Sinker.
func (p *Provider) Sink(config middlewares.Config) (abstract.Sinker, error) {
	return NewSink(p.transfer.Dst.(*Destination))
}

func (p Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*Source)
	if !ok {
		return nil, xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}

	return NewStorage(src, p.logger, p.registry)
}
