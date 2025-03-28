package iceberg

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// To verify providers contract implementation
var (
	_ abstract.Sinker = (*Sink)(nil)
)

type Sink struct {
	cfg *Destination
}

// Close implements abstract.Sinker.
func (s *Sink) Close() error {
	return nil
}

// Push implements abstract.Sinker.
func (s *Sink) Push(items []abstract.ChangeItem) error {
	return xerrors.New("not implemented")
}

func NewSink(cfg *Destination) (*Sink, error) {
	return &Sink{
		cfg: cfg,
	}, nil
}
