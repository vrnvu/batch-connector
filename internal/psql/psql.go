package psql

import (
	"github.com/vrnvu/batch-connector/pkg/valuable"
)

// CopyFromSource is used by pgx to efficiently write batches into postgres
type CopyFromSource struct {
	errorChannel  <-chan error
	sourceChannel <-chan valuable.Valuable
	err           error
	closed        bool
	current       valuable.Valuable
}

// New returns a CopyFromSource reference defining the source and error channels
func New(sourceChannel <-chan valuable.Valuable, errorChannel <-chan error) *CopyFromSource {
	return &CopyFromSource{
		sourceChannel: sourceChannel,
		errorChannel:  errorChannel,
	}
}

// Following methods are required by pgx interface

func (c *CopyFromSource) Err() error {
	return c.err
}

func (c *CopyFromSource) Next() bool {
	if c.closed {
		return false
	}

	var open bool

	select {
	case c.current, open = <-c.sourceChannel:
	case c.err = <-c.errorChannel:
	}

	if !open {
		c.closed = true
		return false
	}

	if c.err != nil {
		return false
	}

	return true
}

// For now we assuming that all persistance database drivers will requiere some sort of method
// like Values. If keeping the valuable.Valuable type interface is not possible. Each persistance
// database will requiere its own copier.
// The batcher uses this copier to persist data into the database.
func (c *CopyFromSource) Values() ([]interface{}, error) {
	return c.current.Values(), nil
}
