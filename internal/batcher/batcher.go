package batcher

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v4"
	"github.com/vrnvu/batch-connector/internal/psql"
	"github.com/vrnvu/batch-connector/pkg/valuable"
)

// Batcher defines the criterias to write a data stream to a database, only psql supported
type Batcher struct {
	conn        *pgx.Conn
	size        int64
	tableName   string
	columnNames []string
}

// New returns a Batcher reference
func New(conn *pgx.Conn, size int64, tableName string, columnNames []string) *Batcher {
	return &Batcher{
		conn:        conn,
		size:        size,
		tableName:   tableName,
		columnNames: columnNames,
	}
}

// Copy reads from a channel of valuable and persist to a database according to the batch size
func (b *Batcher) Copy(ctx context.Context, outChannel <-chan valuable.Valuable) <-chan error {
	outErrChannel := make(chan error)
	var mutex sync.Mutex
	var copyFromErr error

	copyFrom := func(batchChannel <-chan valuable.Valuable, batchErrChannel <-chan error) <-chan error {
		copyFromOutErrChannel := make(chan error)

		go func() {
			defer close(copyFromOutErrChannel)

			// only psql is supported
			copier := psql.New(batchChannel, batchErrChannel)

			_, err := b.conn.CopyFrom(ctx,
				pgx.Identifier{b.tableName},
				b.columnNames,
				copier)

			if err != nil {
				mutex.Lock()
				copyFromErr = err
				mutex.Unlock()
			}
		}()

		return copyFromOutErrChannel
	}

	go func() {
		batchErrChannel := make(chan error)
		batchChannel := make(chan valuable.Valuable)

		copyFromOutErrChannel := copyFrom(batchChannel, batchErrChannel)

		defer func() {
			close(batchErrChannel)
			close(batchChannel)
			close(outErrChannel)
		}()

		var index int64

		for {
			select {
			case n, open := <-outChannel:
				if !open {
					return
				}

				mutex.Lock()
				if copyFromErr != nil {
					outChannel = nil
					mutex.Unlock()
					outErrChannel <- copyFromErr
					return
				}
				mutex.Unlock()

				batchChannel <- n

				index++

				if index == b.size {
					close(batchErrChannel)
					close(batchChannel)

					if err := <-copyFromOutErrChannel; err != nil {
						outErrChannel <- err
						return
					}

					batchErrChannel = make(chan error)
					batchChannel = make(chan valuable.Valuable)

					copyFromOutErrChannel = copyFrom(batchChannel, batchErrChannel)
					index = 0
				}
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					batchErrChannel <- err
					outErrChannel <- err
					return
				}
			}
		}
	}()

	return outErrChannel
}
