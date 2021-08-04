package connector

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/vrnvu/batch-connector/internal/batcher"
	"github.com/vrnvu/batch-connector/pkg/valuable"
	"golang.org/x/sync/errgroup"
)

// Persistance table structure, tableName and columns associated
type Table struct {
	TableName   string
	ColumnNames []string
}

type Connector struct {
	timeout   int
	batchSize int64
	HTTP_URL  string
	DB_URL    string
}

// Transformer function takes a stream of bytes in form of io.ReadCloser
// And publishes them into the Valuable channel
type Transformer func(context.Context, chan valuable.Valuable, chan error, io.ReadCloser)

// New returns a Connector reference
func New(timeout int, batchSize int64, HTTP_URL, DB_URL string) *Connector {
	return &Connector{
		timeout,
		batchSize,
		HTTP_URL,
		DB_URL,
	}
}

// Run executes the connector
func (c *Connector) Run(table Table, transformer Transformer) {
	timeout := time.Now().Add(time.Duration(c.timeout) * time.Minute)

	ctx, cancel := context.WithDeadline(context.Background(), timeout)
	defer cancel()

	group, ctx := errgroup.WithContext(ctx)

	conn, err := c.connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	wait := make(chan struct{}, 1)

	recordsChannel := make(chan valuable.Valuable)
	errChannel := make(chan error, 1)

	group.Go(func() error {

		err = c.produce(ctx, recordsChannel, errChannel, transformer)
		if err != nil {
			close(wait)
			return err
		}

		close(wait)

		return <-errChannel
	})

	<-wait

	tableName := table.tableName
	columnNames := table.columnNames

	group.Go(func() error {
		errChannel := c.consume(ctx, conn, c.batchSize, recordsChannel, tableName, columnNames)
		return <-errChannel
	})

	if err := group.Wait(); err != nil {
		log.Fatalf("processing error %s", err)
	}

	fmt.Println("done")
}

// Connect stablishes and returns a pgx connection
func (c *Connector) connect(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, c.DB_URL)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Produce uses a transformer function to acquire data from a HTTP resource and publish into a channel
func (c *Connector) produce(ctx context.Context, outChannel chan valuable.Valuable, errChannel chan error, transformer Transformer) error {
	fmt.Println("started transformer")

	request, err := http.NewRequest(http.MethodGet, c.HTTP_URL, nil)
	if err != nil {
		return errors.Wrap(err, "instantiating request")
	}

	client := &http.Client{
		Timeout: 10 * time.Minute,
	}

	response, err := client.Do(request)
	if err != nil {
		return errors.Wrap(err, "doing request")
	}

	go func() {
		defer func() {
			close(outChannel)
			close(errChannel)
			response.Body.Close()
		}()

		fmt.Println("started connector transformer")
		transformer(ctx, outChannel, errChannel, response.Body)

	}()

	fmt.Println("success transformer")
	return nil
}

// Consume is responsible to publish the streamed data into a database using a batch strategy
func (c *Connector) consume(ctx context.Context, conn *pgx.Conn, batchSize int64, outChannel <-chan valuable.Valuable, tableName string, columnNames []string) chan error {

	fmt.Println("started consume")
	outErrChannel := make(chan error, 1)
	go func() {
		defer close(outErrChannel)

		batcher := batcher.New(conn, batchSize, tableName, columnNames)
		errChannel := batcher.Copy(ctx, outChannel)

		for {
			select {
			case err := <-errChannel:
				outErrChannel <- err
				return
			case <-ctx.Done():
				outErrChannel <- ctx.Err()
				return
			}
		}
	}()

	fmt.Println("gorutine consume running")
	return outErrChannel
}
