package batchqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var (
	ErrAlreadyOpen               = errors.New("batchqlite: already open")
	ErrInvalidQueueDepth         = errors.New("batchqlite: maxQueueDepth must be positive")
	ErrInvalidBatchSize          = errors.New("batchqlite: maxBatchSizeToProcess must be positive")
	ErrInvalidBatchTime          = errors.New("batchqlite: maxBatchTimeToProcess must be positive")
	ErrInvalidReadPoolSize       = errors.New("batchqlite: maxReadPoolSize must be positive")
	ErrInvalidOnFullPolicy       = errors.New("batchqlite: onFullPolicy must be valid")
	ErrInvalidBusyTimeout        = errors.New("batchqlite: sqliteBusyTimeout must be positive")
	ErrInvalidCheckpointInterval = errors.New("batchqlite: sqliteCheckpointInterval must be positive")
	ErrInvalidCloseTimeout       = errors.New("batchqlite: closeTimeout must be positive")
	ErrConfigAfterOpen           = errors.New("batchqlite: cannot configure after open")
	ErrQueueFull                 = errors.New("batchqlite: queue is full")
	ErrTimeout                   = errors.New("batchqlite: wait timed out")
	ErrClosed                    = errors.New("batchqlite: batch is closed")
	ErrCloseTimedOut             = errors.New("batchqlite: close timed out before queue drained")
	ErrNotOpen                   = errors.New("batchqlite: batch has not been opened")
)

type onFullPolicy int

const (
	Block onFullPolicy = iota
	Reject
)

type writeRequestType int

const (
	loggedRequest writeRequestType = iota // Exec
	quietRequest
	respondRequest
)

type writeRequest struct {
	ctx     context.Context
	typeOf  writeRequestType
	query   string
	args    []any
	pending *Pending
}

type batchqliteConfig struct {
	maxQueueDepth            int
	maxBatchSizeToProcess    int
	maxBatchTimeToProcess    time.Duration
	maxReadPoolSize          int
	onFullPolicy             onFullPolicy
	sqliteBusyTimeout        time.Duration
	sqliteCheckpointInterval time.Duration
	abandonQueueOnClose      bool
	closeTimeout             time.Duration
	logger                   *slog.Logger
}

type Batchqlite struct {
	writeConn *sql.DB
	readPool  *sql.DB
	queue     chan writeRequest
	cfg       batchqliteConfig
	open      bool
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func (b *Batchqlite) WithMaxQueueDepth(v int) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	if v <= 0 {
		return ErrInvalidQueueDepth
	}
	b.cfg.maxQueueDepth = v

	return nil
}

func (b *Batchqlite) WithMaxBatchSizeToProcess(v int) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	if v <= 0 {
		return ErrInvalidBatchSize
	}
	b.cfg.maxBatchSizeToProcess = v

	return nil
}

func (b *Batchqlite) WithMaxBatchTimeToProcess(v time.Duration) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	if v <= 0 {
		return ErrInvalidBatchTime
	}
	b.cfg.maxBatchTimeToProcess = v

	return nil
}

func (b *Batchqlite) WithMaxReadPoolSize(v int) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	if v <= 0 {
		return ErrInvalidReadPoolSize
	}
	b.cfg.maxReadPoolSize = v

	return nil
}

func (b *Batchqlite) WithOnFullPolicy(v onFullPolicy) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	if v != Block && v != Reject {
		return ErrInvalidOnFullPolicy
	}
	b.cfg.onFullPolicy = v

	return nil
}

func (b *Batchqlite) WithSqliteBusyTimeout(v time.Duration) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	if v <= 0 {
		return ErrInvalidBusyTimeout
	}
	b.cfg.sqliteBusyTimeout = v

	return nil
}

func (b *Batchqlite) WithSqliteCheckpointInterval(v time.Duration) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	if v <= 0 {
		return ErrInvalidCheckpointInterval
	}
	b.cfg.sqliteCheckpointInterval = v

	return nil
}

func (b *Batchqlite) WithAbandonQueueOnClose(v bool) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	b.cfg.abandonQueueOnClose = v

	return nil
}

func (b *Batchqlite) WithCloseTimeout(v time.Duration) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	if v <= 0 {
		return ErrInvalidCloseTimeout
	}
	b.cfg.closeTimeout = v

	return nil
}

func (b *Batchqlite) WithLogger(l *slog.Logger) error {
	if b.open {
		return ErrConfigAfterOpen
	}
	b.cfg.logger = l

	return nil
}

func NewBatchqlite() *Batchqlite {
	defaultConfig := batchqliteConfig{
		maxQueueDepth:            1000,
		maxBatchSizeToProcess:    50,
		maxBatchTimeToProcess:    500 * time.Millisecond,
		maxReadPoolSize:          4, // Set to runtime.NumCPU() for system specificity.
		onFullPolicy:             0,
		sqliteBusyTimeout:        5 * time.Second,
		sqliteCheckpointInterval: 60 * time.Second,
		abandonQueueOnClose:      false,
		closeTimeout:             10 * time.Second,
		logger:                   slog.Default(),
	}

	b := &Batchqlite{
		cfg:  defaultConfig,
		open: false,
	}

	return b
}

// init
func (b *Batchqlite) Open(driverName, dataSourceName string) error {
	if b.open {
		return ErrAlreadyOpen
	}

	writeConn, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return err
	}

	writeConn.SetMaxOpenConns(1)
	if err = b.applyWritePragmas(writeConn); err != nil {
		return err
	}

	readPool, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		writeConn.Close()
		return err
	}

	readPool.SetMaxOpenConns(b.cfg.maxReadPoolSize)
	if err := b.applyReadPragmas(readPool); err != nil {
		writeConn.Close()
		readPool.Close()
		return err
	}

	b.writeConn = writeConn
	b.readPool = readPool
	b.queue = make(chan writeRequest, b.cfg.maxQueueDepth)
	b.stopCh = make(chan struct{})
	b.doneCh = make(chan struct{})
	b.open = true

	go b.flushLoop()
	go b.checkpointLoop()

	return nil
}

func (b *Batchqlite) Close() error {
	if !b.open {
		return ErrNotOpen
	}

	close(b.stopCh)

	if b.cfg.abandonQueueOnClose {
		<-b.doneCh
		return b.closeConns()
	}

	select {
	case <-b.doneCh:
		return b.closeConns()
	case <-time.After(b.cfg.closeTimeout):
		b.closeConns()
		return ErrCloseTimedOut
	}
}

// write
func (b *Batchqlite) Exec(ctx context.Context, query string, args ...any) error
func (b *Batchqlite) ExecQuiet(ctx context.Context, query string, args ...any) error
func (b *Batchqlite) ExecAndWait(ctx context.Context, query string, args ...any) (*Pending, error)
func (b *Batchqlite) ExecNow(ctx context.Context, query string, args ...any) error

// read
func (b *Batchqlite) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
func (b *Batchqlite) QueryRow(ctx context.Context, query string, args ...any) *sql.Row

// introspection
func (b *Batchqlite) QueueDepth() int {
	return len(b.queue)
}
func (b *Batchqlite) MaxQueueDepth() int {
	return b.cfg.maxQueueDepth
}
func (b *Batchqlite) MaxBatchSizeToProcess() int {
	return b.cfg.maxBatchSizeToProcess
}
func (b *Batchqlite) MaxBatchTimeToProcess() time.Duration {
	return b.cfg.maxBatchTimeToProcess
}
func (b *Batchqlite) MaxReadPoolSize() int {
	return b.cfg.maxReadPoolSize
}
func (b *Batchqlite) OnFullPolicy() onFullPolicy {
	return b.cfg.onFullPolicy
}
func (b *Batchqlite) SqliteBusyTimeout() time.Duration {
	return b.cfg.sqliteBusyTimeout
}
func (b *Batchqlite) SqliteCheckpointInterval() time.Duration {
	return b.cfg.sqliteCheckpointInterval
}
func (b *Batchqlite) AbandonQueueOnClose() bool {
	return b.cfg.abandonQueueOnClose
}
func (b *Batchqlite) CloseTimeout() time.Duration {
	return b.cfg.closeTimeout
}
func (b *Batchqlite) Logger() *slog.Logger {
	return b.cfg.logger
}

// internal: close
func (b *Batchqlite) closeConns() error {
	b.open = false
	werr := b.writeConn.Close()
	rerr := b.readPool.Close()
	if werr != nil {
		return werr
	}
	return rerr
}

// internal: flush loop
func (b *Batchqlite) flushLoop() {
	defer close(b.doneCh)

	ticker := time.NewTicker(b.cfg.maxBatchTimeToProcess)
	defer ticker.Stop()

	batch := make([]writeRequest, 0, b.cfg.maxBatchSizeToProcess)
	stopping := false

	for {
		select {
		case req := <-b.queue:
			batch = append(batch, req)
			if len(batch) >= b.cfg.maxBatchSizeToProcess {
				b.flushBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				b.flushBatch(batch)
				batch = batch[:0]
			}
			if stopping && len(b.queue) == 0 {
				return
			}
		case <-b.stopCh:
			if b.cfg.abandonQueueOnClose {
				return
			}

			stopping = true
			// drain remaining
			for {
				select {
				case req := <-b.queue:
					batch = append(batch, req)
					if len(batch) >= b.cfg.maxBatchSizeToProcess {
						b.flushBatch(batch)
						batch = batch[:0]
					}
				default:
					if len(batch) > 0 {
						b.flushBatch(batch)
						batch = batch[:0]
					}
					return
				}
			}

		}
	}
}

func (b *Batchqlite) flushBatch(batch []writeRequest) error {
	if len(batch) == 0 {
		return nil
	}

	allQuiet := true
	for _, req := range batch {
		if req.typeOf != quietRequest {
			allQuiet = false
			break
		}
	}

	tx, err := b.writeConn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("batchqlite: could not begin fast path transaction: %w", err)
	}

	fastResults, fastErr := b.execFastPath(tx, batch)

	var fastCommitErr error
	if fastErr == nil {
		fastCommitErr = tx.Commit()
	}

	if fastErr != nil || fastCommitErr != nil {
		tx.Rollback()

		if allQuiet {
			return nil
		}

		saveTx, err := b.writeConn.BeginTx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("batchqlite: could not begin savepoint transaction: %w", err)
		}

		if err = b.execWithSavepoints(saveTx, batch); err != nil {
			return err
		}
		if err = saveTx.Commit(); err != nil {
			return fmt.Errorf("batchqlite: could not commit savepoint transaction: %w", err)
		}

		return nil
	}

	for i, req := range batch {
		if req.typeOf == respondRequest {
			req.pending.complete(fastResults[i], nil)
		}
	}

	return nil
}

// internal: exec
func (b *Batchqlite) execFastPath(tx *sql.Tx, batch []writeRequest) ([]sql.Result, error) {
	if len(batch) == 0 {
		return []sql.Result{}, nil
	}

	results := make([]sql.Result, len(batch))

	for i, req := range batch {
		err := req.ctx.Err()
		if err != nil {
			if req.typeOf == respondRequest {
				req.pending.complete(nil, req.ctx.Err())
			}
			results[i] = nil
			continue
		}
		r, err := tx.Exec(req.query, req.args...)
		if err != nil {
			return []sql.Result{}, err
		}
		results[i] = r
	}

	return results, nil
}

func (b *Batchqlite) execWithSavepoints(tx *sql.Tx, batch []writeRequest) error

// internal: checkpoint
func (b *Batchqlite) checkpointLoop()
func (b *Batchqlite) checkpoint() error

// internal: pragmas
func (b *Batchqlite) applyWritePragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		fmt.Sprintf("PRAGMA busy_timeout=%d", b.cfg.sqliteBusyTimeout.Milliseconds()),
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("batchqlite: could not apply pragma %q: %w", p, err)
		}
	}

	return nil
}
func (b *Batchqlite) applyReadPragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		fmt.Sprintf("PRAGMA busy_timeout=%d", b.cfg.sqliteBusyTimeout.Milliseconds()),
		"PRAGMA query_only=ON",
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("batchqlite: could not apply pragma %q: %w", p, err)
		}
	}

	return nil
}

// internal: drain on close
func (b *Batchqlite) drain(ctx context.Context) error

/*
func main() {
  batch := NewBatchqlite()
  batch.WithMaxWait(10000)
  batch.WithDb(db)
  batch.Open()
}
*/
