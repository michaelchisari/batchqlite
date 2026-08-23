package batchqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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
func (b *Batchqlite) Exec(ctx context.Context, query string, args ...any) error {
	if !b.open {
		return ErrNotOpen
	}

	req := writeRequest{
		ctx:     ctx,
		typeOf:  loggedRequest,
		query:   query,
		args:    args,
		pending: nil,
	}

	switch b.cfg.onFullPolicy {
	case Reject:
		select {
		case b.queue <- req:
			return nil
		default:
			return ErrQueueFull
		}
	case Block:
		select {
		case b.queue <- req:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (b *Batchqlite) ExecAndWait(ctx context.Context, query string, args ...any) (*Pending, error) {
	if !b.open {
		return nil, ErrNotOpen
	}

	p := &Pending{
		result: nil,
		done:   make(chan struct{}),
		err:    nil,
	}

	req := writeRequest{
		ctx:     ctx,
		typeOf:  respondRequest,
		query:   query,
		args:    args,
		pending: p,
	}

	switch b.cfg.onFullPolicy {
	case Reject:
		select {
		case b.queue <- req:
			return p, nil
		default:
			return nil, ErrQueueFull
		}
	case Block:
		select {
		case b.queue <- req:
			return p, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, nil

}

func (b *Batchqlite) ExecQuiet(ctx context.Context, query string, args ...any) error {
	if !b.open {
		return ErrNotOpen
	}

	req := writeRequest{
		ctx:     ctx,
		typeOf:  quietRequest,
		query:   query,
		args:    args,
		pending: nil,
	}

	switch b.cfg.onFullPolicy {
	case Reject:
		select {
		case b.queue <- req:
			return nil
		default:
			return ErrQueueFull
		}
	case Block:
		select {
		case b.queue <- req:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (b *Batchqlite) ExecNow(ctx context.Context, query string, args ...any) error {
	if !b.open {
		return ErrNotOpen
	}

	_, err := b.writeConn.ExecContext(ctx, query, args...)
	return err
}

// read
func (b *Batchqlite) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if !b.open {
		return nil, ErrNotOpen
	}

	r, err := b.readPool.QueryContext(ctx, query, args...)

	return r, err
}

func (b *Batchqlite) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	if !b.open {
		// can't return err, let ctx fail
	}

	r := b.readPool.QueryRowContext(ctx, query, args...)

	return r
}

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

// internal: close connections
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

		err = b.execWithSavepoints(saveTx, batch)
		if err != nil {
			return err
		}
		if err = saveTx.Commit(); err != nil {
			return fmt.Errorf("batchqlite: could not commit savepoint transaction: %w", err)
		}

		return nil
	}

	// close out pending requests
	for i, req := range batch {
		if req.typeOf == respondRequest {
			if req.ctx.Err() != nil {
				if dup := req.pending.complete(nil, req.ctx.Err()); dup {
					b.Logger().Error("batchqlite: duplicate complete (should not happen)", "query", req.query)
				}
			} else {
				if dup := req.pending.complete(fastResults[i], nil); dup {
					b.Logger().Error("batchqlite: duplicate complete (should not happen)", "query", req.query)
				}
			}
		}
	}

	return nil
}

// internal: exec
func (b *Batchqlite) execFastPath(tx *sql.Tx, batch []writeRequest) ([]sql.Result, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	results := make([]sql.Result, len(batch))

	for i, req := range batch {
		err := req.ctx.Err()
		if err != nil {
			if req.typeOf == loggedRequest {
				b.Logger().Error("batchqlite: query error", "query", req.query, "err", err)
			}
			results[i] = nil
			continue
		}
		r, err := tx.Exec(req.query, req.args...)
		if err != nil {
			return nil, err
		}
		results[i] = r
	}

	return results, nil
}

func (b *Batchqlite) execWithSavepoints(tx *sql.Tx, batch []writeRequest) error {
	if len(batch) == 0 {
		return nil
	}
	for i, req := range batch {
		err := req.ctx.Err()
		if err != nil {
			if req.typeOf == respondRequest {
				if dup := req.pending.complete(nil, req.ctx.Err()); dup {
					b.Logger().Error("batchqlite: duplicate complete (should not happen)", "query", req.query)
				}
			}
			if req.typeOf == loggedRequest {
				b.Logger().Error("batchqlite: query error", "query", req.query, "err", req.ctx.Err())
			}
			continue
		}

		is := strconv.Itoa(i)
		savepoint := "SAVEPOINT sp" + is
		rollback := "ROLLBACK TO sp" + is
		release := "RELEASE sp" + is

		_, err = tx.Exec(savepoint)
		if err != nil {
			return err
		}

		r, execErr := tx.Exec(req.query, req.args...)
		if execErr != nil {
			_, rollbackErr := tx.Exec(rollback)
			if rollbackErr != nil {
				return rollbackErr
			}
			if req.typeOf == respondRequest {
				if dup := req.pending.complete(nil, execErr); dup {
					b.Logger().Error("batchqlite: duplicate complete (should not happen)", "query", req.query)
				}
			}
			if req.typeOf == loggedRequest {
				b.Logger().Error("batchqlite: query error", "query", req.query, "err", execErr)
			}
		} else {
			_, releaseErr := tx.Exec(release)
			if releaseErr != nil {
				return releaseErr
			}
			if req.typeOf == respondRequest {
				if dup := req.pending.complete(r, nil); dup {
					b.Logger().Error("batchqlite: duplicate complete (should not happen)", "query", req.query)
				}
			}
			if req.typeOf == loggedRequest {
				// no log required on success
			}
		}
	}

	return nil
}

// internal: checkpoint
func (b *Batchqlite) checkpointLoop() {
	ticker := time.NewTicker(b.cfg.sqliteCheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := b.checkpoint()
			if err != nil {
				b.Logger().Error("batchqlite: checkpoint failed", "err", err)
			}
		case <-b.stopCh:
			return
		}
	}
}

func (b *Batchqlite) checkpoint() error {
	if _, err := b.writeConn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}
	return nil
}

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

// internal: validate
func validateQuery(query string) error {
	/*
	 * TODO
	 * Validate sql quickly through lexical parsing that ignores anything in '', "", [], `` and comments.
	 * Error on invalid keywords and multiple statements.
	 */
	return nil
}

/*
func main() {
  batch := NewBatchqlite()
  batch.WithMaxWait(10000)
  batch.WithDb(db)
  batch.Open()
}
*/
