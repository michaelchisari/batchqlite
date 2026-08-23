package batchqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"
)

type onFullPolicy int

const (
	Block onFullPolicy = iota
	Reject
)

const (
	savepointStmt = "SAVEPOINT __batchqlite_sp"
	rollbackStmt  = "ROLLBACK TO __batchqlite_sp"
	releaseStmt   = "RELEASE __batchqlite_sp"
)

type batchqliteState uint32

const (
	stateClosed batchqliteState = iota
	stateOpening
	stateOpen
	stateClosing
	statePoisoned
)

type savepointResult struct {
	result sql.Result
	err    error
}

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
	state     atomic.Uint32 // governs whether queries can be queued
	stopCh    chan struct{}
	doneCh    chan struct{} // tracks whether background gorotuines have exited
}

func NewBatchqlite(opts ...Option) (*Batchqlite, error) {
	cfg := batchqliteConfig{
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

	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	b := &Batchqlite{cfg: cfg}

	return b, nil
}

// init
func (b *Batchqlite) Open(driverName, dataSourceName string) error {
	if batchqliteState(b.state.Load()) == statePoisoned {
		return ErrPoisoned
	}

	if !b.state.CompareAndSwap(uint32(stateClosed), uint32(stateOpening)) {
		return ErrAlreadyOpen
	}

	writeConn, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		b.state.Store(uint32(stateClosed))
		return err
	}

	writeConn.SetMaxOpenConns(1)
	if err = b.applyWritePragmas(writeConn); err != nil {
		if closeErr := writeConn.Close(); closeErr != nil {
			b.Logger().Error("batchqlite: failed to close write connection during Open cleanup", "err", closeErr)
		}

		b.state.Store(uint32(stateClosed))
		return err
	}

	readPool, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		if closeErr := writeConn.Close(); closeErr != nil {
			b.Logger().Error("batchqlite: failed to close write connection during Open cleanup", "err", closeErr)
		}
		b.state.Store(uint32(stateClosed))
		return err
	}

	readPool.SetMaxOpenConns(b.cfg.maxReadPoolSize)

	// pragma every connection
	// safe since connection cannot be changed while open
	if err := b.initReadPoolPragmas(readPool); err != nil {
		if closeErr := writeConn.Close(); closeErr != nil {
			b.Logger().Error("batchqlite: failed to close write connection during Open cleanup", "err", closeErr)
		}
		if closeErr := readPool.Close(); closeErr != nil {
			b.Logger().Error("batchqlite: failed to close read pool during Open cleanup", "err", closeErr)
		}
		b.state.Store(uint32(stateClosed))
		return err
	}

	b.writeConn = writeConn
	b.readPool = readPool
	b.queue = make(chan writeRequest, b.cfg.maxQueueDepth)
	b.stopCh = make(chan struct{})
	b.doneCh = make(chan struct{})

	go b.flushLoop()
	go b.checkpointLoop()

	b.state.Store(uint32(stateOpen))

	return nil
}

func (b *Batchqlite) Close() error {
	if batchqliteState(b.state.Load()) == statePoisoned {
		return ErrPoisoned
	}

	if !b.state.CompareAndSwap(uint32(stateOpen), uint32(stateClosing)) {
		return ErrNotOpen
	}

	close(b.stopCh)

	if b.cfg.abandonQueueOnClose {
		<-b.doneCh
		b.state.Store(uint32(stateClosed))
		return b.closeConns()
	}

	select {
	case <-b.doneCh:
		b.state.Store(uint32(stateClosed))
		return b.closeConns()
	case <-time.After(b.cfg.closeTimeout):
		closeErr := b.closeConns()
		select {
		case <-b.doneCh:
			if closeErr != nil {
				return closeErr
			}
			return ErrCloseTimedOut
		case <-time.After(b.cfg.closeTimeout):
			b.state.Store(uint32(statePoisoned))
			return ErrCloseForceFailed
		}
	}
}

// write
func (b *Batchqlite) Exec(ctx context.Context, query string, args ...any) error {
	if batchqliteState(b.state.Load()) != stateOpen {
		return ErrNotOpen
	}
	if err := validateQuery(query); err != nil {
		return err
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
		case <-b.stopCh:
			return ErrClosing
		}
	}

	return nil
}

func (b *Batchqlite) ExecAndWait(ctx context.Context, query string, args ...any) (*Pending, error) {
	if batchqliteState(b.state.Load()) != stateOpen {
		return nil, ErrNotOpen
	}
	if err := validateQuery(query); err != nil {
		return nil, err
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
		case <-b.stopCh:
			return nil, ErrClosing
		}
	}

	return nil, nil

}

func (b *Batchqlite) ExecQuiet(ctx context.Context, query string, args ...any) error {
	if batchqliteState(b.state.Load()) != stateOpen {
		return ErrNotOpen
	}
	if err := validateQuery(query); err != nil {
		return err
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
		case <-b.stopCh:
			return ErrClosing
		}
	}

	return nil
}

func (b *Batchqlite) ExecNow(ctx context.Context, query string, args ...any) error {
	if batchqliteState(b.state.Load()) != stateOpen {
		return ErrNotOpen
	}
	if err := validateQuery(query); err != nil {
		return err
	}

	_, err := b.writeConn.ExecContext(ctx, query, args...)
	return err
}

// read
func (b *Batchqlite) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if batchqliteState(b.state.Load()) != stateOpen {
		return nil, ErrNotOpen
	}

	r, err := b.readPool.QueryContext(ctx, query, args...)

	return r, err
}

func (b *Batchqlite) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	if batchqliteState(b.state.Load()) != stateOpen {
		// can't return err, let ctx fail
		// matches database/sql pattern

		// if state is closed or closing,
		// *sql.Row carries driver-level "database is closed" error

		// if readPool is nil (never opened), this will panic
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
func (b *Batchqlite) State() string {
	switch batchqliteState(b.state.Load()) {
	case stateOpening:
		return "opening"
	case stateOpen:
		return "open"
	case stateClosing:
		return "closing"
	case stateClosed:
		return "closed"
	case statePoisoned:
		return "poisoned"
	default:
		return "unknown"
	}
}

// internal: close connections
func (b *Batchqlite) closeConns() error {
	werr := b.writeConn.Close()
	rerr := b.readPool.Close()
	if werr != nil {
		if rerr != nil {
			b.Logger().Error("batchqlite: failed to close read pool", "err", rerr)
		}
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
				b.abandonQueueAndBatch(batch)
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

	tx, txErr := b.writeConn.BeginTx(context.Background(), nil)
	if txErr != nil {
		b.resolveAllAsStructuralFailure(batch)
		return fmt.Errorf("batchqlite: could not begin fast path transaction: %w", txErr)
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
			b.resolveAllAsStructuralFailure(batch)
			return fmt.Errorf("batchqlite: could not begin savepoint transaction: %w", err)
		}

		results, saveErr := b.execWithSavepoints(saveTx, batch)
		if saveErr != nil {
			b.resolveAllAsStructuralFailure(batch)
			return saveErr
		}
		if commitErr := saveTx.Commit(); commitErr != nil {
			b.resolveAllAsStructuralFailure(batch)
			return commitErr
		}

		for i, res := range results {
			if res.err != nil {
				if batch[i].typeOf == respondRequest {
					batch[i].pending.complete(nil, res.err)
				}
				if batch[i].typeOf == loggedRequest {
					b.Logger().Error("batchqlite: query error", "query", batch[i].query, "err", res.err)
				}
			} else {
				if batch[i].typeOf == respondRequest {
					batch[i].pending.complete(res.result, nil)
				}
				if batch[i].typeOf == loggedRequest {
					// no logging on successs
				}
			}
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

func (b *Batchqlite) abandonQueueAndBatch(batch []writeRequest) {
	for _, req := range batch {
		b.resolveAbandoned(req)
	}

	for {
		select {
		case req := <-b.queue:
			b.resolveAbandoned(req)
		default:
			return
		}
	}
}

func (b *Batchqlite) resolveAbandoned(req writeRequest) {
	if req.typeOf == respondRequest {
		if dup := req.pending.complete(nil, ErrAbandoned); dup {
			b.Logger().Error("batchqlite: duplicate complete (should not happen)", "query", req.query)
		}
	}
	if req.typeOf == loggedRequest {
		b.Logger().Error("batchqlite: request abandoned, batch closed with AbandonQueueOnClose", "query", req.query)
	}
}

func (b *Batchqlite) resolveAllAsStructuralFailure(batch []writeRequest) {
	for _, req := range batch {
		if req.typeOf == respondRequest {
			if dup := req.pending.complete(nil, ErrStructuralFailure); dup {
				b.Logger().Error("batchqlite: duplicate complete (should not happen)", "query", req.query)
			}
		}
		if req.typeOf == loggedRequest {
			b.Logger().Error("batchqlite: query not attempted due to structural failure", "query", req.query)
		}
	}
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

func (b *Batchqlite) execWithSavepoints(tx *sql.Tx, batch []writeRequest) ([]savepointResult, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	results := make([]savepointResult, len(batch))

	for i, req := range batch {
		ctxErr := req.ctx.Err()
		if ctxErr != nil {
			results[i].err = ctxErr
			results[i].result = nil
			continue
		}

		_, txErr := tx.Exec(savepointStmt)
		if txErr != nil {
			return nil, txErr
		}

		r, execErr := tx.Exec(req.query, req.args...)
		if execErr != nil {
			_, rollbackErr := tx.Exec(rollbackStmt)
			if rollbackErr != nil {
				return nil, rollbackErr
			}
			results[i].err = execErr
			results[i].result = nil
		} else {
			_, releaseErr := tx.Exec(releaseStmt)
			if releaseErr != nil {
				return nil, releaseErr
			}
			results[i].err = nil
			results[i].result = r
		}
	}

	return results, nil
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

func (b *Batchqlite) initReadPoolPragmas(db *sql.DB) error {
	conns := make([]*sql.Conn, 0, b.cfg.maxReadPoolSize)

	defer func() {
		for _, c := range conns {
			if err := c.Close(); err != nil {
				b.Logger().Error("batchqlite: failed to release read connection during setup", "err", err)
			}
		}
	}()

	walMode := "PRAGMA journal_mode=WAL"
	if _, err := db.Exec(walMode); err != nil {
		return fmt.Errorf("batchqlite: could not apply pragma %q: %w", walMode, err)
	}

	ctx := context.Background()

	pragmas := []string{
		fmt.Sprintf("PRAGMA busy_timeout=%d", b.cfg.sqliteBusyTimeout.Milliseconds()),
		"PRAGMA query_only=ON",
	}

	for i := 0; i < b.cfg.maxReadPoolSize; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("batchqlite: could not reserve read connection %d: %w", i, err)
		}
		conns = append(conns, conn)

		for _, p := range pragmas {
			if _, err := conn.ExecContext(ctx, p); err != nil {
				return fmt.Errorf("batchqlite: could not apply pragma %q: %w", p, err)
			}
		}
	}

	return nil
}

// internal: validate

// validateQuery returns an error if a query contains any illegal SQL reserved
// words or if a query contains multiple statements.
func validateQuery(query string) error {

	words, multiple := lexicalParser(query)

	if multiple {
		return ErrMultipleStatementsInQuery
	}

	var found []string
	for _, k := range illegalKeywords {
		if _, exists := words[k]; exists {
			found = append(found, k)
		}
	}

	if len(found) > 0 {
		return fmt.Errorf("%w: %s", ErrIllegalKeywordInQuery, strings.Join(found, ", "))
	}

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
