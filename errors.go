package batchqlite

import "errors"

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
	ErrIllegalKeywordInQuery     = errors.New("batchqlite: query contains illegal keyword")
	ErrMultipleStatementsInQuery = errors.New("batchqlite: query cannot contain multiple statements")
	ErrStructuralFailure         = errors.New("batchqlite: structural failure, query not attempted")
	ErrAbandoned                 = errors.New("batchqlite: request abandoned, batch closed with AbandonQueueOnClose")
	ErrClosing                   = errors.New("batchqlite: batch is closing, no longer accepting writes")
	ErrCloseForceFailed          = errors.New("batchqlite: flush goroutine did not stop even after force-closing connections")
)
