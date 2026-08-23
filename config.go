package batchqlite

import (
	"log/slog"
	"time"
)

type Option func(*batchqliteConfig) error

func WithMaxQueueDepth(v int) Option {
	return func(cfg *batchqliteConfig) error {
		if v <= 0 {
			return ErrInvalidQueueDepth
		}
		cfg.maxQueueDepth = v
		return nil
	}
}

func WithMaxBatchSizeToProcess(v int) Option {
	return func(cfg *batchqliteConfig) error {
		if v <= 0 {
			return ErrInvalidBatchSize
		}
		cfg.maxBatchSizeToProcess = v
		return nil
	}
}

func WithMaxBatchTimeToProcess(v time.Duration) Option {
	return func(cfg *batchqliteConfig) error {
		if v <= 0 {
			return ErrInvalidBatchTime
		}
		cfg.maxBatchTimeToProcess = v
		return nil
	}
}

func WithMaxReadPoolSize(v int) Option {
	return func(cfg *batchqliteConfig) error {
		if v <= 0 {
			return ErrInvalidReadPoolSize
		}
		cfg.maxReadPoolSize = v
		return nil
	}
}

func WithOnFullPolicy(v onFullPolicy) Option {
	return func(cfg *batchqliteConfig) error {
		if v != Block && v != Reject {
			return ErrInvalidOnFullPolicy
		}
		cfg.onFullPolicy = v
		return nil
	}
}

func WithSqliteBusyTimeout(v time.Duration) Option {
	return func(cfg *batchqliteConfig) error {
		if v <= 0 {
			return ErrInvalidBusyTimeout
		}
		cfg.sqliteBusyTimeout = v
		return nil
	}
}

func WithSqliteCheckpointInterval(v time.Duration) Option {
	return func(cfg *batchqliteConfig) error {
		if v <= 0 {
			return ErrInvalidCheckpointInterval
		}
		cfg.sqliteCheckpointInterval = v
		return nil
	}
}

func WithAbandonQueueOnClose(v bool) Option {
	return func(cfg *batchqliteConfig) error {
		cfg.abandonQueueOnClose = v
		return nil
	}
}

func WithCloseTimeout(v time.Duration) Option {
	return func(cfg *batchqliteConfig) error {
		if v <= 0 {
			return ErrInvalidCloseTimeout
		}
		cfg.closeTimeout = v
		return nil
	}
}

func WithLogger(l *slog.Logger) Option {
	return func(cfg *batchqliteConfig) error {
		cfg.logger = l
		return nil
	}
}
