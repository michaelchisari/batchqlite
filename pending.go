package batchqlite

import (
	"context"
	"database/sql"
	"sync"
)

type Pending struct {
	result sql.Result
	done   chan struct{}
	err    error
	once   sync.Once
}

func (p *Pending) Wait(ctx context.Context) (sql.Result, error) {
	select {
	case <-p.done:
		return p.result, p.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (p *Pending) complete(r sql.Result, err error) bool {
	dup := true
	p.once.Do(func() {
		dup = false
		p.result = r
		p.err = err
		close(p.done)
	})
	return dup // true if this call was a duplicate
}
