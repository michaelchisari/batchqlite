package batchqlite

import (
	"context"
	"database/sql"
)

type Pending struct {
	done chan struct{}
	err  error
}

func (p *Pending) Wait(ctx context.Context) (sql.Result, error)
func (p *Pending) complete(r sql.Result, err error)
