package batchqlite

import "context"

type Pending struct {
	done chan struct{}
	err  error
}

func (p *Pending) Wait(ctx context.Context) error
