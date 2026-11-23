// Package shed implements priority-aware concurrency admission for the origin.
// It reserves capacity for checkout and reservation so read traffic cannot
// exhaust every handler slot during a flash-sale spike.
package shed

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

var ErrShed = errors.New("request shed due to concurrency budget")

type Class string

const (
	Checkout     Class = "checkout"
	Reserve      Class = "reserve"
	Availability Class = "availability"
	NonEssential Class = "nonessential"
)

type Config struct{ Total, Checkout, Reserve, Availability, NonEssential int }
type Limiter struct {
	total   chan struct{}
	budgets map[Class]chan struct{}
	active  [4]atomic.Int64
}
type Permit struct {
	release  func()
	released atomic.Bool
}

func (p *Permit) Release() {
	if p != nil && p.release != nil && p.released.CompareAndSwap(false, true) {
		p.release()
	}
}
func New(c Config) (*Limiter, error) {
	if c.Total <= 0 {
		return nil, fmt.Errorf("total concurrency must be positive")
	}
	if c.Checkout <= 0 {
		c.Checkout = c.Total / 4
	}
	if c.Reserve <= 0 {
		c.Reserve = c.Total / 2
	}
	if c.Availability <= 0 {
		c.Availability = c.Total / 2
	}
	if c.NonEssential <= 0 {
		c.NonEssential = c.Total / 4
	}
	return &Limiter{total: make(chan struct{}, c.Total), budgets: map[Class]chan struct{}{Checkout: make(chan struct{}, c.Checkout), Reserve: make(chan struct{}, c.Reserve), Availability: make(chan struct{}, c.Availability), NonEssential: make(chan struct{}, c.NonEssential)}}, nil
}
func classIndex(c Class) int {
	switch c {
	case Checkout:
		return 0
	case Reserve:
		return 1
	case Availability:
		return 2
	default:
		return 3
	}
}
func (l *Limiter) Acquire(ctx context.Context, c Class) (*Permit, error) {
	if l == nil {
		return &Permit{}, nil
	}
	budget := l.budgets[c]
	if budget == nil {
		return nil, fmt.Errorf("unknown request class %q", c)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case budget <- struct{}{}:
	default:
		return nil, ErrShed
	}
	select {
	case <-ctx.Done():
		<-budget
		return nil, ctx.Err()
	case l.total <- struct{}{}:
	default:
		<-budget
		return nil, ErrShed
	}
	l.active[classIndex(c)].Add(1)
	return &Permit{release: func() { <-budget; <-l.total; l.active[classIndex(c)].Add(-1) }}, nil
}
func (l *Limiter) Active(c Class) int64 {
	if l == nil {
		return 0
	}
	return l.active[classIndex(c)].Load()
}
