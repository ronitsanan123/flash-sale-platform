package orders

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type StatusProjection struct {
	OrderID       string        `json:"order_id"`
	ItemID        string        `json:"item_id"`
	UnitID        string        `json:"unit_id"`
	UserID        string        `json:"user_id"`
	HomeRegion    string        `json:"home_region"`
	OrderStatus   OrderStatus   `json:"order_status"`
	PaymentStatus PaymentStatus `json:"payment_status"`
	Version       int64         `json:"version"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

type StatusStore interface {
	Project(context.Context, Aggregate) error
	Get(context.Context, string) (StatusProjection, bool, error)
}
type MemoryStatusStore struct {
	mu      sync.RWMutex
	records map[string]StatusProjection
}

func NewMemoryStatusStore() *MemoryStatusStore {
	return &MemoryStatusStore{records: map[string]StatusProjection{}}
}
func (s *MemoryStatusStore) Project(ctx context.Context, a Aggregate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := StatusProjection{OrderID: a.Order.ID, ItemID: a.Order.ItemID, UnitID: a.Order.UnitID, UserID: a.Order.UserID, HomeRegion: a.Order.HomeRegion, OrderStatus: a.Order.Status, PaymentStatus: a.Payment.Status, Version: a.Order.Version, UpdatedAt: a.Order.UpdatedAt}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.records[p.OrderID]
	if ok && p.Version < old.Version {
		return fmt.Errorf("stale projection version %d < %d", p.Version, old.Version)
	}
	if ok && p.Version == old.Version && p != old {
		return fmt.Errorf("projection version collision at %d", p.Version)
	}
	s.records[p.OrderID] = p
	return nil
}
func (s *MemoryStatusStore) Get(ctx context.Context, id string) (StatusProjection, bool, error) {
	if err := ctx.Err(); err != nil {
		return StatusProjection{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.records[id]
	return p, ok, nil
}
