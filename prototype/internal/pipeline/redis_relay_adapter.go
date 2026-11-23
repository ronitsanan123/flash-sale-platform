package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"flashsale-platform/prototype/internal/inventory"
)

// RedisRelayAdapter adapts the shard-aware Redis Streams relay to the simple
// item-level RelayRunner interface consumed by the API/demo worker loop.
// Each shard has its own Redis stream, consumer-group pending list and claim
// cursor. RedisStreamRelay still publishes before XACK, so a crash in between
// creates at-least-once delivery rather than event loss.
type RedisRelayAdapter struct {
	Relay  *RedisStreamRelay
	Shards int
}

func (a *RedisRelayAdapter) Once(ctx context.Context, item string) (int, error) {
	if a == nil || a.Relay == nil || a.Shards <= 0 {
		return 0, fmt.Errorf("Redis relay adapter is not configured")
	}
	total := 0
	for shard := 0; shard < a.Shards; shard++ {
		n, err := a.Relay.Once(ctx, item, shard)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// RedisStreamRelayOnceTestFake is deliberately not exported; tests use the
// public RedisOutbox interface to inject a crash after publish and before ACK.
var _ interface {
	Once(context.Context, string, int) (int, error)
} = (*RedisStreamRelay)(nil)

// MemoryOutbox implements inventory outbox access for tests only.
type MemoryOutbox struct {
	mu     sync.Mutex
	events map[string][]inventory.Event
}

func NewMemoryOutbox() *MemoryOutbox { return &MemoryOutbox{events: map[string][]inventory.Event{}} }
func (m *MemoryOutbox) Add(item string, event inventory.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events[item] = append(m.events[item], event)
}
func (m *MemoryOutbox) Events(ctx context.Context, item string, after, limit int) ([]inventory.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	all := m.events[item]
	if after < 0 {
		after = 0
	}
	if after >= len(all) {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	end := after + limit
	if end > len(all) {
		end = len(all)
	}
	return append([]inventory.Event(nil), all[after:end]...), nil
}

// Ensure a stable, non-zero pending timeout for production relays.
func (r *RedisStreamRelay) SetPendingIdle(d time.Duration) {
	if d > 0 {
		r.PendingIdle = d
	}
}
