package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"flashsale-platform/prototype/internal/inventory"
)

type fakeRedisOutbox struct {
	entries []inventory.StreamRecord
	acked   map[string]bool
	pending map[string]bool
}

func (f *fakeRedisOutbox) EnsureOutboxGroup(context.Context, string, int, string) error { return nil }
func (f *fakeRedisOutbox) ClaimOutbox(_ context.Context, _ string, _ int, _ string, _ string, cursor string, _ time.Duration, _ int) (string, []inventory.StreamRecord, error) {
	out := make([]inventory.StreamRecord, 0)
	for _, e := range f.entries {
		if f.pending[e.ID] && !f.acked[e.ID] {
			out = append(out, e)
		}
	}
	return "0-0", out, nil
}
func (f *fakeRedisOutbox) ReadOutboxGroup(_ context.Context, _ string, _ int, _, _ string, _ int, _ time.Duration) ([]inventory.StreamRecord, error) {
	out := make([]inventory.StreamRecord, 0)
	for _, e := range f.entries {
		if !f.acked[e.ID] && !f.pending[e.ID] {
			f.pending[e.ID] = true
			out = append(out, e)
		}
	}
	return out, nil
}
func (f *fakeRedisOutbox) AckOutbox(_ context.Context, _ string, _ int, _ string, ids ...string) error {
	for _, id := range ids {
		f.acked[id] = true
		delete(f.pending, id)
	}
	return nil
}

// A new relay instance can reclaim an unacked entry after the prior process
// has published it. The duplicate broker effect is expected under at-least-once
// delivery and downstream consumers deduplicate by event/hold identity.
func TestRedisStreamRelayCrashBetweenPublishAndAckRedelivers(t *testing.T) {
	ctx := context.Background()
	entry := inventory.StreamRecord{ID: "1000-0", Event: inventory.Event{ID: "evt-1", Type: "CheckoutRequested", ItemID: "item", HoldID: "order-1"}}
	outbox := &fakeRedisOutbox{entries: []inventory.StreamRecord{entry}, acked: map[string]bool{}, pending: map[string]bool{}}
	log := NewMemoryLog()
	first, err := NewRedisStreamRelay(outbox, log, "checkout-events", "relay", "relay-a")
	if err != nil {
		t.Fatal(err)
	}
	first.BeforeAck = func(inventory.Event) error { return errors.New("simulated process crash") }
	if _, err := first.Once(ctx, "item", 0); err == nil {
		t.Fatal("expected simulated crash")
	}
	if outbox.acked[entry.ID] {
		t.Fatal("entry ACKed before crash")
	}

	// The new consumer reclaims the pending entry through the fake XAUTOCLAIM.
	second, err := NewRedisStreamRelay(outbox, log, "checkout-events", "relay", "relay-b")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := second.Once(ctx, "item", 0); err != nil || n != 1 {
		t.Fatalf("second relay: n=%d err=%v", n, err)
	}
	if !outbox.acked[entry.ID] {
		t.Fatal("entry was not ACKed after successful publish")
	}
	messages, err := log.Fetch(ctx, "checkout-events", "consumer", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("published messages=%d, want at-least-once duplicate 2", len(messages))
	}
}
