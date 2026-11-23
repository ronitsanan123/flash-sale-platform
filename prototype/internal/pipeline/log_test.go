package pipeline

import (
	"context"
	"errors"
	"flashsale-platform/prototype/internal/inventory"
	"testing"
)

type failingHandler struct {
	calls int
	fail  int
}

func (h *failingHandler) Handle(ctx context.Context, e inventory.Event) error {
	h.calls++
	if h.calls <= h.fail {
		return errors.New("temporary")
	}
	return nil
}
func TestRelayPublishBeforeAdvanceRedeliversAtLeastOnce(t *testing.T) {
	ctx := context.Background()
	store := inventory.NewMemoryStore(0, 0)
	_ = store.Seed(ctx, "i", []string{"u"})
	_, _ = store.Reserve(ctx, "i", "h", "user", "")
	_, _ = store.LockForPayment(ctx, "i", "h")
	log := NewMemoryLog()
	relay := NewRelay(store, log, "checkout")
	relay.BeforeAdvance = func(inventory.Event) error { return errors.New("crash after publish") }
	if _, err := relay.Once(ctx, "i"); err == nil {
		t.Fatal("expected simulated crash")
	}
	relay.BeforeAdvance = nil
	if n, err := relay.Once(ctx, "i"); err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	msgs, err := log.Fetch(ctx, "checkout", "g", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want first event duplicated plus second event", len(msgs))
	}
}
func TestConsumerCommitsAfterHandlerSuccess(t *testing.T) {
	ctx := context.Background()
	log := NewMemoryLog()
	_, _ = log.Publish(ctx, "t", inventory.Event{Type: "CheckoutRequested"}, 0)
	h := &failingHandler{fail: 1}
	c := NewConsumer(log, "t", "g", h.Handle)
	if ok, err := c.Once(ctx); !ok || err == nil {
		t.Fatalf("first attempt ok=%v err=%v", ok, err)
	}
	msgs, _ := log.Fetch(ctx, "t", "g", 1)
	if len(msgs) != 1 {
		t.Fatal("offset committed before successful handling")
	}
	if ok, err := c.Once(ctx); !ok || err != nil {
		t.Fatalf("second attempt ok=%v err=%v", ok, err)
	}
	msgs, _ = log.Fetch(ctx, "t", "g", 1)
	if len(msgs) != 0 {
		t.Fatal("successful message not committed")
	}
}
func TestConsumerDurablyRequeuesBeforeCommit(t *testing.T) {
	ctx := context.Background()
	log := NewMemoryLog()
	_, _ = log.Publish(ctx, "t", inventory.Event{Type: "CheckoutRequested"}, 0)
	h := &failingHandler{fail: 100}
	c := NewConsumer(log, "t", "g", h.Handle)
	c.MaxInlineRetries = 0
	c.MaxRequeues = 1
	if ok, err := c.Once(ctx); !ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	msgs, _ := log.Fetch(ctx, "t", "g", 1)
	if len(msgs) != 0 {
		t.Fatal("original offset not committed after durable requeue")
	}
	retry, _ := log.Fetch(ctx, "t-retry", "retry-group", 10)
	if len(retry) != 1 {
		t.Fatalf("retry queue count=%d", len(retry))
	}
}

type advancingLog struct {
	messages   []Message
	next       int
	committed  map[int]int64
	published  []inventory.Event
	failCommit bool
}

func (l *advancingLog) Publish(_ context.Context, _ string, e inventory.Event, _ int) (Message, error) {
	l.published = append(l.published, e)
	return Message{}, nil
}
func (l *advancingLog) Fetch(_ context.Context, _, _ string, max int) ([]Message, error) {
	if l.next >= len(l.messages) {
		return nil, nil
	}
	if max < 1 {
		max = 1
	}
	end := l.next + max
	if end > len(l.messages) {
		end = len(l.messages)
	}
	out := append([]Message(nil), l.messages[l.next:end]...)
	l.next = end // emulate Kafka REST Proxy's per-session read position
	return out, nil
}
func (l *advancingLog) Commit(_ context.Context, _, _ string, partition int, offset int64) error {
	if l.failCommit {
		l.failCommit = false
		return errors.New("temporary commit failure")
	}
	if l.committed == nil {
		l.committed = map[int]int64{}
	}
	l.committed[partition] = offset + 1
	return nil
}
func (l *advancingLog) Lag(context.Context, string, string) (int, error) { return 0, nil }

func TestConsumerRetriesSameUncommittedKafkaDeliveryBeforeFetchingNext(t *testing.T) {
	ctx := context.Background()
	log := &advancingLog{messages: []Message{
		{Offset: 0, Partition: 0, Event: inventory.Event{Type: "CheckoutRequested", HoldID: "first"}},
		{Offset: 1, Partition: 0, Event: inventory.Event{Type: "CheckoutRequested", HoldID: "second"}},
	}}
	var seen []string
	h := func(_ context.Context, e inventory.Event) error {
		seen = append(seen, e.HoldID)
		if e.HoldID == "first" && len(seen) == 1 {
			return errors.New("temporary handler failure")
		}
		return nil
	}
	c := NewConsumer(log, "checkout", "workers", h)
	if processed, err := c.Once(ctx); !processed || err == nil {
		t.Fatalf("first attempt processed=%v err=%v", processed, err)
	}
	if processed, err := c.Once(ctx); !processed || err != nil {
		t.Fatalf("retry attempt processed=%v err=%v", processed, err)
	}
	if processed, err := c.Once(ctx); !processed || err != nil {
		t.Fatalf("second event processed=%v err=%v", processed, err)
	}
	want := []string{"first", "first", "second"}
	if len(seen) != len(want) {
		t.Fatalf("handler calls=%v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("handler calls=%v, want %v", seen, want)
		}
	}
}

func TestConsumerCommitRetryDoesNotRepeatSuccessfulHandlerOrRequeue(t *testing.T) {
	ctx := context.Background()
	log := &advancingLog{messages: []Message{{Offset: 0, Partition: 2, Event: inventory.Event{Type: "CheckoutRequested", HoldID: "once"}}}, failCommit: true}
	calls := 0
	c := NewConsumer(log, "checkout", "workers", func(context.Context, inventory.Event) error { calls++; return nil })
	if processed, err := c.Once(ctx); processed || err == nil {
		t.Fatalf("first commit processed=%v err=%v", processed, err)
	}
	if processed, err := c.Once(ctx); !processed || err != nil {
		t.Fatalf("commit retry processed=%v err=%v", processed, err)
	}
	if calls != 1 {
		t.Fatalf("successful handler ran %d times; want once", calls)
	}
}
