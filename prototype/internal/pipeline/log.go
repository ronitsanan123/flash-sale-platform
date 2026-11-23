package pipeline

import (
	"context"
	"fmt"
	"sync"

	"flashsale-platform/prototype/internal/inventory"
)

type Message struct {
	Offset    int64           `json:"offset"`
	Partition int             `json:"partition"`
	Attempt   int             `json:"attempt"`
	Event     inventory.Event `json:"event"`
}
type EventLog interface {
	Publish(context.Context, string, inventory.Event, int) (Message, error)
	Fetch(context.Context, string, string, int) ([]Message, error)
	Commit(context.Context, string, string, int, int64) error
	Lag(context.Context, string, string) (int, error)
}

type MemoryLog struct {
	mu      sync.Mutex
	topics  map[string][]Message
	commits map[string]int64
}

func NewMemoryLog() *MemoryLog {
	return &MemoryLog{topics: map[string][]Message{}, commits: map[string]int64{}}
}
func groupKey(topic, group string) string { return topic + "\x00" + group }
func (l *MemoryLog) Publish(ctx context.Context, topic string, e inventory.Event, attempt int) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	if topic == "" {
		return Message{}, fmt.Errorf("topic required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	list := l.topics[topic]
	m := Message{Offset: int64(len(list)), Attempt: attempt, Event: e}
	l.topics[topic] = append(list, m)
	return m, nil
}
func (l *MemoryLog) Fetch(ctx context.Context, topic, group string, max int) ([]Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if max <= 0 {
		max = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	list := l.topics[topic]
	commit, committed := l.commits[groupKey(topic, group)]
	start := 0
	if committed {
		start = int(commit + 1)
	}
	if start < 0 {
		start = 0
	}
	if start >= len(list) {
		return nil, nil
	}
	end := start + max
	if end > len(list) {
		end = len(list)
	}
	return append([]Message(nil), list[start:end]...), nil
}
func (l *MemoryLog) Commit(ctx context.Context, topic, group string, partition int, offset int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := groupKey(topic, group)
	if offset < l.commits[key] {
		return fmt.Errorf("offset regression")
	}
	if int(offset) >= len(l.topics[topic]) {
		return fmt.Errorf("offset %d has not been published", offset)
	}
	l.commits[key] = offset
	return nil
}
func (l *MemoryLog) Lag(ctx context.Context, topic, group string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	commit, committed := l.commits[groupKey(topic, group)]
	if !committed {
		return len(l.topics[topic]), nil
	}
	return len(l.topics[topic]) - int(commit) - 1, nil
}

// Relay reads the Redis Streams/outbox abstraction and publishes each event
// before advancing its local acknowledged position. A crash after publish but
// before advancing causes harmless at-least-once redelivery.
type Outbox interface {
	Events(context.Context, string, int, int) ([]inventory.Event, error)
}
type Relay struct {
	Outbox        Outbox
	Log           EventLog
	Topic         string
	Batch         int
	positions     map[string]int
	mu            sync.Mutex
	BeforeAdvance func(inventory.Event) error
}

func NewRelay(outbox Outbox, log EventLog, topic string) *Relay {
	return &Relay{Outbox: outbox, Log: log, Topic: topic, Batch: 100, positions: map[string]int{}}
}
func (r *Relay) Once(ctx context.Context, item string) (int, error) {
	if r.Outbox == nil || r.Log == nil {
		return 0, fmt.Errorf("relay dependencies required")
	}
	r.mu.Lock()
	after := r.positions[item]
	r.mu.Unlock()
	limit := r.Batch
	if limit <= 0 {
		limit = 100
	}
	events, err := r.Outbox.Events(ctx, item, after, limit)
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, e := range events {
		if _, err := r.Log.Publish(ctx, r.Topic, e, 0); err != nil {
			return sent, err
		}
		if r.BeforeAdvance != nil {
			if err := r.BeforeAdvance(e); err != nil {
				return sent, err
			}
		}
		r.mu.Lock()
		r.positions[item]++
		r.mu.Unlock()
		sent++
	}
	return sent, nil
}

// Consumer commits only after durable handling, or after a durable requeue/DLQ
// publication. In-memory storage is a deterministic test/demo adapter.
type Handler func(context.Context, inventory.Event) error
type pendingDelivery struct {
	topic    string
	group    string
	message  Message
	requeued bool
}

type Consumer struct {
	Log              EventLog
	Topic            string
	Group            string
	RetryTopic       string
	DLQTopic         string
	MaxInlineRetries int
	MaxRequeues      int
	Handler          Handler
	attempts         map[string]int
	mu               sync.Mutex
	onceMu           sync.Mutex
	pending          *pendingDelivery
}

func NewConsumer(log EventLog, topic, group string, h Handler) *Consumer {
	return &Consumer{Log: log, Topic: topic, Group: group, RetryTopic: topic + "-retry", DLQTopic: topic + "-dlq", MaxInlineRetries: 2, MaxRequeues: 2, Handler: h, attempts: map[string]int{}}
}
func (c *Consumer) Once(ctx context.Context) (bool, error) {
	if c.Log == nil || c.Handler == nil {
		return false, fmt.Errorf("consumer dependencies required")
	}
	// Serialize delivery/commit for this consumer instance. Kafka REST Proxy
	// advances the session's read position when records are fetched, even when
	// group offsets are not committed. Keep the current uncommitted message in
	// memory and retry it before fetching another one; a process crash still
	// replays it from the broker's committed offset.
	c.onceMu.Lock()
	defer c.onceMu.Unlock()

	if c.pending == nil {
		topic, group := c.Topic, c.Group
		msgs, err := c.Log.Fetch(ctx, topic, group, 1)
		if err != nil {
			return false, err
		}
		if len(msgs) == 0 {
			topic, group = c.RetryTopic, c.Group+"-retry"
			msgs, err = c.Log.Fetch(ctx, topic, group, 1)
			if err != nil {
				return false, err
			}
		}
		if len(msgs) == 0 {
			return false, nil
		}
		c.pending = &pendingDelivery{topic: topic, group: group, message: msgs[0]}
	}

	delivery := c.pending
	m := delivery.message
	key := fmt.Sprintf("%s:%s:%d:%d", delivery.topic, delivery.group, m.Partition, m.Offset)
	if !delivery.requeued {
		if err := c.Handler(ctx, m.Event); err != nil {
			c.mu.Lock()
			c.attempts[key]++
			n := c.attempts[key]
			c.mu.Unlock()
			if n <= c.MaxInlineRetries {
				return true, err
			}
			destination := c.RetryTopic
			attempt := m.Attempt + 1
			if attempt > c.MaxRequeues {
				destination = c.DLQTopic
			}
			if _, pubErr := c.Log.Publish(ctx, destination, m.Event, attempt); pubErr != nil {
				return false, pubErr
			}
			// Remember the durable requeue in-process so a transient commit
			// error does not publish the same retry record repeatedly. A process
			// crash between publish and commit can still duplicate; consumers
			// therefore remain idempotent by order_id.
			delivery.requeued = true
		} else {
			delivery.requeued = true
		}
	}
	if err := c.Log.Commit(ctx, delivery.topic, delivery.group, m.Partition, m.Offset); err != nil {
		return false, err
	}
	c.mu.Lock()
	delete(c.attempts, key)
	c.mu.Unlock()
	c.pending = nil
	return true, nil
}
