package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"flashsale-platform/prototype/internal/inventory"
)

type RedisOutbox interface {
	EnsureOutboxGroup(context.Context, string, int, string) error
	ClaimOutbox(context.Context, string, int, string, string, string, time.Duration, int) (string, []inventory.StreamRecord, error)
	ReadOutboxGroup(context.Context, string, int, string, string, int, time.Duration) ([]inventory.StreamRecord, error)
	AckOutbox(context.Context, string, int, string, ...string) error
}

// RedisStreamRelay is the production-shaped Redis Streams → Kafka relay.
// It publishes first and XACKs only after broker ACK. A crash between those
// steps causes at-least-once redelivery, not event loss.
type RedisStreamRelay struct {
	Outbox       RedisOutbox
	Log          EventLog
	Topic        string
	Group        string
	Consumer     string
	Batch        int
	PendingIdle  time.Duration
	Block        time.Duration
	BeforeAck    func(inventory.Event) error
	mu           sync.Mutex
	claimCursors map[string]string
}

func NewRedisStreamRelay(outbox RedisOutbox, log EventLog, topic, group, consumer string) (*RedisStreamRelay, error) {
	if outbox == nil || log == nil || topic == "" || group == "" || consumer == "" {
		return nil, fmt.Errorf("Redis stream relay dependencies are required")
	}
	return &RedisStreamRelay{Outbox: outbox, Log: log, Topic: topic, Group: group, Consumer: consumer, Batch: 100, PendingIdle: 30 * time.Second, Block: 10 * time.Millisecond, claimCursors: map[string]string{}}, nil
}

func (r *RedisStreamRelay) Once(ctx context.Context, item string, shard int) (int, error) {
	if err := r.Outbox.EnsureOutboxGroup(ctx, item, shard, r.Group); err != nil {
		return 0, err
	}
	key := item + ":" + fmt.Sprint(shard)
	r.mu.Lock()
	cursor := r.claimCursors[key]
	r.mu.Unlock()
	next, records, err := r.Outbox.ClaimOutbox(ctx, item, shard, r.Group, r.Consumer, cursor, r.PendingIdle, r.Batch)
	if err != nil {
		return 0, err
	}
	r.mu.Lock()
	r.claimCursors[key] = next
	r.mu.Unlock()
	if len(records) == 0 {
		records, err = r.Outbox.ReadOutboxGroup(ctx, item, shard, r.Group, r.Consumer, r.Batch, r.Block)
		if err != nil {
			return 0, err
		}
	}
	sent := 0
	for _, record := range records {
		if record.Event.Type == "" || record.ID == "" {
			return sent, fmt.Errorf("invalid Redis outbox entry: %#v", record)
		}
		if _, err := r.Log.Publish(ctx, r.Topic, record.Event, 0); err != nil {
			return sent, err // leave pending for a later XAUTOCLAIM
		}
		if r.BeforeAck != nil {
			if err := r.BeforeAck(record.Event); err != nil {
				return sent, err // simulate crash after Kafka ACK, before Redis XACK
			}
		}
		if err := r.Outbox.AckOutbox(ctx, item, shard, r.Group, record.ID); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}

func (r *RedisStreamRelay) DrainItem(ctx context.Context, item string, shards int, maxRounds int) error {
	if maxRounds <= 0 {
		maxRounds = 100
	}
	for round := 0; round < maxRounds; round++ {
		total := 0
		for shard := 0; shard < shards; shard++ {
			n, err := r.Once(ctx, item, shard)
			if err != nil {
				return err
			}
			total += n
		}
		if total == 0 {
			return nil
		}
	}
	return fmt.Errorf("Redis outbox drain exceeded %d rounds", maxRounds)
}
