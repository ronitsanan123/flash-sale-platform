package inventory

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"flashsale-platform/prototype/internal/rediswire"
)

type StreamRecord struct {
	ID    string
	Event Event
}

func (s *RedisStore) EnsureOutboxGroup(ctx context.Context, item string, shard int, group string) error {
	if group == "" || shard < 0 || shard >= s.Shards {
		return ErrInvalidClaim
	}
	key := s.keys(item, shard)[3]
	_, err := s.do(ctx, key, "XGROUP", "CREATE", key, group, "0", "MKSTREAM")
	if err != nil && strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		return nil
	}
	return err
}

// ClaimOutbox uses XAUTOCLAIM to recover pending entries abandoned by a
// crashed relay. The cursor is held by RedisStreamRelay and must be retained
// across calls for one consumer instance.
func (s *RedisStore) ClaimOutbox(ctx context.Context, item string, shard int, group, consumer, cursor string, minIdle time.Duration, count int) (string, []StreamRecord, error) {
	if group == "" || consumer == "" || shard < 0 || shard >= s.Shards {
		return cursor, nil, ErrInvalidClaim
	}
	if cursor == "" {
		cursor = "0-0"
	}
	if count <= 0 {
		count = 100
	}
	key := s.keys(item, shard)[3]
	v, err := s.do(ctx, key, "XAUTOCLAIM", key, group, consumer, strconv.FormatInt(minIdle.Milliseconds(), 10), cursor, "COUNT", strconv.Itoa(count))
	if err != nil {
		return cursor, nil, err
	}
	arr, ok := v.([]any)
	if !ok || len(arr) < 2 {
		return cursor, nil, fmt.Errorf("unexpected XAUTOCLAIM response %T", v)
	}
	next, _ := arr[0].(string)
	records, err := parseStreamEntries(item, arr[1])
	return next, records, err
}

func (s *RedisStore) ReadOutboxGroup(ctx context.Context, item string, shard int, group, consumer string, count int, block time.Duration) ([]StreamRecord, error) {
	if group == "" || consumer == "" || shard < 0 || shard >= s.Shards {
		return nil, ErrInvalidClaim
	}
	if count <= 0 {
		count = 100
	}
	if block <= 0 {
		block = time.Millisecond
	}
	key := s.keys(item, shard)[3]
	v, err := s.do(ctx, key, "XREADGROUP", "GROUP", group, consumer, "COUNT", strconv.Itoa(count), "BLOCK", strconv.FormatInt(block.Milliseconds(), 10), "STREAMS", key, ">")
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	outer, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected XREADGROUP response %T", v)
	}
	out := make([]StreamRecord, 0)
	for _, stream := range outer {
		sa, ok := stream.([]any)
		if !ok || len(sa) != 2 {
			continue
		}
		records, err := parseStreamEntries(item, sa[1])
		if err != nil {
			return nil, err
		}
		out = append(out, records...)
	}
	return out, nil
}

func (s *RedisStore) AckOutbox(ctx context.Context, item string, shard int, group string, ids ...string) error {
	if group == "" || shard < 0 || shard >= s.Shards {
		return ErrInvalidClaim
	}
	if len(ids) == 0 {
		return nil
	}
	key := s.keys(item, shard)[3]
	args := []string{"XACK", key, group}
	args = append(args, ids...)
	_, err := s.do(ctx, key, args...)
	return err
}

func parseStreamEntries(item string, raw any) ([]StreamRecord, error) {
	entries, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected stream entry list %T", raw)
	}
	out := make([]StreamRecord, 0, len(entries))
	for _, entry := range entries {
		parts, ok := entry.([]any)
		if !ok || len(parts) != 2 {
			continue
		}
		id, ok := parts[0].(string)
		if !ok {
			continue
		}
		fields, ok := parts[1].([]any)
		if !ok {
			continue
		}
		kv := make(map[string]string, len(fields)/2)
		for i := 0; i+1 < len(fields); i += 2 {
			k, _ := fields[i].(string)
			v, _ := fields[i+1].(string)
			kv[k] = v
		}
		ms := int64(0)
		if first, _, found := strings.Cut(id, "-"); found {
			ms, _ = strconv.ParseInt(first, 10, 64)
		}
		out = append(out, StreamRecord{ID: id, Event: Event{ID: id, Type: kv["type"], HoldID: kv["hold_id"], ItemID: item, UnitID: kv["unit"], UserID: kv["user"], CreatedAt: time.UnixMilli(ms).UTC(), Fields: kv}})
	}
	return out, nil
}

var _ = rediswire.Error("")
