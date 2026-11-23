package orders

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"flashsale-platform/prototype/internal/rediswire"
)

const statusProjectLua = `
local raw = redis.call('GET', KEYS[1])
if raw then
  local old = cjson.decode(raw)
  local incoming = cjson.decode(ARGV[1])
  if tonumber(incoming.version) < tonumber(old.version) then return {'STALE'} end
  if tonumber(incoming.version) == tonumber(old.version) then
    if raw == ARGV[1] then return {'OK'} end
    return {'VERSION_COLLISION'}
  end
end
if tonumber(ARGV[2]) > 0 then redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
else redis.call('SET', KEYS[1], ARGV[1]) end
return {'OK'}
`

// RedisStatusStore is the versioned KV projection. The PostgreSQL order and
// payment rows remain authoritative; this store rejects stale/out-of-order
// updates atomically with Lua.
type RedisStatusStore struct {
	Client    *rediswire.Cluster
	Retention time.Duration
}

func redisStatusKey(id string) string {
	return "kv:order:{" + base64.RawURLEncoding.EncodeToString([]byte(id)) + "}"
}
func (s *RedisStatusStore) Project(ctx context.Context, a Aggregate) error {
	if s.Client == nil {
		return fmt.Errorf("status Redis client is nil")
	}
	p := StatusProjection{OrderID: a.Order.ID, ItemID: a.Order.ItemID, UnitID: a.Order.UnitID, UserID: a.Order.UserID, HomeRegion: a.Order.HomeRegion, OrderStatus: a.Order.Status, PaymentStatus: a.Payment.Status, Version: a.Order.Version, UpdatedAt: a.Order.UpdatedAt}
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	ttl := int64(0)
	if s.Retention > 0 {
		ttl = int64(s.Retention.Seconds())
		if ttl < 1 {
			ttl = 1
		}
	}
	key := redisStatusKey(p.OrderID)
	v, err := s.Client.Do(ctx, key, "EVAL", statusProjectLua, "1", key, string(b), fmt.Sprint(ttl))
	if err != nil {
		return err
	}
	a2, ok := v.([]any)
	if !ok || len(a2) == 0 {
		return fmt.Errorf("unexpected status projection reply %T", v)
	}
	switch a2[0] {
	case "OK":
		return nil
	case "STALE":
		return fmt.Errorf("stale status projection version %d", p.Version)
	case "VERSION_COLLISION":
		return fmt.Errorf("status version collision at %d", p.Version)
	default:
		return fmt.Errorf("status projection: %v", a2[0])
	}
}
func (s *RedisStatusStore) Get(ctx context.Context, id string) (StatusProjection, bool, error) {
	if s.Client == nil {
		return StatusProjection{}, false, fmt.Errorf("status Redis client is nil")
	}
	key := redisStatusKey(id)
	v, err := s.Client.Do(ctx, key, "GET", key)
	if err != nil {
		return StatusProjection{}, false, err
	}
	if v == nil {
		return StatusProjection{}, false, nil
	}
	raw, ok := v.(string)
	if !ok {
		return StatusProjection{}, false, fmt.Errorf("unexpected status reply %T", v)
	}
	var p StatusProjection
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return StatusProjection{}, false, err
	}
	return p, true, nil
}

var _ StatusStore = (*RedisStatusStore)(nil)
