package admission

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"flashsale-platform/prototype/internal/rediswire"
)

const watermarkPutLua = `
local old = redis.call('GET', KEYS[1])
if old then
  local prior = cjson.decode(old)
  local next = cjson.decode(ARGV[1])
  if tonumber(next.version) < tonumber(prior.version) then return {'STALE'} end
end
redis.call('SET', KEYS[1], ARGV[1])
return {'OK'}
`

// RedisWatermarkStore stores compact per-item watermarks in the dedicated HA
// CTRL Redis service. It must be configured with a different Redis endpoint
// from the hot inventory cluster so inventory shard pressure cannot consume
// the control-plane connection/keyspace budget.
type RedisWatermarkStore struct{ Client *rediswire.Cluster }

func watermarkKey(item string) string {
	return "ctrl:watermark:{" + base64.RawURLEncoding.EncodeToString([]byte(item)) + "}"
}
func (s *RedisWatermarkStore) Put(ctx context.Context, w Watermark) error {
	if s.Client == nil {
		return fmt.Errorf("CTRL Redis client is nil")
	}
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	key := watermarkKey(w.Item)
	v, err := s.Client.Do(ctx, key, "EVAL", watermarkPutLua, "1", key, string(b))
	if err != nil {
		return err
	}
	a, ok := v.([]any)
	if !ok || len(a) == 0 {
		return fmt.Errorf("unexpected watermark reply %T", v)
	}
	if a[0] != "OK" {
		return fmt.Errorf("watermark update rejected: %v", a[0])
	}
	return nil
}
func (s *RedisWatermarkStore) Get(ctx context.Context, item string) (Watermark, bool, error) {
	if s.Client == nil {
		return Watermark{}, false, fmt.Errorf("CTRL Redis client is nil")
	}
	key := watermarkKey(item)
	v, err := s.Client.Do(ctx, key, "GET", key)
	if err != nil {
		return Watermark{}, false, err
	}
	if v == nil {
		return Watermark{}, false, nil
	}
	raw, ok := v.(string)
	if !ok {
		return Watermark{}, false, fmt.Errorf("unexpected watermark value %T", v)
	}
	var w Watermark
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return Watermark{}, false, err
	}
	return w, true, nil
}
