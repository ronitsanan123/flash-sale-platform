-- KEYS: available, holds, deadlines, outbox, sold, all-units, forfeited, terminal-cleanup (same {item-shard} slot)
-- ARGV: hold_id, proof (EXPIRED_HELD|PSP_DECLINED|PSP_VOIDED), terminal_retention_ms
local raw = redis.call('HGET', KEYS[2], ARGV[1])
if not raw then return {'NOT_FOUND'} end
local h = cjson.decode(raw)
if h.state == 'RELEASED' then return {'OK', raw} end
if h.state == 'CONFIRMED' or h.state == 'FORFEITED' then return {'INVALID_STATE'} end
if h.state == 'HELD' then
  if ARGV[2] ~= 'EXPIRED_HELD' then return {'UNSAFE_RELEASE'} end
  local t = redis.call('TIME')
  local now = t[1] * 1000 + math.floor(t[2] / 1000)
  if now < tonumber(h.deadline) then return {'NOT_EXPIRED'} end
elseif h.state == 'PAYING' then
  if ARGV[2] ~= 'PSP_DECLINED' and ARGV[2] ~= 'PSP_VOIDED' then return {'UNSAFE_RELEASE'} end
else
  return {'INVALID_STATE'}
end
h.state = 'RELEASED'
h.deadline = nil
h.release_proof = ARGV[2]
local encoded = cjson.encode(h)
redis.call('HSET', KEYS[2], ARGV[1], encoded)
redis.call('ZREM', KEYS[3], ARGV[1])
if redis.call('SISMEMBER', KEYS[5], h.unit) == 0 then redis.call('SADD', KEYS[1], h.unit) end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[8], now + tonumber(ARGV[3]), ARGV[1])
redis.call('XADD', KEYS[4], '*', 'type', 'HoldReleased', 'hold_id', ARGV[1], 'unit', h.unit, 'proof', ARGV[2])
return {'OK', encoded}
