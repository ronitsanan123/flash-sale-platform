-- KEYS: available, holds, deadlines, outbox, sold, all-units, forfeited, terminal-cleanup (same {item-shard} slot)
-- ARGV: hold_id, proof (NOT_ATTEMPTED|PSP_DECLINED|PSP_VOIDED), terminal_retention_ms
-- The proof is a trusted assertion derived from durable, fenced payment state.
local raw = redis.call('HGET', KEYS[2], ARGV[1])
if not raw then return {'NOT_FOUND'} end
local h = cjson.decode(raw)
if h.state == 'FORFEITED' then return {'OK', raw} end
if h.state == 'RELEASED' or h.state == 'CONFIRMED' then return {'INVALID_STATE'} end
if h.state == 'HELD' then
  if ARGV[2] ~= 'NOT_ATTEMPTED' then return {'UNSAFE_FORFEIT'} end
elseif h.state == 'PAYING' then
  if ARGV[2] ~= 'NOT_ATTEMPTED' and ARGV[2] ~= 'PSP_DECLINED' and ARGV[2] ~= 'PSP_VOIDED' then
    return {'UNSAFE_FORFEIT'}
  end
else
  return {'INVALID_STATE'}
end
h.state = 'FORFEITED'
h.deadline = nil
h.forfeit_proof = ARGV[2]
local encoded = cjson.encode(h)
redis.call('HSET', KEYS[2], ARGV[1], encoded)
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('SREM', KEYS[1], h.unit)
-- A unit already durably represented as sold must remain in the sold set.
-- This can happen during a DB unique-guard repair after inventory rebuild.
if redis.call('SISMEMBER', KEYS[5], h.unit) == 0 then
  redis.call('SADD', KEYS[7], h.unit)
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[8], now + tonumber(ARGV[3]), ARGV[1])
redis.call('XADD', KEYS[4], '*', 'type', 'HoldForfeited', 'hold_id', ARGV[1], 'unit', h.unit, 'proof', ARGV[2])
return {'OK', encoded}
