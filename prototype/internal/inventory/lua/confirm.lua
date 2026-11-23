-- KEYS: available, holds, deadlines, outbox, sold, all-units, forfeited, terminal-cleanup (same {item-shard} slot)
-- ARGV: hold_id, terminal_retention_ms
local raw = redis.call('HGET', KEYS[2], ARGV[1])
if not raw then return {'NOT_FOUND'} end
local h = cjson.decode(raw)
if h.state == 'CONFIRMED' then return {'OK', raw} end
if h.state ~= 'PAYING' then return {'INVALID_STATE'} end
h.state = 'CONFIRMED'
h.deadline = nil
local encoded = cjson.encode(h)
redis.call('HSET', KEYS[2], ARGV[1], encoded)
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('SREM', KEYS[1], h.unit)
redis.call('SADD', KEYS[5], h.unit)
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[8], now + tonumber(ARGV[2]), ARGV[1])
redis.call('XADD', KEYS[4], '*', 'type', 'HoldConfirmed', 'hold_id', ARGV[1], 'unit', h.unit, 'user', h.user)
return {'OK', encoded}
