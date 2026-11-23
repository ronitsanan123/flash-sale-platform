-- KEYS: holds, deadlines, outbox (same {item-shard} slot)
-- ARGV: hold_id, paying_ttl_ms
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return {'NOT_FOUND'} end
local h = cjson.decode(raw)
if h.state == 'PAYING' or h.state == 'CONFIRMED' then return {'OK', raw} end
if h.state == 'RELEASED' then return {'RELEASED'} end
if h.state == 'FORFEITED' then return {'FORFEITED'} end
if h.state ~= 'HELD' then return {'INVALID_STATE'} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
if now >= tonumber(h.deadline) then return {'EXPIRED'} end
h.state = 'PAYING'
h.deadline = now + tonumber(ARGV[2])
h.pay_locked_at = now
local encoded = cjson.encode(h)
redis.call('HSET', KEYS[1], ARGV[1], encoded)
redis.call('ZADD', KEYS[2], h.deadline, ARGV[1])
redis.call('XADD', KEYS[3], '*', 'type', 'CheckoutRequested', 'hold_id', ARGV[1], 'unit', h.unit, 'user', h.user)
return {'OK', encoded}
