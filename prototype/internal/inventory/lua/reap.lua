-- KEYS: available, holds, deadlines, outbox, sold, all-units, forfeited, terminal-cleanup (same {item-shard} slot)
-- ARGV: batch_limit, terminal_retention_ms
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, ARGV[1])
local released = 0
local overdue = 0
for _, id in ipairs(ids) do
  local raw = redis.call('HGET', KEYS[2], id)
  if raw then
    local h = cjson.decode(raw)
    if h.state == 'HELD' then
      h.state = 'RELEASED'
      h.deadline = nil
      local encoded = cjson.encode(h)
      redis.call('HSET', KEYS[2], id, encoded)
      redis.call('ZREM', KEYS[3], id)
      if redis.call('SISMEMBER', KEYS[5], h.unit) == 0 then redis.call('SADD', KEYS[1], h.unit) end
      local t2 = redis.call('TIME')
      local now2 = t2[1] * 1000 + math.floor(t2[2] / 1000)
      redis.call('ZADD', KEYS[8], now2 + tonumber(ARGV[2]), id)
      redis.call('XADD', KEYS[4], '*', 'type', 'HoldReleased', 'hold_id', id, 'unit', h.unit, 'proof', 'EXPIRED_HELD')
      released = released + 1
    elseif h.state == 'PAYING' then
      -- Keep it indexed just past now so a later reaper pass emits the signal
      -- again until REC or a worker resolves it; never return the unit.
      redis.call('ZADD', KEYS[3], now + 1000, id)
      redis.call('XADD', KEYS[4], '*', 'type', 'HoldPayOverdue', 'hold_id', id, 'unit', h.unit)
      overdue = overdue + 1
    else
      redis.call('ZREM', KEYS[3], id)
    end
  else
    redis.call('ZREM', KEYS[3], id)
  end
end
return {released, overdue}
