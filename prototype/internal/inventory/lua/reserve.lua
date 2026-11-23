-- KEYS: available, holds, deadlines, outbox, sold, all-units, forfeited, terminal-cleanup (same {item-shard} slot)
-- ARGV: hold_id, user_id, ttl_ms, requested_unit_id
-- The immutable per-hold claim was resolved before this same-slot script.
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  local prior = cjson.decode(existing)
  if prior.user ~= ARGV[2] or prior.requested_unit ~= ARGV[4] then
    return {'REPLAY_MISMATCH'}
  end
  if prior.state == 'RELEASED' then return {'RELEASED'} end
  if prior.state == 'FORFEITED' then return {'FORFEITED'} end
  if prior.state == 'HELD' or prior.state == 'PAYING' or prior.state == 'CONFIRMED' then
    return {'OK', existing}
  end
  return {'INVALID_STATE'}
end
local unit
if ARGV[4] ~= '' then
  if redis.call('SREM', KEYS[1], ARGV[4]) == 0 then return {'TAKEN'} end
  unit = ARGV[4]
else
  unit = redis.call('SPOP', KEYS[1])
  if not unit then return {'SOLD_OUT'} end
end
local t = redis.call('TIME')
local deadline = t[1] * 1000 + math.floor(t[2] / 1000) + tonumber(ARGV[3])
local rec = cjson.encode({unit=unit,user=ARGV[2],requested_unit=ARGV[4],state='HELD',deadline=deadline})
redis.call('HSET', KEYS[2], ARGV[1], rec)
redis.call('ZADD', KEYS[3], deadline, ARGV[1])
redis.call('XADD', KEYS[4], '*', 'type', 'HoldCreated', 'hold_id', ARGV[1], 'unit', unit, 'user', ARGV[2])
return {'OK', rec}
