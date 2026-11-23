-- KEYS: available, all-units (same {item-shard} slot)
-- ARGV: unit_id
-- Seed is idempotent and will not put an already-known unit back on sale.
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return {'EXISTS'}
end
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('SADD', KEYS[1], ARGV[1])
return {'SEEDED'}
