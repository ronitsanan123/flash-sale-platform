-- KEYS: holds, terminal-cleanup; ARGV: hold_id
redis.call('HDEL',KEYS[1],ARGV[1])
return {'OK'}
