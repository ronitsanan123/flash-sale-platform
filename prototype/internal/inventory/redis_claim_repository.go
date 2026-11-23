package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"flashsale-platform/prototype/internal/rediswire"
)

type RedisClaimRepository struct {
	Client    *rediswire.Cluster
	Retention time.Duration
	Timeout   time.Duration
}

func (r *RedisClaimRepository) ctx() (context.Context, context.CancelFunc) {
	d := r.Timeout
	if d <= 0 {
		d = 2 * time.Second
	}
	return context.WithTimeout(context.Background(), d)
}
func (r *RedisClaimRepository) Get(holdID string) (ShardClaim, bool, error) {
	if r.Client == nil {
		return ShardClaim{}, false, fmt.Errorf("Redis claim client is nil")
	}
	key, err := RedisClaimKey(holdID)
	if err != nil {
		return ShardClaim{}, false, err
	}
	ctx, cancel := r.ctx()
	defer cancel()
	v, err := r.Client.Do(ctx, key, "HGET", key, "claim")
	if err != nil {
		return ShardClaim{}, false, err
	}
	if v == nil {
		return ShardClaim{}, false, nil
	}
	raw, ok := v.(string)
	if !ok {
		return ShardClaim{}, false, fmt.Errorf("unexpected HGET reply %T", v)
	}
	var claim ShardClaim
	if err := json.Unmarshal([]byte(raw), &claim); err != nil {
		return ShardClaim{}, false, fmt.Errorf("decode claim: %w", err)
	}
	return claim, true, nil
}
func (r *RedisClaimRepository) PutIfAbsent(claim ShardClaim) (ShardClaim, bool, error) {
	if r.Client == nil {
		return ShardClaim{}, false, fmt.Errorf("Redis claim client is nil")
	}
	key, err := RedisClaimKey(claim.HoldID)
	if err != nil {
		return ShardClaim{}, false, err
	}
	raw, err := json.Marshal(claim)
	if err != nil {
		return ShardClaim{}, false, err
	}
	ctx, cancel := r.ctx()
	defer cancel()
	v, err := r.Client.Do(ctx, key, "HSETNX", key, "claim", string(raw))
	if err != nil {
		return ShardClaim{}, false, err
	}
	inserted, ok := v.(int64)
	if !ok {
		return ShardClaim{}, false, fmt.Errorf("unexpected HSETNX reply %T", v)
	}
	if inserted == 1 {
		return claim, true, nil
	}
	existing, found, err := r.Get(claim.HoldID)
	if err != nil {
		return ShardClaim{}, false, err
	}
	if !found {
		return ShardClaim{}, false, fmt.Errorf("claim disappeared after HSETNX lost race")
	}
	return existing, false, nil
}

// ExpireAfterTerminal is deliberately not called on claim creation. Active
// HELD/PAYING claims never expire; only a durable terminal shard transition
// permits a retention TTL longer than every replay/credential/recovery window.
func (r *RedisClaimRepository) ExpireAfterTerminal(holdID string) error {
	if r.Retention <= 0 {
		return fmt.Errorf("terminal claim retention must be configured")
	}
	key, err := RedisClaimKey(holdID)
	if err != nil {
		return err
	}
	seconds := int64(math.Ceil(r.Retention.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	ctx, cancel := r.ctx()
	defer cancel()
	v, err := r.Client.Do(ctx, key, "EXPIRE", key, fmt.Sprint(seconds))
	if err != nil {
		return err
	}
	n, ok := v.(int64)
	if !ok || n != 1 {
		return fmt.Errorf("terminal claim EXPIRE did not affect one key")
	}
	return nil
}

// MarkTerminal stores the terminal disposition in the per-hold claim key. It
// intentionally does not apply TTL yet: RedisStore removes the shard-local
// hold tombstone first, then applies claim retention. This ordering prevents a
// claim from expiring while an old shard hash record still exists.
func (r *RedisClaimRepository) MarkTerminal(holdID string, state HoldState) error {
	if state != HoldReleased && state != HoldForfeited && state != HoldConfirmed {
		return fmt.Errorf("not a terminal hold state: %s", state)
	}
	claim, ok, err := r.Get(holdID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("claim not found")
	}
	claim.TerminalState = state
	raw, err := json.Marshal(claim)
	if err != nil {
		return err
	}
	key, err := RedisClaimKey(holdID)
	if err != nil {
		return err
	}
	ctx, cancel := r.ctx()
	defer cancel()
	_, err = r.Client.Do(ctx, key, "HSET", key, "claim", string(raw))
	return err
}
