package inventory

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"flashsale-platform/prototype/internal/rediswire"
)

//go:embed lua/reserve.lua
var reserveLua string

//go:embed lua/checkout.lua
var checkoutLua string

//go:embed lua/confirm.lua
var confirmLua string

//go:embed lua/release.lua
var releaseLua string

//go:embed lua/forfeit.lua
var forfeitLua string

//go:embed lua/reap.lua
var reapLua string

//go:embed lua/seed.lua
var seedLua string

//go:embed lua/terminal_cleanup_due.lua
var terminalCleanupDueLua string

//go:embed lua/terminal_cleanup_delete.lua
var terminalCleanupDeleteLua string

type RedisStore struct {
	Client                  *rediswire.Cluster
	Claims                  *RedisClaimRepository
	Router                  *Router
	Shards                  int
	HoldTTL                 time.Duration
	PayingTTL               time.Duration
	TerminalRecordRetention time.Duration
}

func NewRedisStore(client *rediswire.Cluster, shards int, holdTTL, payingTTL, claimRetention, terminalRetention time.Duration) (*RedisStore, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis Cluster client required")
	}
	if shards <= 0 {
		return nil, fmt.Errorf("positive shard count required")
	}
	if claimRetention <= 0 || terminalRetention <= 0 {
		return nil, fmt.Errorf("claim and terminal record retentions must be positive")
	}
	claims := &RedisClaimRepository{Client: client, Retention: claimRetention}
	router, err := NewRouter(shards, claims)
	if err != nil {
		return nil, err
	}
	if holdTTL <= 0 {
		holdTTL = 5 * time.Minute
	}
	if payingTTL <= 0 {
		payingTTL = 2 * time.Minute
	}
	return &RedisStore{Client: client, Claims: claims, Router: router, Shards: shards, HoldTTL: holdTTL, PayingTTL: payingTTL, TerminalRecordRetention: terminalRetention}, nil
}
func shardTag(item string, shard int) string { return base64URL(item) + "-" + strconv.Itoa(shard) }
func base64URL(s string) string              { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
func (s *RedisStore) keys(item string, shard int) []string {
	prefix := "inv:{" + shardTag(item, shard) + "}:"
	return []string{prefix + "available", prefix + "holds", prefix + "deadlines", prefix + "outbox", prefix + "sold", prefix + "all-units", prefix + "forfeited", prefix + "terminal-cleanup"}
}
func (s *RedisStore) do(ctx context.Context, key string, args ...string) (any, error) {
	if s.Client == nil {
		return nil, fmt.Errorf("Redis client unavailable")
	}
	return s.Client.Do(ctx, key, args...)
}
func (s *RedisStore) Seed(ctx context.Context, item string, units []string) error {
	for _, u := range units {
		if u == "" {
			return fmt.Errorf("empty unit id")
		}
		shard, err := ShardForSpecificSeat(u, s.Shards)
		if err != nil {
			return err
		}
		keys := s.keys(item, shard)
		if _, err := s.eval(ctx, keys[0], seedLua, []string{keys[0], keys[5]}, u); err != nil {
			return err
		}
	}
	return nil
}
func (s *RedisStore) Reserve(ctx context.Context, item, holdID, user, unit string) (Hold, error) {
	claim, err := s.Router.Resolve(RouteRequest{HoldID: holdID, ItemID: item, UserID: user, RequestedUnitID: unit}, func(item string, shard int) (bool, error) {
		key := s.keys(item, shard)[0]
		v, e := s.do(ctx, key, "SCARD", key)
		if e != nil {
			return false, e
		}
		n, ok := v.(int64)
		if !ok {
			return false, fmt.Errorf("unexpected SCARD reply %T", v)
		}
		return n > 0, nil
	})
	if err != nil {
		return Hold{}, err
	}
	keys := s.keys(item, claim.Shard)
	v, err := s.eval(ctx, keys[0], reserveLua, keys, holdID, user, strconv.FormatInt(s.HoldTTL.Milliseconds(), 10), unit)
	if err != nil {
		return Hold{}, err
	}
	arr, err := asArray(v)
	if err != nil {
		return Hold{}, err
	}
	status := asString(arr, 0)
	switch status {
	case "OK":
		return decodeHold(asString(arr, 1), item, holdID)
	case "SOLD_OUT":
		return Hold{}, ErrSoldOut
	case "TAKEN":
		return Hold{}, ErrTaken
	case "REPLAY_MISMATCH":
		return Hold{}, ErrReplayMismatch
	case "RELEASED":
		return Hold{}, ErrHoldReleased
	case "FORFEITED":
		return Hold{}, ErrHoldForfeited
	default:
		return Hold{}, fmt.Errorf("reserve script: %s", status)
	}
}
func (s *RedisStore) LockForPayment(ctx context.Context, item, holdID string) (Hold, error) {
	claim, err := s.getClaim(holdID)
	if err != nil {
		return Hold{}, err
	}
	if claim.TerminalState == HoldReleased {
		return Hold{}, ErrHoldReleased
	}
	if claim.TerminalState == HoldForfeited {
		return Hold{}, ErrHoldForfeited
	}
	if claim.TerminalState == HoldConfirmed {
		return Hold{}, ErrHoldConfirmed
	}
	keys := s.keys(item, claim.Shard)
	v, err := s.eval(ctx, keys[1], checkoutLua, []string{keys[1], keys[2], keys[3]}, holdID, strconv.FormatInt(s.PayingTTL.Milliseconds(), 10))
	if err != nil {
		return Hold{}, err
	}
	a, err := asArray(v)
	if err != nil {
		return Hold{}, err
	}
	switch asString(a, 0) {
	case "OK":
		return decodeHold(asString(a, 1), item, holdID)
	case "EXPIRED":
		return Hold{}, ErrExpired
	case "NOT_FOUND":
		return Hold{}, ErrHoldNotFound
	case "RELEASED":
		return Hold{}, ErrHoldReleased
	case "FORFEITED":
		return Hold{}, ErrHoldForfeited
	default:
		return Hold{}, fmt.Errorf("checkout script: %s", asString(a, 0))
	}
}
func (s *RedisStore) Confirm(ctx context.Context, item, holdID string) error {
	claim, err := s.getClaim(holdID)
	if err != nil {
		return err
	}
	if claim.TerminalState == HoldConfirmed {
		return nil
	}
	if claim.TerminalState == HoldReleased {
		return ErrHoldReleased
	}
	if claim.TerminalState == HoldForfeited {
		return ErrHoldForfeited
	}
	keys := s.keys(item, claim.Shard)
	v, err := s.eval(ctx, keys[1], confirmLua, keys, holdID, strconv.FormatInt(s.TerminalRecordRetention.Milliseconds(), 10))
	if err != nil {
		return err
	}
	a, err := asArray(v)
	if err != nil {
		return err
	}
	switch asString(a, 0) {
	case "OK":
		return nil // terminal hold tombstone remains until CleanupTerminals
	case "NOT_FOUND":
		return ErrHoldNotFound
	default:
		return fmt.Errorf("confirm script: %s", asString(a, 0))
	}
}
func (s *RedisStore) Release(ctx context.Context, item, holdID string) error {
	return s.release(ctx, item, holdID, "EXPIRED_HELD")
}
func (s *RedisStore) ReleaseAfterPayment(ctx context.Context, item, holdID string, proof ForfeitProof) error {
	if proof != ForfeitProofPSPDeclined && proof != ForfeitProofPSPVoided {
		return ErrUnsafeForfeit
	}
	return s.release(ctx, item, holdID, string(proof))
}
func (s *RedisStore) release(ctx context.Context, item, holdID, proof string) error {
	claim, err := s.getClaim(holdID)
	if err != nil {
		return err
	}
	if claim.TerminalState == HoldReleased {
		return nil
	}
	if claim.TerminalState == HoldConfirmed || claim.TerminalState == HoldForfeited {
		return ErrInvalidState
	}
	keys := s.keys(item, claim.Shard)
	v, err := s.eval(ctx, keys[1], releaseLua, keys, holdID, proof, strconv.FormatInt(s.TerminalRecordRetention.Milliseconds(), 10))
	if err != nil {
		return err
	}
	a, err := asArray(v)
	if err != nil {
		return err
	}
	switch asString(a, 0) {
	case "OK":
		return nil // terminal hold tombstone remains until CleanupTerminals
	case "NOT_FOUND":
		return ErrHoldNotFound
	case "NOT_EXPIRED":
		return ErrExpired
	case "UNSAFE_RELEASE":
		return ErrUnsafeForfeit
	default:
		return fmt.Errorf("release script: %s", asString(a, 0))
	}
}
func (s *RedisStore) Forfeit(ctx context.Context, item, holdID string, proof ForfeitProof) error {
	claim, err := s.getClaim(holdID)
	if err != nil {
		return err
	}
	if claim.TerminalState == HoldForfeited {
		return nil
	}
	if claim.TerminalState == HoldReleased || claim.TerminalState == HoldConfirmed {
		return ErrInvalidState
	}
	h, found, err := s.Get(ctx, item, holdID)
	if err != nil {
		return err
	}
	if !found {
		return ErrHoldNotFound
	}
	if h.State == HoldForfeited {
		return nil
	}
	if err := ValidateForfeitProof(h.State, proof); err != nil {
		return err
	}
	keys := s.keys(item, claim.Shard)
	v, err := s.eval(ctx, keys[1], forfeitLua, keys, holdID, string(proof), strconv.FormatInt(s.TerminalRecordRetention.Milliseconds(), 10))
	if err != nil {
		return err
	}
	a, err := asArray(v)
	if err != nil {
		return err
	}
	switch asString(a, 0) {
	case "OK":
		return nil // terminal hold tombstone remains until CleanupTerminals
	case "NOT_FOUND":
		return ErrHoldNotFound
	case "UNSAFE_FORFEIT":
		return ErrUnsafeForfeit
	default:
		return fmt.Errorf("forfeit script: %s", asString(a, 0))
	}
}
func (s *RedisStore) Reap(ctx context.Context, item string, shard, limit int) (released, overdue int, err error) {
	if shard < 0 || shard >= s.Shards {
		return 0, 0, ErrInvalidClaim
	}
	if limit <= 0 {
		limit = 100
	}
	keys := s.keys(item, shard)
	v, err := s.eval(ctx, keys[0], reapLua, keys, strconv.Itoa(limit), strconv.FormatInt(s.TerminalRecordRetention.Milliseconds(), 10))
	if err != nil {
		return
	}
	a, err := asArray(v)
	if err != nil {
		return 0, 0, err
	}
	if len(a) >= 2 {
		if n, e := asInt(a[0]); e == nil {
			released = int(n)
		}
		if n, e := asInt(a[1]); e == nil {
			overdue = int(n)
		}
	}
	return
}
func (s *RedisStore) Get(ctx context.Context, item, holdID string) (Hold, bool, error) {
	claim, err := s.getClaim(holdID)
	if err != nil {
		if strings.Contains(err.Error(), "claim not found") {
			return Hold{}, false, nil
		}
		return Hold{}, false, err
	}
	keys := s.keys(item, claim.Shard)
	v, err := s.do(ctx, keys[1], "HGET", keys[1], holdID)
	if err != nil {
		return Hold{}, false, err
	}
	if v == nil {
		return Hold{}, false, nil
	}
	raw, ok := v.(string)
	if !ok {
		return Hold{}, false, fmt.Errorf("unexpected hold reply %T", v)
	}
	h, err := decodeHold(raw, item, holdID)
	return h, err == nil, err
}
func (s *RedisStore) Availability(ctx context.Context, item string) (available, held, paying, confirmed, forfeited, total int, err error) {
	for shard := 0; shard < s.Shards; shard++ {
		keys := s.keys(item, shard)
		readCardinality := func(key string) (int, error) {
			v, e := s.do(ctx, key, "SCARD", key)
			if e != nil {
				return 0, e
			}
			n, ok := v.(int64)
			if !ok {
				return 0, fmt.Errorf("unexpected SCARD reply %T", v)
			}
			return int(n), nil
		}
		if n, e := readCardinality(keys[0]); e != nil {
			err = e
			return
		} else {
			available += n
		}
		if n, e := readCardinality(keys[4]); e != nil {
			err = e
			return
		} else {
			confirmed += n
		}
		if n, e := readCardinality(keys[6]); e != nil {
			err = e
			return
		} else {
			forfeited += n
		}
		if n, e := readCardinality(keys[5]); e != nil {
			err = e
			return
		} else {
			total += n
		}
		v, e := s.do(ctx, keys[1], "HVALS", keys[1])
		if e != nil {
			err = e
			return
		}
		arr, ok := v.([]any)
		if !ok {
			continue
		}
		for _, x := range arr {
			raw, ok := x.(string)
			if !ok {
				continue
			}
			var record struct {
				State HoldState `json:"state"`
			}
			if json.Unmarshal([]byte(raw), &record) != nil {
				continue
			}
			switch record.State {
			case HoldHeld:
				held++
			case HoldPaying:
				paying++
			}
		}
	}
	return
}

func (s *RedisStore) Events(ctx context.Context, item string, after, limit int) ([]Event, error) {
	all := make([]Event, 0)
	for shard := 0; shard < s.Shards; shard++ {
		key := s.keys(item, shard)[3]
		v, err := s.do(ctx, key, "XRANGE", key, "-", "+", "COUNT", "10000")
		if err != nil {
			return nil, err
		}
		entries, ok := v.([]any)
		if !ok {
			continue
		}
		for _, raw := range entries {
			a, _ := raw.([]any)
			if len(a) != 2 {
				continue
			}
			id, _ := a[0].(string)
			fields, _ := a[1].([]any)
			m := map[string]string{}
			for i := 0; i+1 < len(fields); i += 2 {
				k, _ := fields[i].(string)
				val, _ := fields[i+1].(string)
				m[k] = val
			}
			all = append(all, Event{ID: id, Type: m["type"], HoldID: m["hold_id"], ItemID: item, UnitID: m["unit"], UserID: m["user"], CreatedAt: parseStreamTime(id), Fields: m})
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt.Before(all[j].CreatedAt) })
	start := after
	if start < 0 {
		start = 0
	}
	if start > len(all) {
		start = len(all)
	}
	end := len(all)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	return append([]Event(nil), all[start:end]...), nil
}

// CleanupTerminals applies retention safely in two stages: mark the claim
// terminal (so Router rejects all replays), remove the shard-local hold tombstone,
// then start claim TTL. The cleanup ZSET remains until the final EXPIRE succeeds,
// making every crash point retryable.
func (s *RedisStore) CleanupTerminals(ctx context.Context, item string, shard, limit int) (cleaned int, err error) {
	if shard < 0 || shard >= s.Shards {
		return 0, ErrInvalidClaim
	}
	if limit <= 0 {
		limit = 100
	}
	keys := s.keys(item, shard)
	v, err := s.eval(ctx, keys[7], terminalCleanupDueLua, []string{keys[7]}, strconv.Itoa(limit))
	if err != nil {
		return 0, err
	}
	ids, err := asArray(v)
	if err != nil {
		return 0, err
	}
	for _, rawID := range ids {
		id, ok := rawID.(string)
		if !ok {
			continue
		}
		h, found, getErr := s.Get(ctx, item, id)
		if getErr != nil {
			return cleaned, getErr
		}
		state := HoldState("")
		if found {
			state = h.State
		} else {
			claim, claimErr := s.getClaim(id)
			if claimErr != nil {
				return cleaned, claimErr
			}
			state = claim.TerminalState
		}
		if state != HoldReleased && state != HoldForfeited && state != HoldConfirmed {
			return cleaned, fmt.Errorf("terminal cleanup encountered non-terminal hold %s (%s)", id, state)
		}
		if err := s.Claims.MarkTerminal(id, state); err != nil {
			return cleaned, err
		}
		if _, err := s.eval(ctx, keys[1], terminalCleanupDeleteLua, []string{keys[1], keys[7]}, id); err != nil {
			return cleaned, err
		}
		if err := s.expireClaim(id); err != nil {
			return cleaned, err
		}
		if _, err := s.do(ctx, keys[7], "ZREM", keys[7], id); err != nil {
			return cleaned, err
		}
		cleaned++
	}
	return cleaned, nil
}

func (s *RedisStore) getClaim(id string) (ShardClaim, error) {
	claim, ok, err := s.Claims.Get(id)
	if err != nil {
		return ShardClaim{}, err
	}
	if !ok {
		return ShardClaim{}, fmt.Errorf("claim not found")
	}
	return claim, nil
}
func (s *RedisStore) expireClaim(id string) error { return s.Claims.ExpireAfterTerminal(id) }
func (s *RedisStore) eval(ctx context.Context, route string, script string, keys []string, args ...string) (any, error) {
	cmd := []string{"EVAL", script, strconv.Itoa(len(keys))}
	cmd = append(cmd, keys...)
	cmd = append(cmd, args...)
	return s.do(ctx, route, cmd...)
}
func decodeHold(raw, item, id string) (Hold, error) {
	var v struct {
		Unit     string    `json:"unit"`
		User     string    `json:"user"`
		State    HoldState `json:"state"`
		Deadline int64     `json:"deadline"`
	}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return Hold{}, err
	}
	h := Hold{ID: id, Item: item, Unit: v.Unit, User: v.User, State: v.State}
	if v.Deadline > 0 {
		h.Deadline = time.UnixMilli(v.Deadline)
	}
	return h, nil
}
func asArray(v any) ([]any, error) {
	a, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected Redis array, got %T", v)
	}
	return a, nil
}
func asString(a []any, i int) string {
	if i >= len(a) || a[i] == nil {
		return ""
	}
	s, _ := a[i].(string)
	return s
}
func asInt(v any) (int64, error) {
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("expected Redis integer, got %T", v)
	}
	return n, nil
}
func parseStreamTime(id string) time.Time {
	parts := strings.SplitN(id, "-", 2)
	if len(parts) == 0 {
		return time.Time{}
	}
	ms, _ := strconv.ParseInt(parts[0], 10, 64)
	return time.UnixMilli(ms)
}

var _ Store = (*RedisStore)(nil)
