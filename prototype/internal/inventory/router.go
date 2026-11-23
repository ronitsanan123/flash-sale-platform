package inventory

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"sync"
)

var (
	// ErrNoShardHasStock means the stock probes found no candidate shard. It is
	// safe to return sold-out for this attempt, but never to remap an existing
	// hold claim to another shard.
	ErrNoShardHasStock = ErrSoldOut
)

// ShardClaim is the immutable routing decision for one reservation attempt.
// A claim is keyed by HoldID, not by item, so hot items do not funnel all
// claims through one Redis Cluster hash slot.
type ShardClaim struct {
	HoldID          string    `json:"hold_id"`
	ItemID          string    `json:"item_id"`
	UserID          string    `json:"user_id"`
	RequestedUnitID string    `json:"requested_unit_id"` // empty means any unit
	Shard           int       `json:"shard"`
	TerminalState   HoldState `json:"terminal_state,omitempty"`
}

// RouteRequest contains the stable inputs used to choose and bind a shard.
type RouteRequest struct {
	HoldID          string
	ItemID          string
	UserID          string
	RequestedUnitID string // empty means any unit
}

// StockProbe is a read-only hint used only when initially selecting an
// any-unit shard. It is not an allocation guarantee: reserve must still be
// atomic on the selected shard.
type StockProbe func(itemID string, shard int) (hasCandidate bool, err error)

// ClaimRepository must implement Get and PutIfAbsent atomically per HoldID.
// A Redis implementation uses a per-hold key and HSETNX (or SET NX) for the
// immutable claim. It must not use one per-item claim key.
type ClaimRepository interface {
	Get(holdID string) (ShardClaim, bool, error)
	PutIfAbsent(claim ShardClaim) (stored ShardClaim, inserted bool, err error)
}

// Router selects a candidate shard and pins it to a hold ID before the
// per-shard reserve operation. The claim and reserve are intentionally
// separate because their Redis Cluster keys occupy different hash slots.
type Router struct {
	shardCount int
	claims     ClaimRepository
}

func NewRouter(shardCount int, claims ClaimRepository) (*Router, error) {
	if shardCount <= 0 {
		return nil, fmt.Errorf("shard count must be positive")
	}
	if claims == nil {
		return nil, fmt.Errorf("claim repository is required")
	}
	return &Router{shardCount: shardCount, claims: claims}, nil
}

// ShardForSpecificSeat deterministically places a requested unit. The same
// unit always maps to the same shard for the lifetime of the item topology.
func ShardForSpecificSeat(unitID string, shardCount int) (int, error) {
	if unitID == "" || shardCount <= 0 {
		return 0, fmt.Errorf("unit id and positive shard count are required")
	}
	return hashToShard(unitID, shardCount), nil
}

// StartingShardForUser returns the first shard to probe for an any-unit
// reservation. The caller walks subsequent shards in a stable circular order.
func StartingShardForUser(userID string, shardCount int) (int, error) {
	if userID == "" || shardCount <= 0 {
		return 0, fmt.Errorf("user id and positive shard count are required")
	}
	return hashToShard(userID, shardCount), nil
}

func hashToShard(value string, shardCount int) int {
	sum := sha256.Sum256([]byte(value))
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(shardCount))
}

// Resolve returns the immutable shard claim for this hold. For a new
// any-unit hold it walks from hash(user_id) % S and binds the first shard whose
// stock probe reports a candidate. For a specific seat it uses hash(unit_id)
// % S. Concurrent duplicate requests converge on the winner of PutIfAbsent.
//
// Once a claim exists, this method does not probe or move it. If the selected
// shard drains between the stock probe and the atomic reserve script, reserve
// returns SOLD_OUT for this hold ID. The caller must not try another shard:
// it would make ambiguous retries capable of allocating twice.
func (r *Router) Resolve(req RouteRequest, hasStock StockProbe) (ShardClaim, error) {
	if req.HoldID == "" || req.ItemID == "" || req.UserID == "" {
		return ShardClaim{}, fmt.Errorf("hold id, item id and user id are required")
	}

	if existing, ok, err := r.claims.Get(req.HoldID); err != nil {
		return ShardClaim{}, err
	} else if ok {
		return r.validateStoredClaim(req, existing)
	}

	candidate, err := r.chooseShard(req, hasStock)
	if err != nil {
		// Another concurrent duplicate may have installed the claim while this
		// request was probing. Resolve to that exact claim before returning.
		if existing, ok, getErr := r.claims.Get(req.HoldID); getErr != nil {
			return ShardClaim{}, getErr
		} else if ok {
			return r.validateStoredClaim(req, existing)
		}
		return ShardClaim{}, err
	}

	proposed := ShardClaim{
		HoldID:          req.HoldID,
		ItemID:          req.ItemID,
		UserID:          req.UserID,
		RequestedUnitID: req.RequestedUnitID,
		Shard:           candidate,
	}
	stored, _, err := r.claims.PutIfAbsent(proposed)
	if err != nil {
		return ShardClaim{}, err
	}
	return r.validateStoredClaim(req, stored)
}

func (r *Router) chooseShard(req RouteRequest, hasStock StockProbe) (int, error) {
	if req.RequestedUnitID != "" {
		return ShardForSpecificSeat(req.RequestedUnitID, r.shardCount)
	}
	if hasStock == nil {
		return 0, fmt.Errorf("stock probe is required for any-unit routing")
	}
	start, err := StartingShardForUser(req.UserID, r.shardCount)
	if err != nil {
		return 0, err
	}
	for offset := 0; offset < r.shardCount; offset++ {
		shard := (start + offset) % r.shardCount
		hasCandidate, probeErr := hasStock(req.ItemID, shard)
		if probeErr != nil {
			// Unknown stock is not equivalent to an empty shard. Fail closed.
			return 0, fmt.Errorf("probe shard %d: %w", shard, probeErr)
		}
		if hasCandidate {
			return shard, nil
		}
	}
	return 0, ErrNoShardHasStock
}

func (r *Router) validateStoredClaim(req RouteRequest, claim ShardClaim) (ShardClaim, error) {
	switch claim.TerminalState {
	case HoldReleased:
		return ShardClaim{}, ErrHoldReleased
	case HoldForfeited:
		return ShardClaim{}, ErrHoldForfeited
	case HoldConfirmed:
		return ShardClaim{}, ErrHoldConfirmed
	}
	if claim.HoldID != req.HoldID || claim.ItemID != req.ItemID || claim.UserID != req.UserID || claim.RequestedUnitID != req.RequestedUnitID {
		return ShardClaim{}, ErrReplayMismatch
	}
	if claim.Shard < 0 || claim.Shard >= r.shardCount {
		return ShardClaim{}, ErrInvalidClaim
	}
	return claim, nil
}

// RedisClaimKey returns a per-hold Redis key. Base64url keeps user-provided
// hold IDs from injecting Redis Cluster hash-tag braces. The resulting
// `{...}` tag is unique per hold, spreading claims across cluster slots.
func RedisClaimKey(holdID string) (string, error) {
	if holdID == "" {
		return "", fmt.Errorf("hold id is required")
	}
	tag := base64.RawURLEncoding.EncodeToString([]byte(holdID))
	return "hold-claim:{" + tag + "}", nil
}

// MemoryClaimRepository is a deterministic stand-in for the Redis HSETNX
// claim repository. It is useful for local tests; it is not distributed or
// durable and must be replaced by a Redis implementation in deployment.
type MemoryClaimRepository struct {
	mu     sync.Mutex
	claims map[string]ShardClaim
}

func NewMemoryClaimRepository() *MemoryClaimRepository {
	return &MemoryClaimRepository{claims: make(map[string]ShardClaim)}
}

func (m *MemoryClaimRepository) Get(holdID string) (ShardClaim, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	claim, ok := m.claims[holdID]
	return claim, ok, nil
}

func (m *MemoryClaimRepository) PutIfAbsent(claim ShardClaim) (ShardClaim, bool, error) {
	if claim.HoldID == "" || claim.ItemID == "" || claim.UserID == "" || claim.Shard < 0 {
		return ShardClaim{}, false, ErrInvalidClaim
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claims == nil {
		m.claims = make(map[string]ShardClaim)
	}
	if existing, ok := m.claims[claim.HoldID]; ok {
		return existing, false, nil
	}
	m.claims[claim.HoldID] = claim
	return claim, true, nil
}
