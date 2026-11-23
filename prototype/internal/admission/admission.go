package admission

import (
	"context"
	"fmt"
	"sync"
	"time"

	"flashsale-platform/prototype/internal/token"
)

type InventoryState string
type GateState string

const (
	InventoryOpen      InventoryState = "OPEN"
	InventoryTemporary InventoryState = "TEMPORARILY_UNAVAILABLE"
	InventorySoldOut   InventoryState = "SOLD_OUT"
	GateActive         GateState      = "ACTIVE"
	GatePaused         GateState      = "PAUSED"
)

type Stock struct {
	Available, Held, Paying, Confirmed, Forfeited, Total int
	Known                                                bool
}
type Health struct {
	PSPHealthy           bool
	PSPCapacityPerSecond int
	StandbyACKHealthy    bool
	StandbyACKAt         time.Time
	ControlPlaneHealthy  bool
	WorkersMonitored     bool
	WorkersHealthy       bool
	WorkersSignalAt      time.Time
	Now                  time.Time
}
type Watermark struct {
	Item      string         `json:"item"`
	Version   uint64         `json:"version"`
	Head      int64          `json:"head"`
	Rate      int64          `json:"rate"`
	Inventory InventoryState `json:"inventory_state"`
	Gate      GateState      `json:"gate"`
	Reason    string         `json:"reason,omitempty"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type Controller struct {
	mu            sync.Mutex
	maxStockAge   time.Duration
	maxStandbyAge time.Duration
	version       map[string]uint64
}

func NewController(maxStockAge, maxStandbyAge time.Duration) *Controller {
	if maxStockAge <= 0 {
		maxStockAge = 2 * time.Second
	}
	if maxStandbyAge <= 0 {
		maxStandbyAge = 3 * time.Second
	}
	return &Controller{maxStockAge: maxStockAge, maxStandbyAge: maxStandbyAge, version: map[string]uint64{}}
}

func (c *Controller) Evaluate(item string, head, configuredRate int64, stock Stock, stockAt time.Time, health Health) Watermark {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := health.Now
	if now.IsZero() {
		now = time.Now()
	}
	if configuredRate < 0 {
		configuredRate = 0
	}
	w := Watermark{Item: item, Head: head, Rate: configuredRate, Inventory: InventoryTemporary, Gate: GateActive, UpdatedAt: now}
	switch {
	case !stock.Known || stockAt.IsZero() || now.Sub(stockAt) > c.maxStockAge:
		w.Gate = GatePaused
		w.Reason = "unknown or stale stock"
	case stock.Available > 0:
		w.Inventory = InventoryOpen
	case stock.Held > 0 || stock.Paying > 0:
		w.Inventory = InventoryTemporary
	case stock.Total > 0 && stock.Confirmed+stock.Forfeited >= stock.Total:
		w.Inventory = InventorySoldOut
	default:
		w.Inventory = InventoryTemporary
	}
	if !health.ControlPlaneHealthy {
		w.Gate = GatePaused
		w.Reason = "control plane unavailable"
	}
	if !health.PSPHealthy || health.PSPCapacityPerSecond <= 0 {
		w.Gate = GatePaused
		w.Reason = "PSP breaker open or capacity unavailable"
	}
	if !health.StandbyACKHealthy || health.StandbyACKAt.IsZero() || now.Sub(health.StandbyACKAt) > c.maxStandbyAge {
		w.Gate = GatePaused
		w.Reason = "remote standby ACK stale or unavailable"
	}
	if health.WorkersMonitored && (!health.WorkersHealthy || health.WorkersSignalAt.IsZero() || now.Sub(health.WorkersSignalAt) > c.maxStandbyAge) {
		w.Gate = GatePaused
		w.Reason = "order worker heartbeat stale or unavailable"
	}
	if w.Inventory == InventorySoldOut {
		w.Gate = GatePaused
		if w.Reason == "" {
			w.Reason = "terminal sold out"
		}
	}
	c.version[item]++
	w.Version = c.version[item]
	return w
}

type Entry struct {
	Item        string    `json:"item"`
	User        string    `json:"user"`
	Position    int64     `json:"position"`
	AdmissionID string    `json:"admission_id"`
	JoinedAt    time.Time `json:"joined_at"`
}
type Queue struct {
	mu         sync.Mutex
	items      map[string][]Entry
	byUser     map[string]map[string]Entry
	watermarks map[string]Watermark
	signer     *token.Signer
	tokenTTL   time.Duration
}

func NewQueue(signer *token.Signer, tokenTTL time.Duration) *Queue {
	if tokenTTL <= 0 {
		tokenTTL = 2 * time.Minute
	}
	return &Queue{items: map[string][]Entry{}, byUser: map[string]map[string]Entry{}, watermarks: map[string]Watermark{}, signer: signer, tokenTTL: tokenTTL}
}
func (q *Queue) Join(ctx context.Context, item, user string) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	if item == "" || user == "" {
		return Entry{}, fmt.Errorf("item and user required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.byUser[item] == nil {
		q.byUser[item] = map[string]Entry{}
	}
	if e, ok := q.byUser[item][user]; ok {
		return e, nil
	}
	list := q.items[item]
	e := Entry{Item: item, User: user, Position: int64(len(list) + 1), AdmissionID: fmt.Sprintf("%s-%s-%d", item, user, time.Now().UnixNano()), JoinedAt: time.Now().UTC()}
	q.items[item] = append(list, e)
	q.byUser[item][user] = e
	return e, nil
}
func (q *Queue) PublishWatermark(w Watermark) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.watermarks[w.Item] = w
}

type Status struct {
	Position       int64     `json:"position"`
	RetryAfterMS   int64     `json:"retry_after_ms"`
	Watermark      Watermark `json:"watermark"`
	AdmissionToken string    `json:"admission_token,omitempty"`
	Admitted       bool      `json:"admitted"`
	Paused         bool      `json:"paused"`
	Terminal       bool      `json:"terminal"`
}

func (q *Queue) Poll(ctx context.Context, item, user string) (Status, error) {
	if err := ctx.Err(); err != nil {
		return Status{}, err
	}
	q.mu.Lock()
	e, ok := q.byUser[item][user]
	w, wok := q.watermarks[item]
	q.mu.Unlock()
	if !ok {
		return Status{}, fmt.Errorf("queue entry not found")
	}
	if !wok {
		return Status{Position: e.Position, RetryAfterMS: 1000}, nil
	}
	st := Status{Position: e.Position, RetryAfterMS: 1000, Watermark: w}
	st.Terminal = w.Inventory == InventorySoldOut
	st.Paused = w.Gate == GatePaused && !st.Terminal
	if st.Terminal || st.Paused || w.Gate != GateActive || w.Inventory != InventoryOpen || e.Position > w.Head {
		return st, nil
	}
	raw, err := q.signer.Issue(token.Claims{JTI: e.AdmissionID, Subject: user, Item: item, Audience: "flash-sale-api", Purpose: "admission"}, q.tokenTTL)
	if err != nil {
		return Status{}, err
	}
	st.AdmissionToken = raw
	st.Admitted = true
	return st, nil
}
func (q *Queue) UpdateGate(w Watermark) { q.PublishWatermark(w) }
func (q *Queue) Position(item, user string) (int64, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.byUser[item][user]
	return e.Position, ok
}

// SetRate changes the configured admission rate without advancing the queue
// head. The admission controller owns the once-per-tick head advancement.
func (q *Queue) SetRate(item string, rate int64) Watermark {
	q.mu.Lock()
	defer q.mu.Unlock()
	if rate < 0 {
		rate = 0
	}
	w := q.watermarks[item]
	w.Item = item
	w.Rate = rate
	w.UpdatedAt = time.Now().UTC()
	w.Version++
	q.watermarks[item] = w
	return w
}

// AdvanceTick admits at most Rate additional positions, and only while both
// the gate and inventory are open. A paused or sold-out queue keeps its place.
func (q *Queue) AdvanceTick(item string) Watermark {
	q.mu.Lock()
	defer q.mu.Unlock()
	w := q.watermarks[item]
	if w.Gate == GateActive && w.Inventory == InventoryOpen && w.Rate > 0 {
		w.Head += w.Rate
	}
	w.UpdatedAt = time.Now().UTC()
	w.Version++
	q.watermarks[item] = w
	return w
}

// Advance is retained for deterministic tests and operator-triggered demos.
// Production admission progression uses AdvanceTick from RunAdmissionController.
func (q *Queue) Advance(item string, rate int64) Watermark {
	q.mu.Lock()
	defer q.mu.Unlock()
	w := q.watermarks[item]
	if rate < 0 {
		rate = 0
	}
	w.Head += rate
	w.Rate = rate
	w.UpdatedAt = time.Now().UTC()
	w.Version++
	q.watermarks[item] = w
	return w
}

func (q *Queue) Watermark(item string) (Watermark, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	w, ok := q.watermarks[item]
	return w, ok
}
