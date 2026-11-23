package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"flashsale-platform/prototype/internal/admission"
	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/orders"
	"flashsale-platform/prototype/internal/pipeline"
	"flashsale-platform/prototype/internal/psp"
	"flashsale-platform/prototype/internal/token"
)

type Config struct {
	HomeRegion     string
	WriterID       string
	LeaseEpoch     int64
	QueueTokenTTL  time.Duration
	HoldTTL        time.Duration
	PayingTTL      time.Duration
	PSPAmountMinor int64
	SigningKeyID   string
	SigningKey     []byte
}

type RelayRunner interface {
	Once(context.Context, string) (int, error)
}

type EngineDependencies struct {
	Inventory  inventory.Store
	Repo       orders.Repository
	Status     orders.StatusStore
	PSP        orders.PSP
	Gate       orders.StandbyAckGate
	Log        pipeline.EventLog
	Relay      RelayRunner
	Watermarks interface {
		Put(context.Context, admission.Watermark) error
	}
}

type Engine struct {
	Inventory  inventory.Store
	Signer     *token.Signer
	Queue      *admission.Queue
	Controller *admission.Controller
	Repo       orders.Repository
	Status     orders.StatusStore
	PSP        orders.PSP
	Gate       orders.StandbyAckGate
	Processor  *orders.Processor
	Log        pipeline.EventLog
	Relay      RelayRunner
	Consumer   *pipeline.Consumer
	Watermarks interface {
		Put(context.Context, admission.Watermark) error
	}
	Mode                 string
	HomeRegion           string
	Lease                orders.LeaseToken
	mu                   sync.Mutex
	items                map[string]struct{}
	health               admission.Health
	stockAt              map[string]time.Time
	availability         map[string]availabilityCache
	availabilityInflight map[string]*availabilityCall
	lastWatermark        map[string]admission.Watermark
	requests             atomic.Uint64
	reserved             atomic.Uint64
	checkout             atomic.Uint64
	confirmed            atomic.Uint64
	cancelled            atomic.Uint64
	operatorPaused       map[string]bool
}
type availabilityCache struct {
	expires time.Time
	value   map[string]int
}
type availabilityCall struct {
	done  chan struct{}
	value map[string]int
	err   error
}

func NewDemoEngine(cfg Config) (*Engine, error) {
	if cfg.HomeRegion == "" {
		cfg.HomeRegion = "local-region"
	}
	if cfg.WriterID == "" {
		cfg.WriterID = cfg.HomeRegion + "-primary"
	}
	if cfg.LeaseEpoch <= 0 {
		cfg.LeaseEpoch = 1
	}
	inv := inventory.NewMemoryStore(cfg.HoldTTL, cfg.PayingTTL)
	repo := orders.NewMemoryRepository()
	repo.SeedLease(cfg.HomeRegion, cfg.WriterID, cfg.LeaseEpoch, 24*time.Hour)
	status := orders.NewMemoryStatusStore()
	mock := psp.NewMock()
	gate := &orders.StaticStandbyGate{Available: true}
	log := pipeline.NewMemoryLog()
	relay := pipeline.NewRelay(inv, log, "checkout-events")
	e, err := NewEngineWithDependencies(cfg, EngineDependencies{Inventory: inv, Repo: repo, Status: status, PSP: psp.DirectClient{Mock: mock}, Gate: gate, Log: log, Relay: relay})
	if err == nil {
		e.Mode = "in-memory-demo"
	}
	return e, err
}

// NewEngineWithDependencies wires the same HTTP/admission surface to either
// deterministic in-memory adapters or the Redis/PostgreSQL/Redpanda adapters.
// The waiting-room queue remains process-local in this prototype; production
// uses the external waiting-room service described in the HLD.
func NewEngineWithDependencies(cfg Config, deps EngineDependencies) (*Engine, error) {
	if cfg.HomeRegion == "" || cfg.WriterID == "" || cfg.LeaseEpoch <= 0 {
		return nil, fmt.Errorf("home region, logical writer ID and positive epoch are required")
	}
	if deps.Inventory == nil || deps.Repo == nil || deps.Status == nil || deps.PSP == nil || deps.Gate == nil || deps.Log == nil || deps.Relay == nil {
		return nil, fmt.Errorf("engine dependencies incomplete")
	}
	keyID := cfg.SigningKeyID
	if keyID == "" {
		keyID = "dev-k1"
	}
	key := cfg.SigningKey
	if len(key) == 0 {
		key = []byte("development-only-change-me-32-bytes!")
	}
	signer, err := token.NewSigner(keyID, map[string][]byte{keyID: key})
	if err != nil {
		return nil, err
	}
	lease := orders.LeaseToken{HomeRegion: cfg.HomeRegion, OwnerID: cfg.WriterID, Epoch: cfg.LeaseEpoch}
	proc := &orders.Processor{Repo: deps.Repo, Inventory: deps.Inventory, PSP: deps.PSP, Status: deps.Status, Gate: deps.Gate, Lease: lease, HomeRegion: cfg.HomeRegion, AmountMinor: cfg.PSPAmountMinor, Currency: "USD"}
	consumer := pipeline.NewConsumer(deps.Log, "checkout-events", "order-workers", proc.Process)
	consumer.MaxInlineRetries = 2
	consumer.MaxRequeues = 2
	e := &Engine{Inventory: deps.Inventory, Signer: signer, Queue: admission.NewQueue(signer, cfg.QueueTokenTTL), Controller: admission.NewController(2*time.Second, 3*time.Second), Repo: deps.Repo, Status: deps.Status, PSP: deps.PSP, Gate: deps.Gate, Processor: proc, Log: deps.Log, Relay: deps.Relay, Consumer: consumer, Watermarks: deps.Watermarks, Mode: "distributed-adapters", HomeRegion: cfg.HomeRegion, Lease: lease, items: map[string]struct{}{}, operatorPaused: map[string]bool{}, stockAt: map[string]time.Time{}, availability: map[string]availabilityCache{}, availabilityInflight: map[string]*availabilityCall{}, lastWatermark: map[string]admission.Watermark{}}
	proc.OnPause = func(reason string) {
		e.mu.Lock()
		e.health.StandbyACKHealthy = false
		e.health.StandbyACKAt = time.Time{}
		e.mu.Unlock()
		e.refreshAllGate(reason)
	}
	e.health = admission.Health{PSPHealthy: true, PSPCapacityPerSecond: 1400, StandbyACKHealthy: true, StandbyACKAt: time.Now(), ControlPlaneHealthy: true, Now: time.Now()}
	return e, nil
}

func (e *Engine) registerItem(item string) bool {
	if item == "" {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.items[item]; exists {
		return false
	}
	e.items[item] = struct{}{}
	return true
}

func (e *Engine) RegisterItem(item string) {
	if !e.registerItem(item) {
		return
	}
	// An API process hydrates its watermark from authoritative inventory.
	// Do not advance the queue head here; AdmissionTick is the only advancer.
	wm, ok := e.Queue.Watermark(item)
	rate, head := int64(1750), int64(0)
	if ok {
		rate, head = wm.Rate, wm.Head
		if rate <= 0 {
			rate = 1750
		}
	}
	e.Queue.SetRate(item, rate)
	e.RefreshAdmission(item, head, rate)
}

// RegisterWorkerItem registers an item for relay/reaper loops without writing
// an admission watermark. Only the API/admission-controller process owns that
// control-plane projection; workers must not overwrite it with local health.
func (e *Engine) RegisterWorkerItem(item string) {
	e.registerItem(item)
}

func (e *Engine) SeedItem(ctx context.Context, item string, units []string) error {
	if err := e.Inventory.Seed(ctx, item, units); err != nil {
		return err
	}
	e.mu.Lock()
	e.items[item] = struct{}{}
	e.stockAt[item] = time.Now()
	e.mu.Unlock()
	wm, ok := e.Queue.Watermark(item)
	rate := int64(1750)
	head := int64(0)
	if ok {
		head, rate = wm.Head, wm.Rate
		if rate <= 0 {
			rate = 1750
		}
	}
	e.Queue.SetRate(item, rate)
	e.RefreshAdmission(item, head, rate)
	return nil
}
func (e *Engine) SetHealth(h admission.Health) {
	e.mu.Lock()
	e.health = h
	e.mu.Unlock()
	e.refreshAllGate("health changed")
}
func (e *Engine) RefreshAdmission(item string, head, rate int64) admission.Watermark {
	avail, held, paying, confirmed, forfeited, total, err := e.Inventory.Availability(context.Background(), item)
	now := time.Now()
	e.mu.Lock()
	h := e.health
	if err == nil {
		e.stockAt[item] = now
	} else {
		h.ControlPlaneHealthy = false
	}
	stockAt := e.stockAt[item]
	operatorPaused := e.operatorPaused[item]
	h.Now = now
	e.mu.Unlock()
	w := e.Controller.Evaluate(item, head, rate, admission.Stock{Available: avail, Held: held, Paying: paying, Confirmed: confirmed, Forfeited: forfeited, Total: total, Known: err == nil}, stockAt, h)
	if operatorPaused {
		w.Gate = admission.GatePaused
		w.Reason = "operator pause"
	}
	if e.Watermarks != nil {
		if err := e.Watermarks.Put(context.Background(), w); err != nil {
			w.Gate = admission.GatePaused
			w.Reason = "control-plane watermark publish failed"
		}
	}
	e.Queue.PublishWatermark(w)
	e.mu.Lock()
	e.lastWatermark[item] = w
	e.mu.Unlock()
	return w
}
func (e *Engine) refreshAllGate(reason string) {
	e.mu.Lock()
	items := make([]string, 0, len(e.items))
	for item := range e.items {
		items = append(items, item)
	}
	health := e.health
	e.mu.Unlock()
	for _, item := range items {
		avail, held, paying, confirmed, forfeited, total, err := e.Inventory.Availability(context.Background(), item)
		now := time.Now()
		prior, priorOK := e.Queue.Watermark(item)
		head, rate := int64(0), int64(1750)
		if priorOK {
			head, rate = prior.Head, prior.Rate
			if rate <= 0 {
				rate = 1750
			}
		}
		e.mu.Lock()
		h := health
		if err == nil {
			e.stockAt[item] = now
		} else {
			h.ControlPlaneHealthy = false
		}
		stockAt := e.stockAt[item]
		operatorPaused := e.operatorPaused[item]
		e.mu.Unlock()
		h.Now = now
		w := e.Controller.Evaluate(item, head, rate, admission.Stock{Available: avail, Held: held, Paying: paying, Confirmed: confirmed, Forfeited: forfeited, Total: total, Known: err == nil}, stockAt, h)
		if operatorPaused {
			w.Gate = admission.GatePaused
			w.Reason = "operator pause"
		}
		if w.Gate == admission.GatePaused && w.Reason == "" {
			w.Reason = reason
		}
		if e.Watermarks != nil {
			if err := e.Watermarks.Put(context.Background(), w); err != nil {
				w.Gate = admission.GatePaused
				w.Reason = "control-plane watermark publish failed"
			}
		}
		e.Queue.PublishWatermark(w)
		e.mu.Lock()
		e.lastWatermark[item] = w
		e.mu.Unlock()
	}
}
func (e *Engine) SetStandbyAvailable(available bool) {
	if setter, ok := e.Gate.(interface{ SetAvailable(bool) }); ok {
		setter.SetAvailable(available)
	}
	e.mu.Lock()
	e.health.StandbyACKHealthy = available
	if available {
		e.health.StandbyACKAt = time.Now()
	} else {
		e.health.StandbyACKAt = time.Time{}
	}
	e.mu.Unlock()
	e.refreshAllGate("remote standby ACK unavailable")
}
func (e *Engine) SetPSPHealth(healthy bool, capacity int) {
	e.mu.Lock()
	e.health.PSPHealthy = healthy
	e.health.PSPCapacityPerSecond = capacity
	e.mu.Unlock()
	e.refreshAllGate("PSP breaker-open or capacity unavailable")
}
func (e *Engine) Join(ctx context.Context, item, user string) (admission.Entry, error) {
	return e.Queue.Join(ctx, item, user)
}
func (e *Engine) Poll(ctx context.Context, item, user string) (admission.Status, error) {
	return e.Queue.Poll(ctx, item, user)
}
func (e *Engine) AdvanceQueue(item string, amount int64) admission.Watermark {
	w := e.Queue.Advance(item, amount)
	return e.RefreshAdmission(item, w.Head, w.Rate)
}

func (e *Engine) SetAdmissionRate(item string, rate int64) admission.Watermark {
	w := e.Queue.SetRate(item, rate)
	return e.RefreshAdmission(item, w.Head, w.Rate)
}

// AdmissionTick advances the waiting-room head once per controller tick. It
// never advances positions while paused, stale, or sold out.
func (e *Engine) AdmissionTick(ctx context.Context) error {
	for _, item := range e.Items() {
		if err := ctx.Err(); err != nil {
			return err
		}
		prior, ok := e.Queue.Watermark(item)
		if !ok {
			prior = admission.Watermark{Item: item, Rate: 1750, Gate: admission.GatePaused, Inventory: admission.InventoryTemporary}
			e.Queue.PublishWatermark(prior)
		}
		current := e.RefreshAdmission(item, prior.Head, prior.Rate)
		advanced := e.Queue.AdvanceTick(item)
		// Keep the refreshed gate state; AdvanceTick advances only when active.
		if advanced.Gate != current.Gate || advanced.Inventory != current.Inventory {
			advanced.Gate, advanced.Inventory, advanced.Reason = current.Gate, current.Inventory, current.Reason
			e.Queue.PublishWatermark(advanced)
		}
		e.RefreshAdmission(item, advanced.Head, advanced.Rate)
	}
	return nil
}

func (e *Engine) RunAdmissionController(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	if err := e.AdmissionTick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := e.AdmissionTick(ctx); err != nil {
				return err
			}
		}
	}
}
func (e *Engine) Reserve(ctx context.Context, rawToken, item string) (inventory.Hold, error) {
	claims, err := e.Signer.Verify(rawToken, "flash-sale-api", "admission")
	if err != nil {
		return inventory.Hold{}, err
	}
	if claims.Item != item {
		return inventory.Hold{}, fmt.Errorf("token item mismatch")
	}
	w := e.currentWatermark(item)
	if w.Gate != admission.GateActive || w.Inventory != admission.InventoryOpen {
		return inventory.Hold{}, fmt.Errorf("admissions paused or inventory unavailable")
	}
	h, err := e.Inventory.Reserve(ctx, item, claims.JTI, claims.Subject, "")
	if err == nil {
		e.reserved.Add(1)
		e.RefreshAdmission(item, w.Head, w.Rate)
	}
	return h, err
}
func (e *Engine) Checkout(ctx context.Context, item, holdID, user string) (inventory.Hold, error) {
	h, found, err := e.Inventory.Get(ctx, item, holdID)
	if err != nil {
		return inventory.Hold{}, err
	}
	if !found {
		return inventory.Hold{}, inventory.ErrHoldNotFound
	}
	if user != "" && h.User != user {
		return inventory.Hold{}, fmt.Errorf("hold owner mismatch")
	}
	locked, err := e.Inventory.LockForPayment(ctx, item, holdID)
	if err == nil {
		e.checkout.Add(1)
	}
	return locked, err
}
func (e *Engine) currentWatermark(item string) admission.Watermark {
	if w, ok := e.Queue.Watermark(item); ok {
		return w
	}
	return admission.Watermark{Item: item, Gate: admission.GatePaused, Inventory: admission.InventoryTemporary}
}

func (e *Engine) SetPaused(item string, paused bool) admission.Watermark {
	e.mu.Lock()
	e.operatorPaused[item] = paused
	e.mu.Unlock()
	old := e.currentWatermark(item)
	return e.RefreshAdmission(item, old.Head, old.Rate)
}

func (e *Engine) RelayOnce(ctx context.Context) (int, error) {
	e.mu.Lock()
	items := make([]string, 0, len(e.items))
	for item := range e.items {
		items = append(items, item)
	}
	e.mu.Unlock()
	total := 0
	for _, item := range items {
		n, err := e.Relay.Once(ctx, item)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
func (e *Engine) ProcessOnce(ctx context.Context) (bool, error) { return e.Consumer.Once(ctx) }
func (e *Engine) Drain(ctx context.Context, max int) error {
	if max <= 0 {
		max = 10000
	}
	for i := 0; i < max; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := e.RelayOnce(ctx)
		if err != nil {
			return err
		}
		processed, processErr := e.Consumer.Once(ctx)
		if processErr != nil {
			if errors.Is(processErr, orders.ErrStandbyUnavailable) {
				return processErr
			}
			continue
		}
		if n == 0 && !processed {
			return nil
		}
	}
	return fmt.Errorf("drain exceeded %d iterations", max)
}
func (e *Engine) CaptureOnce(ctx context.Context, limit int) error {
	return e.Processor.CapturePending(ctx, limit)
}

func (e *Engine) HandlePSPWebhook(ctx context.Context, charge psp.Charge) error {
	return e.Processor.HandleWebhook(ctx, charge)
}

// Items returns a stable snapshot of seeded item identifiers.
func (e *Engine) Items() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	items := make([]string, 0, len(e.items))
	for item := range e.items {
		items = append(items, item)
	}
	sort.Strings(items)
	return items
}

// WorkerTick advances the local demo workers once. Production deployments use
// the Redis/Redpanda/PG adapters and independent processes; this loop is a
// deterministic single-process harness for the assignment demo.
func (e *Engine) WorkerTick(ctx context.Context) error {
	items := e.Items()
	for _, item := range items {
		for i := 0; i < 20; i++ {
			n, err := e.Relay.Once(ctx, item)
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
		}
	}
	for i := 0; i < 500; i++ {
		processed, err := e.Consumer.Once(ctx)
		if err != nil {
			if errors.Is(err, orders.ErrStandbyUnavailable) {
				break
			}
			// The consumer retains the uncommitted message and retries on the next tick.
			break
		}
		if !processed {
			break
		}
	}
	for _, item := range items {
		shards := 1
		if redisStore, ok := e.Inventory.(*inventory.RedisStore); ok {
			shards = redisStore.Shards
		}
		for shard := 0; shard < shards; shard++ {
			if _, _, err := e.Inventory.Reap(ctx, item, shard, 100); err != nil {
				return err
			}
			if redisStore, ok := e.Inventory.(*inventory.RedisStore); ok {
				if _, err := redisStore.CleanupTerminals(ctx, item, shard, 100); err != nil {
					return err
				}
			}
		}
	}
	return e.CaptureOnce(ctx, 100)
}

func (e *Engine) RunWorkers(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := e.WorkerTick(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
func (e *Engine) StatusFor(ctx context.Context, item, orderID, user string) (map[string]any, error) {
	// The versioned KV projection carries the immutable owner identity. Do not
	// return a cached status when the owner cannot be established.
	p, found, projectionErr := e.Status.Get(ctx, orderID)
	if projectionErr == nil && found {
		if user == "" || p.UserID == "" || p.UserID != user || (item != "" && p.ItemID != item) {
			return nil, fmt.Errorf("order owner or item mismatch")
		}
		return map[string]any{"order_id": p.OrderID, "status": p.OrderStatus, "payment_status": p.PaymentStatus, "version": p.Version}, nil
	}

	// KV miss, stale projection, or KV outage falls back to the authoritative
	// PostgreSQL record. Projection is repaired only after ownership is checked.
	a, dbErr := e.Repo.Get(ctx, orderID)
	if dbErr == nil {
		if user == "" || a.Order.UserID != user || (item != "" && a.Order.ItemID != item) {
			return nil, fmt.Errorf("order owner or item mismatch")
		}
		_ = e.Status.Project(ctx, a)
		return map[string]any{"order_id": a.Order.ID, "status": a.Order.Status, "payment_status": a.Payment.Status, "version": a.Order.Version}, nil
	}
	if !errors.Is(dbErr, orders.ErrOrderNotFound) {
		return nil, dbErr
	}
	if item != "" {
		if h, ok, getErr := e.Inventory.Get(ctx, item, orderID); getErr == nil && ok {
			if user == "" || h.User != user {
				return nil, fmt.Errorf("hold owner mismatch")
			}
			return map[string]any{"order_id": orderID, "status": "PROCESSING", "payment_status": "NOT_CREATED", "hold_state": h.State}, nil
		} else if getErr != nil {
			return nil, getErr
		}
	}
	if projectionErr != nil {
		return nil, fmt.Errorf("status KV unavailable (%v); %w", projectionErr, dbErr)
	}
	return nil, orders.ErrOrderNotFound
}
func (e *Engine) Availability(ctx context.Context, item string) (map[string]int, error) {
	clone := func(in map[string]int) map[string]int {
		out := make(map[string]int, len(in))
		for k, v := range in {
			out[k] = v
		}
		return out
	}
	e.mu.Lock()
	if c, ok := e.availability[item]; ok && time.Now().Before(c.expires) {
		out := clone(c.value)
		e.mu.Unlock()
		return out, nil
	}
	if call := e.availabilityInflight[item]; call != nil {
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
		}
		if call.err != nil {
			return nil, call.err
		}
		return clone(call.value), nil
	}
	call := &availabilityCall{done: make(chan struct{})}
	e.availabilityInflight[item] = call
	e.mu.Unlock()
	a, h, p, c, f, total, err := e.Inventory.Availability(ctx, item)
	var value map[string]int
	if err == nil {
		value = map[string]int{"available": a, "held": h, "paying": p, "confirmed": c, "forfeited": f, "total": total}
	}
	e.mu.Lock()
	call.value, call.err = value, err
	if err == nil {
		e.availability[item] = availabilityCache{expires: time.Now().Add(time.Second), value: clone(value)}
	}
	delete(e.availabilityInflight, item)
	close(call.done)
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return clone(value), nil
}

func (e *Engine) Metrics() map[string]uint64 {
	return map[string]uint64{"requests_total": e.requests.Load(), "reservations_total": e.reserved.Load(), "checkouts_total": e.checkout.Load(), "confirmed_total": e.confirmed.Load(), "cancelled_total": e.cancelled.Load()}
}
