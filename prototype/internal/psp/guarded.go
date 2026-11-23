package psp

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrCircuitOpen  = errors.New("PSP circuit breaker is open")
	ErrBulkheadFull = errors.New("PSP bulkhead is full")
	ErrGuardRate    = errors.New("PSP client rate budget exhausted")
)

type Operations interface {
	Authorize(string, Request) (Charge, error)
	Get(string) (Charge, error)
	Capture(string) (Charge, error)
	Void(string) (Charge, error)
}

type GuardConfig struct {
	RequestsPerSecond float64
	Burst             int
	MaxConcurrent     int
	FailureThreshold  int
	OpenDuration      time.Duration
	AcquireTimeout    time.Duration
}

type GuardSnapshot struct {
	Healthy            bool
	CapacityPerSecond  int
	OpenUntil          time.Time
	ConsecutiveFailure int
	InFlight           int
}

type GuardedClient struct {
	base Operations
	cfg  GuardConfig

	mu               sync.Mutex
	tokens           float64
	lastRefill       time.Time
	failures         int
	openUntil        time.Time
	halfOpenInFlight bool
	inFlight         int
	now              func() time.Time
	sleep            func(time.Duration)
	bulkhead         chan struct{}
}

func NewGuardedClient(base Operations, cfg GuardConfig) (*GuardedClient, error) {
	if base == nil {
		return nil, fmt.Errorf("PSP operations client required")
	}
	if cfg.RequestsPerSecond <= 0 {
		cfg.RequestsPerSecond = 1400
	}
	if cfg.Burst <= 0 {
		cfg.Burst = 50
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 64
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenDuration <= 0 {
		cfg.OpenDuration = 5 * time.Second
	}
	if cfg.AcquireTimeout <= 0 {
		cfg.AcquireTimeout = 2 * time.Second
	}
	now := time.Now()
	return &GuardedClient{base: base, cfg: cfg, tokens: float64(cfg.Burst), lastRefill: now, now: time.Now, sleep: time.Sleep, bulkhead: make(chan struct{}, cfg.MaxConcurrent)}, nil
}

func (g *GuardedClient) acquireRate() error {
	deadline := g.now().Add(g.cfg.AcquireTimeout)
	for {
		g.mu.Lock()
		now := g.now()
		elapsed := now.Sub(g.lastRefill).Seconds()
		if elapsed > 0 {
			g.tokens += elapsed * g.cfg.RequestsPerSecond
			if g.tokens > float64(g.cfg.Burst) {
				g.tokens = float64(g.cfg.Burst)
			}
			g.lastRefill = now
		}
		if g.tokens >= 1 {
			g.tokens--
			g.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - g.tokens) / g.cfg.RequestsPerSecond * float64(time.Second))
		g.mu.Unlock()
		if now.Add(wait).After(deadline) {
			return ErrGuardRate
		}
		g.sleep(wait)
	}
}

func (g *GuardedClient) begin() error {
	now := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.openUntil.IsZero() {
		if now.Before(g.openUntil) {
			return ErrCircuitOpen
		}
		if g.halfOpenInFlight {
			return ErrCircuitOpen
		}
		g.halfOpenInFlight = true
	}
	return nil
}
func (g *GuardedClient) finish(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err == nil {
		g.failures = 0
		g.openUntil = time.Time{}
		g.halfOpenInFlight = false
		return
	}
	// A declined authorization and an already-voided key are definitive PSP
	// business outcomes, not dependency-health failures.
	if errors.Is(err, ErrDeclined) || errors.Is(err, ErrVoided) || errors.Is(err, ErrNotFound) {
		g.halfOpenInFlight = false
		return
	}
	g.failures++
	g.halfOpenInFlight = false
	if g.failures >= g.cfg.FailureThreshold {
		g.openUntil = g.now().Add(g.cfg.OpenDuration)
	}
}
func (g *GuardedClient) call(fn func() (Charge, error)) (Charge, error) {
	if err := g.begin(); err != nil {
		return Charge{}, err
	}
	if err := g.acquireRate(); err != nil {
		g.finish(err)
		return Charge{}, err
	}
	select {
	case g.bulkhead <- struct{}{}:
		g.mu.Lock()
		g.inFlight++
		g.mu.Unlock()
	default:
		g.finish(ErrBulkheadFull)
		return Charge{}, ErrBulkheadFull
	}
	c, err := fn()
	<-g.bulkhead
	g.mu.Lock()
	g.inFlight--
	g.mu.Unlock()
	g.finish(err)
	return c, err
}
func (g *GuardedClient) Authorize(key string, req Request) (Charge, error) {
	return g.call(func() (Charge, error) { return g.base.Authorize(key, req) })
}
func (g *GuardedClient) Get(key string) (Charge, error) {
	return g.call(func() (Charge, error) { return g.base.Get(key) })
}
func (g *GuardedClient) Capture(key string) (Charge, error) {
	return g.call(func() (Charge, error) { return g.base.Capture(key) })
}
func (g *GuardedClient) Void(key string) (Charge, error) {
	return g.call(func() (Charge, error) { return g.base.Void(key) })
}
func (g *GuardedClient) Snapshot() GuardSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	healthy := g.openUntil.IsZero() || !g.now().Before(g.openUntil)
	return GuardSnapshot{Healthy: healthy, CapacityPerSecond: int(g.cfg.RequestsPerSecond), OpenUntil: g.openUntil, ConsecutiveFailure: g.failures, InFlight: g.inFlight}
}

var _ Operations = (*GuardedClient)(nil)
