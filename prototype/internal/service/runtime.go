package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"flashsale-platform/prototype/internal/admission"
	"flashsale-platform/prototype/internal/app"
	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/orders"
	"flashsale-platform/prototype/internal/pgwire"
	"flashsale-platform/prototype/internal/pipeline"
	"flashsale-platform/prototype/internal/psp"
	"flashsale-platform/prototype/internal/rediswire"
)

type Config struct {
	RedisSeeds        []string
	RedisCTRLAddress  string
	RedisKVAddress    string
	PostgresDSN       string
	PostgresPoolSize  int
	RedpandaURL       string
	PSPURL            string
	HomeRegion        string
	WriterID          string
	LeaseDuration     time.Duration
	LeaseEpoch        int64
	BootstrapLease    bool
	SimulateStandby   bool
	ItemID            string
	SeedUnits         int
	Shards            int
	HoldTTL           time.Duration
	PayingTTL         time.Duration
	ClaimRetention    time.Duration
	TerminalRetention time.Duration
	SigningKeyID      string
	SigningKey        []byte
	PSPWebhookSecret  []byte
	PSPRatePerSecond  float64
	PSPBurst          int
	PSPMaxConcurrent  int
}

type Runtime struct {
	Engine     *app.Engine
	Inventory  *inventory.RedisStore
	Repository *orders.PostgresRepository
	PG         *pgwire.Pool
	CTRL       *rediswire.Cluster
	KV         *rediswire.Cluster
	Log        *pipeline.RedpandaHTTPLog
	Gate       *orders.RedisStandbyGate
	PSPRaw     *psp.Client
	PSPGuard   *psp.GuardedClient
	Config     Config
}

func ConfigFromEnv() (Config, error) {
	seeds := strings.Split(env("REDIS_CLUSTER_SEEDS", "redis-inventory-1:6379,redis-inventory-2:6379,redis-inventory-3:6379"), ",")
	cfg := Config{
		RedisSeeds:        seeds,
		RedisCTRLAddress:  env("REDIS_CTRL_ADDR", "redis-ctrl:6379"),
		RedisKVAddress:    env("REDIS_KV_ADDR", "redis-kv:6379"),
		PostgresDSN:       os.Getenv("DATABASE_URL"),
		PostgresPoolSize:  envInt("DB_POOL_SIZE", 8),
		RedpandaURL:       env("REDPANDA_HTTP_URL", "http://redpanda:8082"),
		PSPURL:            env("PSP_URL", "http://mockpsp:8090"),
		HomeRegion:        env("HOME_REGION", "local-region"),
		WriterID:          env("REGION_WRITER_ID", "local-region-primary"),
		LeaseDuration:     time.Duration(envInt("LEASE_DURATION_SECONDS", 30)) * time.Second,
		LeaseEpoch:        int64(envInt("LEASE_EPOCH", 1)),
		BootstrapLease:    envBool("BOOTSTRAP_LEASE", false),
		SimulateStandby:   envBool("SIMULATE_STANDBY_ACK", false),
		ItemID:            env("ITEM_ID", "flash-sale-demo"),
		SeedUnits:         envInt("SEED_UNITS", 0),
		Shards:            envInt("INVENTORY_SHARDS", 3),
		HoldTTL:           time.Duration(envInt("HOLD_TTL_SECONDS", 300)) * time.Second,
		PayingTTL:         time.Duration(envInt("PAYING_TTL_SECONDS", 120)) * time.Second,
		ClaimRetention:    time.Duration(envInt("CLAIM_RETENTION_HOURS", 72)) * time.Hour,
		TerminalRetention: time.Duration(envInt("TERMINAL_RETENTION_HOURS", 24)) * time.Hour,
		SigningKeyID:      env("ADMISSION_SIGNING_KEY_ID", "admission-v1"),
		SigningKey:        []byte(os.Getenv("ADMISSION_SIGNING_KEY")),
		PSPWebhookSecret:  []byte(os.Getenv("PSP_WEBHOOK_SECRET")),
		PSPRatePerSecond:  float64(envInt("PSP_RATE_PER_SECOND", 1400)),
		PSPBurst:          envInt("PSP_BURST", 50),
		PSPMaxConcurrent:  envInt("PSP_MAX_CONCURRENT", 64),
	}
	if cfg.PostgresDSN == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if len(cfg.SigningKey) < 32 {
		return Config{}, fmt.Errorf("ADMISSION_SIGNING_KEY must be at least 32 bytes")
	}
	if len(cfg.PSPWebhookSecret) < 32 {
		return Config{}, fmt.Errorf("PSP_WEBHOOK_SECRET must be at least 32 bytes")
	}
	return cfg, nil
}

func Open(ctx context.Context, cfg Config) (*Runtime, error) {
	if cfg.HomeRegion == "" || cfg.WriterID == "" || cfg.PostgresDSN == "" || cfg.ItemID == "" {
		return nil, fmt.Errorf("runtime region, writer, database and item configuration are required")
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.LeaseEpoch <= 0 {
		cfg.LeaseEpoch = 1
	}
	if cfg.Shards <= 0 {
		cfg.Shards = 3
	}
	if cfg.PostgresPoolSize <= 0 {
		cfg.PostgresPoolSize = 8
	}
	if cfg.ClaimRetention <= 0 {
		cfg.ClaimRetention = 72 * time.Hour
	}
	if cfg.TerminalRetention <= 0 {
		cfg.TerminalRetention = 24 * time.Hour
	}

	cluster, err := rediswire.NewCluster(cfg.RedisSeeds, 3*time.Second)
	if err != nil {
		return nil, err
	}
	ctrl, err := rediswire.NewCluster([]string{cfg.RedisCTRLAddress}, 2*time.Second)
	if err != nil {
		return nil, err
	}
	kv, err := rediswire.NewCluster([]string{cfg.RedisKVAddress}, 2*time.Second)
	if err != nil {
		return nil, err
	}
	pg, err := pgwire.NewPool(cfg.PostgresDSN, cfg.PostgresPoolSize)
	if err != nil {
		return nil, err
	}
	cleanup := func(e error) (*Runtime, error) { _ = pg.Close(); return nil, e }
	repo, err := orders.NewPostgresRepository(pg, true)
	if err != nil {
		return cleanup(err)
	}
	if cfg.BootstrapLease {
		lease := orders.Lease{HomeRegion: cfg.HomeRegion, OwnerID: cfg.WriterID, Epoch: cfg.LeaseEpoch, ExpiresAt: time.Now().Add(cfg.LeaseDuration)}
		if err := repo.CreateLease(ctx, lease); err != nil {
			return cleanup(err)
		}
	}
	lease, err := repo.Lease(ctx, cfg.HomeRegion)
	if err != nil {
		return cleanup(fmt.Errorf("load regional lease (run bootstrap once if this is a new environment): %w", err))
	}
	if lease.OwnerID != cfg.WriterID || lease.Epoch != cfg.LeaseEpoch || !time.Now().Before(lease.ExpiresAt) {
		return cleanup(fmt.Errorf("configured logical writer/epoch does not match current lease owner=%q epoch=%d", lease.OwnerID, lease.Epoch))
	}

	inv, err := inventory.NewRedisStore(cluster, cfg.Shards, cfg.HoldTTL, cfg.PayingTTL, cfg.ClaimRetention, cfg.TerminalRetention)
	if err != nil {
		return cleanup(err)
	}
	status := &orders.RedisStatusStore{Client: kv, Retention: 30 * 24 * time.Hour}
	pspClient := &psp.Client{BaseURL: cfg.PSPURL}
	pspGuard, err := psp.NewGuardedClient(pspClient, psp.GuardConfig{RequestsPerSecond: cfg.PSPRatePerSecond, Burst: cfg.PSPBurst, MaxConcurrent: cfg.PSPMaxConcurrent, FailureThreshold: 5, OpenDuration: 5 * time.Second, AcquireTimeout: 2 * time.Second})
	if err != nil {
		return cleanup(err)
	}
	logAdapter, err := pipeline.NewRedpandaHTTPLog(cfg.RedpandaURL, nil)
	if err != nil {
		return cleanup(err)
	}
	consumerID := strings.TrimSpace(os.Getenv("RELAY_CONSUMER_ID"))
	if consumerID == "" {
		if host, hostErr := os.Hostname(); hostErr == nil && host != "" {
			consumerID = host
		} else {
			consumerID = fmt.Sprintf("relay-%d", time.Now().UnixNano())
		}
	}
	streamRelay, err := pipeline.NewRedisStreamRelay(inv, logAdapter, "checkout-events", "checkout-relay", consumerID)
	if err != nil {
		return cleanup(err)
	}
	relay := &pipeline.RedisRelayAdapter{Relay: streamRelay, Shards: cfg.Shards}
	gate := &orders.RedisStandbyGate{Client: ctrl, MaxAge: 3 * time.Second}
	watermarks := &admission.RedisWatermarkStore{Client: ctrl}
	engine, err := app.NewEngineWithDependencies(app.Config{HomeRegion: cfg.HomeRegion, WriterID: cfg.WriterID, LeaseEpoch: cfg.LeaseEpoch, QueueTokenTTL: 2 * time.Minute, PSPAmountMinor: 1000, SigningKeyID: cfg.SigningKeyID, SigningKey: cfg.SigningKey}, app.EngineDependencies{Inventory: inv, Repo: repo, Status: status, PSP: pspGuard, Gate: gate, Log: logAdapter, Relay: relay, Watermarks: watermarks})
	if err != nil {
		return cleanup(err)
	}
	engine.SetHealth(admission.Health{PSPHealthy: false, PSPCapacityPerSecond: 0, StandbyACKHealthy: false, ControlPlaneHealthy: false, Now: time.Now()})
	if cfg.SeedUnits > 0 {
		units := make([]string, cfg.SeedUnits)
		for i := range units {
			units[i] = fmt.Sprintf("unit-%06d", i+1)
		}
		if err := engine.SeedItem(ctx, cfg.ItemID, units); err != nil {
			return cleanup(err)
		}
	} else {
		engine.RegisterWorkerItem(cfg.ItemID)
	}
	return &Runtime{Engine: engine, Inventory: inv, Repository: repo, PG: pg, CTRL: ctrl, KV: kv, Log: logAdapter, Gate: gate, PSPRaw: pspClient, PSPGuard: pspGuard, Config: cfg}, nil
}

// RunStandbyMonitor publishes a timestamped signal to the dedicated CTRL Redis
// service. In real mode it checks PostgreSQL's configured synchronous standby;
// SIMULATE_STANDBY_ACK is only a laptop fault-injection mode and is not proof of
// remote WAL replication.
func (r *Runtime) RunStandbyMonitor(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		now := time.Now().UTC()
		healthy := r.Config.SimulateStandby
		reason := "standby ACK simulation enabled"
		if !r.Config.SimulateStandby {
			err := r.Repository.CheckSynchronousStandby(ctx)
			healthy = err == nil
			if err != nil {
				reason = err.Error()
			} else {
				reason = "synchronous standby streaming"
			}
		}
		recoveryPaused, recoveryErr := IsRecoveryPaused(ctx, r.CTRL, r.Config.HomeRegion)
		if recoveryErr != nil {
			healthy = false
			reason = "unable to read regional recovery gate: " + recoveryErr.Error()
		} else if recoveryPaused {
			healthy = false
			reason = "regional recovery scan/reconciliation in progress"
		}
		signal := orders.StandbySignal{HomeRegion: r.Config.HomeRegion, Epoch: r.Config.LeaseEpoch, Healthy: healthy, CheckedAt: now, Reason: reason}
		pubErr := r.Gate.Publish(ctx, signal)
		pspState := psp.GuardSnapshot{Healthy: true, CapacityPerSecond: int(r.Config.PSPRatePerSecond)}
		if r.PSPGuard != nil {
			pspState = r.PSPGuard.Snapshot()
		}
		workersHealthy, workersAt := r.WorkerHeartbeat(ctx, now)
		h := admission.Health{PSPHealthy: pspState.Healthy, PSPCapacityPerSecond: pspState.CapacityPerSecond, StandbyACKHealthy: healthy && pubErr == nil, ControlPlaneHealthy: pubErr == nil, WorkersMonitored: true, WorkersHealthy: workersHealthy, WorkersSignalAt: workersAt, Now: now}
		if h.StandbyACKHealthy {
			h.StandbyACKAt = now
		}
		r.Engine.SetHealth(h)
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

type workerHeartbeat struct {
	HomeRegion string    `json:"home_region"`
	WriterID   string    `json:"writer_id"`
	Epoch      int64     `json:"epoch"`
	Healthy    bool      `json:"healthy"`
	CheckedAt  time.Time `json:"checked_at"`
}

func workerHeartbeatKey(region string) string {
	return "ctrl:worker-heartbeat:{" + base64.RawURLEncoding.EncodeToString([]byte(region)) + "}"
}

// PublishWorkerHeartbeat is called only by the worker service. Its short TTL
// makes a dead/stalled consumer pause new admissions without treating stock as
// terminal SOLD_OUT.
func (r *Runtime) PublishWorkerHeartbeat(ctx context.Context, healthy bool, checkedAt time.Time) error {
	if r.CTRL == nil || r.Config.HomeRegion == "" || r.Config.WriterID == "" || r.Config.LeaseEpoch <= 0 {
		return fmt.Errorf("worker heartbeat configuration incomplete")
	}
	payload, err := json.Marshal(workerHeartbeat{HomeRegion: r.Config.HomeRegion, WriterID: r.Config.WriterID, Epoch: r.Config.LeaseEpoch, Healthy: healthy, CheckedAt: checkedAt.UTC()})
	if err != nil {
		return err
	}
	key := workerHeartbeatKey(r.Config.HomeRegion)
	_, err = r.CTRL.Do(ctx, key, "SET", key, string(payload), "EX", "3")
	return err
}

func (r *Runtime) WorkerHeartbeat(ctx context.Context, now time.Time) (bool, time.Time) {
	if r.CTRL == nil || r.Config.HomeRegion == "" {
		return false, time.Time{}
	}
	key := workerHeartbeatKey(r.Config.HomeRegion)
	v, err := r.CTRL.Do(ctx, key, "GET", key)
	if err != nil || v == nil {
		return false, time.Time{}
	}
	raw, ok := v.(string)
	if !ok {
		return false, time.Time{}
	}
	var heartbeat workerHeartbeat
	if json.Unmarshal([]byte(raw), &heartbeat) != nil {
		return false, time.Time{}
	}
	if heartbeat.HomeRegion != r.Config.HomeRegion || heartbeat.WriterID != r.Config.WriterID || heartbeat.Epoch != r.Config.LeaseEpoch || !heartbeat.Healthy || heartbeat.CheckedAt.IsZero() {
		return false, heartbeat.CheckedAt
	}
	if now.IsZero() {
		now = time.Now()
	}
	age := now.Sub(heartbeat.CheckedAt)
	return age >= 0 && age <= 3*time.Second, heartbeat.CheckedAt
}

func (r *Runtime) RunWorkers(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	const maxBackoff = 3 * time.Second
	backoff := interval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	publish := func(healthy bool) error {
		publishCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
		defer cancel()
		return r.PublishWorkerHeartbeat(publishCtx, healthy, time.Now())
	}
	wait := func(d time.Duration) error {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			markCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_ = r.PublishWorkerHeartbeat(markCtx, false, time.Now())
			cancel()
			return err
		}

		workerErr := r.Engine.WorkerTick(ctx)
		if workerErr == nil {
			workerErr = publish(true)
		}
		if workerErr != nil {
			markCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_ = r.PublishWorkerHeartbeat(markCtx, false, time.Now())
			cancel()
			if errors.Is(workerErr, context.Canceled) || errors.Is(workerErr, context.DeadlineExceeded) && ctx.Err() != nil {
				return ctx.Err()
			}
			// A stale epoch/lease is not transient: the worker must stop. Other
			// dependency failures retry with bounded backoff while the short-TTL
			// heartbeat makes the API pause new admissions.
			if errors.Is(workerErr, orders.ErrFenced) || errors.Is(workerErr, orders.ErrLeaseLost) {
				return fmt.Errorf("worker self-fenced: %w", workerErr)
			}
			if err := wait(backoff); err != nil {
				return err
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		backoff = interval
		select {
		case <-ctx.Done():
			markCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_ = r.PublishWorkerHeartbeat(markCtx, false, time.Now())
			cancel()
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Runtime) RunAdmissionController(ctx context.Context, interval time.Duration) error {
	return r.Engine.RunAdmissionController(ctx, interval)
}

func (r *Runtime) RunLeaseManager(ctx context.Context) error {
	manager := &orders.LeaseManager{Repo: r.Repository, Token: r.Engine.Lease, Duration: r.Config.LeaseDuration, RenewalLead: r.Config.LeaseDuration / 3, OnSelfFence: func(error) {
		r.Engine.SetPaused(r.Config.ItemID, true)
	}}
	return manager.Run(ctx)
}

func (r *Runtime) Close(ctx context.Context) error {
	if r.Log != nil {
		_ = r.Log.Close(ctx)
	}
	if r.PG != nil {
		return r.PG.Close()
	}
	return nil
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
func envBool(key string, fallback bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}
