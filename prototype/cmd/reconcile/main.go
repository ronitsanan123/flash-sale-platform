package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flashsale-platform/prototype/internal/orders"
	"flashsale-platform/prototype/internal/rediswire"
	"flashsale-platform/prototype/internal/service"
)

type report struct {
	HomeRegion      string    `json:"home_region"`
	WriterID        string    `json:"writer_id"`
	Epoch           int64     `json:"fencing_epoch"`
	StartedAt       time.Time `json:"started_at"`
	CompletedAt     time.Time `json:"completed_at"`
	ScannedEvents   int       `json:"scanned_events"`
	DiscoveredIDs   int       `json:"discovered_ids"`
	Preserved       int       `json:"preserved_confirmations"`
	Voided          int       `json:"orphan_authorizations_voided"`
	Declined        int       `json:"definitive_declines"`
	CompleteThrough time.Time `json:"complete_through"`
	SnapshotID      string    `json:"snapshot_id"`
}

func main() {
	fromText := flag.String("from", os.Getenv("RECOVERY_SCAN_FROM"), "RFC3339 earliest scan bound (required)")
	failoverText := flag.String("failover-detected-at", "", "RFC3339 failure detection time; defaults to current UTC time")
	settlement := flag.Duration("settlement-grace", envDuration("AUTHORIZATION_SETTLEMENT_GRACE", 2*time.Minute), "PSP maximum authorization settlement delay plus clock-skew allowance")
	overlap := flag.Duration("overlap", envDuration("RECOVERY_SCAN_OVERLAP", 2*time.Minute), "overlap before the last complete-through checkpoint")
	pageSize := flag.Int("page-size", 100, "PSP ledger page size (1..1000)")
	oldPrimaryFenced := flag.Bool("old-primary-fenced", false, "operator attestation: old home-region primary is unreachable or infrastructure-fenced")
	flag.Parse()

	if !*oldPrimaryFenced {
		log.Fatal("refusing regional recovery: pass --old-primary-fenced only after independently verifying the old primary is unreachable or infrastructure-fenced")
	}
	if *settlement <= 0 || *overlap < 0 || *pageSize < 1 || *pageSize > 1000 {
		log.Fatal("invalid settlement-grace, overlap or page-size")
	}

	cfg, err := service.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	if cfg.BootstrapLease {
		log.Fatal("BOOTSTRAP_LEASE must be false for recovery; promote the standby lease explicitly after old-primary fencing")
	}
	if *fromText == "" {
		log.Fatal("--from is required when no PSP ledger checkpoint exists")
	}
	from, err := time.Parse(time.RFC3339Nano, *fromText)
	if err != nil {
		log.Fatalf("invalid --from: %v", err)
	}
	failoverAt := time.Now().UTC()
	if *failoverText != "" {
		failoverAt, err = time.Parse(time.RFC3339Nano, *failoverText)
		if err != nil {
			log.Fatalf("invalid --failover-detected-at: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctrl, err := rediswire.NewCluster([]string{cfg.RedisCTRLAddress}, 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	// Pause before opening the recovery runtime. If any later step fails, keep
	// the key in CTRL; the API's standby monitor will continue to fail closed.
	if err := service.SetRecoveryPauseSignal(ctx, ctrl, cfg.HomeRegion, true); err != nil {
		log.Fatalf("set regional recovery pause: %v", err)
	}

	rt, err := service.Open(ctx, cfg)
	if err != nil {
		log.Fatalf("open promoted-region runtime (pause remains set): %v", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rt.Close(closeCtx)
	}()

	// In a real deployment, do not run regional recovery against a promoted
	// database unless its remote synchronous standby is actually configured
	// and streaming. The single-node Compose profile deliberately uses the
	// application-level StandbyAckGate simulation instead.
	if !rt.Config.SimulateStandby {
		if err := rt.Repository.CheckSynchronousStandby(ctx); err != nil {
			log.Fatalf("promoted database has no healthy synchronous remote standby; recovery pause remains set: %v", err)
		}
	}

	started := time.Now().UTC()
	lease := orders.LeaseToken{HomeRegion: cfg.HomeRegion, OwnerID: cfg.WriterID, Epoch: cfg.LeaseEpoch}
	scanner := orders.RecoveryScanner{
		PSP: rt.PSPRaw, Repo: rt.Repository, Inventory: rt.Inventory,
		PageSize: *pageSize, Overlap: *overlap, SettlementGrace: *settlement,
	}
	result, err := scanner.Run(ctx, lease, from, failoverAt)
	if err != nil {
		log.Fatalf("regional recovery incomplete; admissions remain paused: %v", err)
	}
	// Clear only after all pages are complete, PSP outcomes resolved, durable
	// recovery records written and Redis inventory repaired.
	if err := service.SetRecoveryPauseSignal(ctx, rt.CTRL, cfg.HomeRegion, false); err != nil {
		log.Fatalf("recovery completed but could not clear pause; keep admissions paused: %v", err)
	}
	out := report{HomeRegion: cfg.HomeRegion, WriterID: cfg.WriterID, Epoch: cfg.LeaseEpoch, StartedAt: started, CompletedAt: time.Now().UTC(), ScannedEvents: result.ScannedEvents, DiscoveredIDs: result.DiscoveredIDs, Preserved: result.Preserved, Voided: result.Voided, Declined: result.Declined, CompleteThrough: result.CompleteThrough, SnapshotID: result.SnapshotID}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
