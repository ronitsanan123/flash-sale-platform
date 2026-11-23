package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"flashsale-platform/prototype/internal/service"
)

func main() {
	cfg, err := service.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rt, err := service.Open(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rt.Close(closeCtx)
	}()

	if envBool("RUN_STANDBY_MONITOR", false) {
		go func() {
			if err := rt.RunStandbyMonitor(ctx, 500*time.Millisecond); err != nil && err != context.Canceled {
				log.Printf("standby monitor stopped: %v", err)
				stop()
			}
		}()
	}
	if envBool("RUN_LEASE_MANAGER", true) {
		go func() {
			if err := rt.RunLeaseManager(ctx); err != nil {
				log.Printf("lease manager self-fenced: %v", err)
				stop()
			}
		}()
	}
	log.Printf("worker started region=%s writer=%s epoch=%d item=%s shards=%d", cfg.HomeRegion, cfg.WriterID, cfg.LeaseEpoch, cfg.ItemID, cfg.Shards)
	if err := rt.RunWorkers(ctx, 100*time.Millisecond); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}
