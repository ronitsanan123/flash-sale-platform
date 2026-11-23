package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"flashsale-platform/prototype/internal/app"
	"flashsale-platform/prototype/internal/service"
	"flashsale-platform/prototype/internal/shed"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if strings.EqualFold(os.Getenv("MODE"), "distributed") {
		if err := runDistributed(ctx, stop); err != nil && err != context.Canceled {
			log.Fatal(err)
		}
		return
	}
	if err := runDemo(ctx, stop); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}

func runDemo(ctx context.Context, stop context.CancelFunc) error {
	item := env("ITEM_ID", "flash-sale-demo")
	units := envInt("SEED_UNITS", 500)
	region := env("HOME_REGION", "local-region")
	writer := env("REGION_WRITER_ID", region+"-primary")
	engine, err := app.NewDemoEngine(app.Config{HomeRegion: region, WriterID: writer, LeaseEpoch: 1, QueueTokenTTL: 2 * time.Minute, HoldTTL: 5 * time.Minute, PayingTTL: 2 * time.Minute})
	if err != nil {
		return err
	}
	seed := make([]string, units)
	for i := range seed {
		seed[i] = fmt.Sprintf("unit-%06d", i+1)
	}
	if err := engine.SeedItem(ctx, item, seed); err != nil {
		return err
	}
	adminToken := os.Getenv("ADMIN_TOKEN")
	if adminToken == "" {
		if os.Getenv("MODE") != "demo" {
			return fmt.Errorf("ADMIN_TOKEN must be set; set MODE=demo only for local demonstration")
		}
		adminToken = "dev-admin-change-me"
	}
	addr := env("HTTP_ADDR", ":8080")
	server := &http.Server{Addr: addr, Handler: (&app.HTTPServer{Engine: engine, AdminToken: adminToken, RequestTimeout: 3 * time.Second}).Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := engine.RunWorkers(ctx, 100*time.Millisecond); err != nil && err != context.Canceled {
			log.Printf("demo worker stopped: %v", err)
			stop()
		}
	}()
	go func() {
		if err := engine.RunAdmissionController(ctx, time.Second); err != nil && err != context.Canceled {
			log.Printf("demo admission controller stopped: %v", err)
			stop()
		}
	}()
	log.Printf("flash-sale demo listening on %s item=%s units=%d backend=in-memory", addr, item, units)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return ctx.Err()
}

func runDistributed(ctx context.Context, stop context.CancelFunc) error {
	cfg, err := service.ConfigFromEnv()
	if err != nil {
		return err
	}
	rt, err := service.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rt.Close(closeCtx)
	}()
	// This is a single-process local deployment profile. The standalone worker
	// command uses the same adapters when the API and worker are split.
	if envBool("RUN_STANDBY_MONITOR", true) {
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
	if envBool("RUN_ADMISSION_CONTROLLER", true) {
		go func() {
			if err := rt.RunAdmissionController(ctx, time.Second); err != nil && err != context.Canceled {
				log.Printf("admission controller stopped: %v", err)
				stop()
			}
		}()
	}
	if envBool("RUN_WORKERS", false) {
		go func() {
			if err := rt.Engine.RunWorkers(ctx, 100*time.Millisecond); err != nil && err != context.Canceled {
				log.Printf("workers stopped: %v", err)
				stop()
			}
		}()
	}
	adminToken := os.Getenv("ADMIN_TOKEN")
	if adminToken == "" {
		return fmt.Errorf("ADMIN_TOKEN is required in distributed mode")
	}
	addr := env("HTTP_ADDR", ":8080")
	limiter, err := shed.New(shed.Config{Total: 2048, Checkout: 512, Reserve: 768, Availability: 512, NonEssential: 256})
	if err != nil {
		return err
	}
	server := &http.Server{Addr: addr, Handler: (&app.HTTPServer{Engine: rt.Engine, AdminToken: adminToken, RequestTimeout: 3 * time.Second, Limiter: limiter, WebhookSecret: cfg.PSPWebhookSecret, WebhookMaxSkew: 5 * time.Minute}).Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("flash-sale API listening on %s item=%s backend=redis+postgres+redpanda", addr, cfg.ItemID)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return ctx.Err()
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
