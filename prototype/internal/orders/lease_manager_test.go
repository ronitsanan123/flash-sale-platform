package orders

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaseManagerRenewsBeforeExpiry(t *testing.T) {
	repo := NewMemoryRepository()
	initial := repo.SeedLease("region-a", "region-a-primary", 7, 120*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	manager := &LeaseManager{Repo: repo, Token: LeaseToken{HomeRegion: "region-a", OwnerID: "region-a-primary", Epoch: 7}, Duration: 120 * time.Millisecond, RenewalLead: 60 * time.Millisecond}
	finished := make(chan error, 1)
	go func() { finished <- manager.Run(ctx) }()
	time.Sleep(280 * time.Millisecond)
	lease, err := repo.Lease(context.Background(), "region-a")
	if err != nil {
		t.Fatal(err)
	}
	if !lease.ExpiresAt.After(initial.ExpiresAt) {
		t.Fatalf("lease was not renewed: before=%v after=%v", initial.ExpiresAt, lease.ExpiresAt)
	}
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("manager exit=%v", err)
	}
}

func TestLeaseManagerSelfFencesAfterTakeover(t *testing.T) {
	repo := NewMemoryRepository()
	repo.SeedLease("region-a", "region-a-primary", 7, 50*time.Millisecond)
	old := LeaseToken{HomeRegion: "region-a", OwnerID: "region-a-primary", Epoch: 7}
	if _, err := repo.TakeoverLease(context.Background(), "region-a", "region-a-standby", true, time.Second); err != nil {
		t.Fatal(err)
	}
	var fenced atomic.Bool
	manager := &LeaseManager{Repo: repo, Token: old, Duration: 100 * time.Millisecond, OnSelfFence: func(error) { fenced.Store(true) }}
	if err := manager.Run(context.Background()); err == nil {
		t.Fatal("stale manager unexpectedly started")
	}
	if !fenced.Load() {
		t.Fatal("self-fence callback not invoked")
	}
}
