package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"flashsale-platform/prototype/internal/orders"
	"flashsale-platform/prototype/internal/psp"
)

// This integration-slice test exercises the PSP discovery/recovery contract
// with the fault-injectable PSP and lease-aware memory repository. The
// PostgreSQL/Redis adapters are exercised by their dedicated Compose tests.
func TestRegionalRecoveryUnknownOrderIDIsDiscoveredAndVoided(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)
	clock := base.Add(30 * time.Second)
	mock := psp.NewMock()
	mock.SetClock(func() time.Time { return clock })
	key := "recovery-order-unknown-to-standby"
	metadata := psp.Metadata{OrderID: key, ItemID: "hot-item", UnitID: "seat-17", UserID: "user-8", HomeRegion: "home-a", FencingEpoch: 41}
	if _, _, err := mock.Authorize(key, psp.Request{AmountMinor: 1900, Currency: "USD", Metadata: metadata}); err != nil {
		t.Fatal(err)
	}
	clock = base.Add(time.Minute)

	repo := orders.NewMemoryRepository()
	lease := repo.SeedLease("home-a", "home-a-standby-primary", 42, time.Hour)
	scanner := orders.RecoveryScanner{PSP: mock, Repo: repo, PageSize: 1, Overlap: 30 * time.Second, SettlementGrace: 5 * time.Second}
	result, err := scanner.Run(ctx, orders.LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, base, clock.Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result.DiscoveredIDs != 1 || result.Voided != 1 || result.Preserved != 0 {
		t.Fatalf("unexpected recovery result: %+v", result)
	}
	charge, err := mock.Get(key)
	if err != nil || charge.Status != "VOIDED" {
		t.Fatalf("orphan authorization was not voided: %+v err=%v", charge, err)
	}
	recovered, err := repo.Get(ctx, key)
	if err != nil || recovered.Order.Status != orders.OrderCancelled || recovered.Payment.Status != orders.PaymentVoided {
		t.Fatalf("missing reconstructed recovery record: %+v err=%v", recovered, err)
	}
}

func TestRegionalRecoveryIncompleteWatermarkKeepsGateClosed(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 23, 1, 0, 0, 0, time.UTC)
	clock := base.Add(time.Minute)
	mock := psp.NewMock()
	mock.SetClock(func() time.Time { return clock })
	mock.SetFaults(psp.Faults{LedgerIndexDelayMS: 60_000})
	key := "recovery-ledger-index-delayed"
	if _, _, err := mock.Authorize(key, psp.Request{AmountMinor: 1900, Currency: "USD", Metadata: psp.Metadata{OrderID: key, ItemID: "item", UnitID: "unit", UserID: "user", HomeRegion: "home-a", FencingEpoch: 4}}); err != nil {
		t.Fatal(err)
	}
	repo := orders.NewMemoryRepository()
	lease := repo.SeedLease("home-a", "home-a-primary", 5, time.Hour)
	scanner := orders.RecoveryScanner{PSP: mock, Repo: repo, SettlementGrace: time.Second}
	_, err := scanner.Run(ctx, orders.LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, base, clock.Add(-10*time.Second))
	if !errors.Is(err, orders.ErrLedgerIncomplete) {
		t.Fatalf("expected incomplete watermark, got %v", err)
	}
	if _, ok, _ := repo.LoadLedgerCheckpoint(ctx, "home-a"); ok {
		t.Fatal("recovery checkpoint advanced on incomplete ledger")
	}
	charge, err := mock.Get(key)
	if err != nil || charge.Status != "AUTHORIZED" {
		t.Fatalf("scanner mutated an ambiguous/incomplete PSP state: %+v err=%v", charge, err)
	}
}
