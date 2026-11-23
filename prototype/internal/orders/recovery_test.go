package orders

import (
	"context"
	"errors"
	"testing"
	"time"

	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/psp"
)

func TestRegionalRecoveryDiscoversUnknownOrderIDAndVoidsOrphan(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)
	clock := base
	mock := psp.NewMock()
	mock.SetClock(func() time.Time { return clock })
	clock = base.Add(30 * time.Second)
	key := "lost-local-order-001"
	metadata := psp.Metadata{OrderID: key, ItemID: "hot-item", UnitID: "seat-17", UserID: "customer-9", HomeRegion: "region-a", FencingEpoch: 7}
	if _, _, err := mock.Authorize(key, psp.Request{AmountMinor: 2500, Currency: "USD", Metadata: metadata}); err != nil {
		t.Fatal(err)
	}
	clock = base.Add(time.Minute)

	repo := NewMemoryRepository()
	lease := repo.SeedLease("region-a", "region-a-standby-promoted", 8, time.Hour)
	scanner := RecoveryScanner{PSP: mock, Repo: repo, PageSize: 1, Overlap: time.Minute, SettlementGrace: 5 * time.Second}
	result, err := scanner.Run(ctx, LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, base, clock.Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result.DiscoveredIDs != 1 || result.Voided != 1 || result.Preserved != 0 {
		t.Fatalf("unexpected recovery result: %+v", result)
	}
	charge, err := mock.Get(key)
	if err != nil || charge.Status != "VOIDED" {
		t.Fatalf("orphan not voided: charge=%+v err=%v", charge, err)
	}
	recovered, err := repo.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Order.Status != OrderCancelled || recovered.Order.InventoryDisposition != DispositionRelease || recovered.Payment.Status != PaymentVoided {
		t.Fatalf("wrong reconstructed record: %+v", recovered)
	}
	checkpoint, ok, err := repo.LoadLedgerCheckpoint(ctx, "region-a")
	if err != nil || !ok || checkpoint.CompleteThrough.Before(checkpoint.ScanTo) {
		t.Fatalf("checkpoint not durably modeled after complete scan: %+v ok=%v err=%v", checkpoint, ok, err)
	}

	// Repeating an overlapping scan must not increment the aggregate version
	// or create a second external void effect.
	version := recovered.Order.Version
	clock = clock.Add(time.Second)
	result2, err := scanner.Run(ctx, LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, base, clock.Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	again, err := repo.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if again.Order.Version != version {
		t.Fatalf("overlap replay changed order version: before=%d after=%d result=%+v", version, again.Order.Version, result2)
	}
}

func TestRegionalRecoveryDoesNotVoidDurablyConfirmedOrder(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 22, 23, 0, 0, 0, time.UTC)
	clock := base
	mock := psp.NewMock()
	mock.SetClock(func() time.Time { return clock })
	clock = base.Add(30 * time.Second)
	repo := NewMemoryRepository()
	lease := repo.SeedLease("region-a", "region-a-primary", 3, time.Hour)
	token := LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	key := "durable-confirmed-001"
	event := CheckoutEvent{OrderID: key, ItemID: "item", UnitID: "unit-1", UserID: "user", HomeRegion: "region-a", FencingEpoch: 3, AmountMinor: 1000, Currency: "USD"}
	if _, err := repo.Begin(ctx, token, event); err != nil {
		t.Fatal(err)
	}
	charge, _, err := mock.Authorize(key, psp.Request{AmountMinor: 1000, Currency: "USD", Metadata: psp.Metadata{OrderID: key, ItemID: "item", UnitID: "unit-1", UserID: "user", HomeRegion: "region-a", FencingEpoch: 3}})
	if err != nil {
		t.Fatal(err)
	}
	clock = base.Add(time.Minute)
	if _, err := repo.MarkAuthorized(ctx, token, key, charge.ChargeID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Confirm(ctx, token, key); err != nil {
		t.Fatal(err)
	}

	scanner := RecoveryScanner{PSP: mock, Repo: repo, PageSize: 10, SettlementGrace: time.Second}
	result, err := scanner.Run(ctx, token, base, clock.Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result.Preserved != 1 || result.Voided != 0 {
		t.Fatalf("confirmed record was not preserved: %+v", result)
	}
	current, err := mock.Get(key)
	if err != nil || current.Status != "AUTHORIZED" {
		t.Fatalf("confirmed PSP authorization was incorrectly voided: %+v err=%v", current, err)
	}
}

func TestRegionalRecoveryFailsClosedWhenLedgerWatermarkLags(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 22, 22, 0, 0, 0, time.UTC)
	clock := base.Add(time.Minute)
	mock := psp.NewMock()
	mock.SetClock(func() time.Time { return clock })
	mock.SetFaults(psp.Faults{LedgerIndexDelayMS: 60_000})
	key := "unknown-index-delay"
	_, _, err := mock.Authorize(key, psp.Request{AmountMinor: 500, Currency: "USD", Metadata: psp.Metadata{OrderID: key, ItemID: "item", UnitID: "u1", UserID: "u1", HomeRegion: "region-a", FencingEpoch: 1}})
	if err != nil {
		t.Fatal(err)
	}
	repo := NewMemoryRepository()
	lease := repo.SeedLease("region-a", "region-a-primary", 2, time.Hour)
	scanner := RecoveryScanner{PSP: mock, Repo: repo, SettlementGrace: time.Second}
	_, err = scanner.Run(ctx, LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, base, clock.Add(-10*time.Second))
	if !errors.Is(err, ErrLedgerIncomplete) {
		t.Fatalf("expected incomplete ledger error, got %v", err)
	}
	if _, ok, _ := repo.LoadLedgerCheckpoint(ctx, "region-a"); ok {
		t.Fatal("checkpoint advanced despite incomplete ledger watermark")
	}
	charge, _ := mock.Get(key)
	if charge.Status != "AUTHORIZED" {
		t.Fatalf("scanner should fail closed without voiding partial page: %s", charge.Status)
	}
}

func TestRegionalRecoveryRepairsRedisForVoidedOrphan(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 23, 2, 0, 0, 0, time.UTC)
	clock := base.Add(30 * time.Second)
	mock := psp.NewMock()
	mock.SetClock(func() time.Time { return clock })
	key := "redis-orphan-1"
	metadata := psp.Metadata{OrderID: key, ItemID: "hot-item", UnitID: "seat-9", UserID: "buyer-9", HomeRegion: "region-z", FencingEpoch: 3}
	if _, _, err := mock.Authorize(key, psp.Request{AmountMinor: 1200, Currency: "USD", Metadata: metadata}); err != nil {
		t.Fatal(err)
	}
	clock = base.Add(2 * time.Minute)

	repo := NewMemoryRepository()
	lease := repo.SeedLease("region-z", "region-z-promoted", 4, time.Hour)
	inv := inventory.NewMemoryStore(time.Minute, time.Minute)
	if err := inv.Seed(ctx, "hot-item", []string{"seat-9"}); err != nil {
		t.Fatal(err)
	}
	if _, err := inv.Reserve(ctx, "hot-item", key, "buyer-9", "seat-9"); err != nil {
		t.Fatal(err)
	}
	if _, err := inv.LockForPayment(ctx, "hot-item", key); err != nil {
		t.Fatal(err)
	}

	scanner := RecoveryScanner{PSP: mock, Repo: repo, Inventory: inv, PageSize: 10, SettlementGrace: 5 * time.Second}
	result, err := scanner.Run(ctx, LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, base, clock.Add(-5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result.Voided != 1 || result.DiscoveredIDs != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	available, held, paying, confirmed, forfeited, total, err := inv.Availability(ctx, "hot-item")
	if err != nil {
		t.Fatal(err)
	}
	if available != 1 || held != 0 || paying != 0 || confirmed != 0 || forfeited != 0 || total != 1 {
		t.Fatalf("Redis inventory was not safely released: a=%d h=%d p=%d c=%d f=%d total=%d", available, held, paying, confirmed, forfeited, total)
	}
}

func TestRegionalRecoveryRepairsRedisForDurableConfirmation(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 23, 3, 0, 0, 0, time.UTC)
	clock := base.Add(30 * time.Second)
	mock := psp.NewMock()
	mock.SetClock(func() time.Time { return clock })
	repo := NewMemoryRepository()
	lease := repo.SeedLease("region-y", "region-y-primary", 6, time.Hour)
	token := LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	key := "redis-confirmed-1"
	inv := inventory.NewMemoryStore(time.Minute, time.Minute)
	if err := inv.Seed(ctx, "hot-item", []string{"seat-10"}); err != nil {
		t.Fatal(err)
	}
	if _, err := inv.Reserve(ctx, "hot-item", key, "buyer-10", "seat-10"); err != nil {
		t.Fatal(err)
	}
	if _, err := inv.LockForPayment(ctx, "hot-item", key); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Begin(ctx, token, CheckoutEvent{OrderID: key, ItemID: "hot-item", UnitID: "seat-10", UserID: "buyer-10", HomeRegion: "region-y", FencingEpoch: 6, AmountMinor: 1200}); err != nil {
		t.Fatal(err)
	}
	charge, _, err := mock.Authorize(key, psp.Request{AmountMinor: 1200, Currency: "USD", Metadata: psp.Metadata{OrderID: key, ItemID: "hot-item", UnitID: "seat-10", UserID: "buyer-10", HomeRegion: "region-y", FencingEpoch: 6}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.MarkAuthorized(ctx, token, key, charge.ChargeID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Confirm(ctx, token, key); err != nil {
		t.Fatal(err)
	}
	clock = base.Add(2 * time.Minute)
	scanner := RecoveryScanner{PSP: mock, Repo: repo, Inventory: inv, SettlementGrace: 5 * time.Second}
	result, err := scanner.Run(ctx, token, base, clock.Add(-5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result.Preserved != 1 || result.Voided != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	available, held, paying, confirmed, forfeited, total, err := inv.Availability(ctx, "hot-item")
	if err != nil {
		t.Fatal(err)
	}
	if available != 0 || held != 0 || paying != 0 || confirmed != 1 || forfeited != 0 || total != 1 {
		t.Fatalf("Redis confirmed ownership not repaired: a=%d h=%d p=%d c=%d f=%d total=%d", available, held, paying, confirmed, forfeited, total)
	}
}

func TestRegionalRecoveryMissingRedisHoldFailsBeforeCheckpoint(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 23, 4, 0, 0, 0, time.UTC)
	clock := base.Add(30 * time.Second)
	mock := psp.NewMock()
	mock.SetClock(func() time.Time { return clock })
	key := "missing-redis-hold"
	if _, _, err := mock.Authorize(key, psp.Request{AmountMinor: 1000, Currency: "USD", Metadata: psp.Metadata{OrderID: key, ItemID: "i", UnitID: "u", UserID: "x", HomeRegion: "r", FencingEpoch: 2}}); err != nil {
		t.Fatal(err)
	}
	clock = base.Add(2 * time.Minute)
	repo := NewMemoryRepository()
	lease := repo.SeedLease("r", "r-promoted", 3, time.Hour)
	inv := inventory.NewMemoryStore(time.Minute, time.Minute)
	_ = inv.Seed(ctx, "i", []string{"other-unit"})
	scanner := RecoveryScanner{PSP: mock, Repo: repo, Inventory: inv, SettlementGrace: 5 * time.Second}
	_, err := scanner.Run(ctx, LeaseToken{HomeRegion: lease.HomeRegion, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, base, clock.Add(-5*time.Second))
	if !errors.Is(err, inventory.ErrHoldNotFound) {
		t.Fatalf("expected missing Redis hold fail-closed, got %v", err)
	}
	if _, ok, _ := repo.LoadLedgerCheckpoint(ctx, "r"); ok {
		t.Fatal("checkpoint advanced despite incomplete Redis repair")
	}
}
