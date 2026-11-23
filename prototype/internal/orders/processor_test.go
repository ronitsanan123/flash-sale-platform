package orders

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/psp"
)

func setupProcessor(t *testing.T, gate *StaticStandbyGate, faults psp.Faults) (*Processor, *MemoryRepository, *inventory.MemoryStore, *psp.Mock, *MemoryStatusStore, context.Context) {
	t.Helper()
	ctx := context.Background()
	repo := NewMemoryRepository()
	repo.SeedLease("r1", "r1-primary", 1, time.Minute)
	inv := inventory.NewMemoryStore(time.Minute, time.Minute)
	if err := inv.Seed(ctx, "item", []string{"unit-1", "unit-2"}); err != nil {
		t.Fatal(err)
	}
	srvPSP := psp.NewMock()
	srvPSP.SetFaults(faults)
	server := httptest.NewServer(srvPSP)
	t.Cleanup(server.Close)
	client := &psp.Client{BaseURL: server.URL}
	status := NewMemoryStatusStore()
	proc := &Processor{Repo: repo, Inventory: inv, PSP: client, Status: status, Gate: gate, Lease: LeaseToken{HomeRegion: "r1", OwnerID: "r1-primary", Epoch: 1}, HomeRegion: "r1", AmountMinor: 1000, Currency: "USD"}
	return proc, repo, inv, srvPSP, status, ctx
}
func checkoutHold(t *testing.T, ctx context.Context, inv *inventory.MemoryStore, id, unit string) {
	t.Helper()
	if _, err := inv.Reserve(ctx, "item", id, "user", unit); err != nil {
		t.Fatal(err)
	}
	if _, err := inv.LockForPayment(ctx, "item", id); err != nil {
		t.Fatal(err)
	}
}
func TestProcessorLostResponseIsSingleAuthorizationAndReplaySafe(t *testing.T) {
	gate := &StaticStandbyGate{Available: true}
	proc, repo, inv, mock, status, ctx := setupProcessor(t, gate, psp.Faults{LostResponseRate: 1})
	checkoutHold(t, ctx, inv, "order-1", "unit-1")
	ev := inventory.Event{Type: "CheckoutRequested", HoldID: "order-1", ItemID: "item"}
	if err := proc.Process(ctx, ev); err != nil {
		t.Fatal(err)
	}
	a, err := repo.Get(ctx, "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if a.Order.Status != OrderConfirmed || a.Payment.Status != PaymentAuthorized {
		t.Fatalf("aggregate %+v", a)
	}
	if err := proc.Process(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if charges, events := mock.Stats(); charges != 1 || events != 1 {
		t.Fatalf("PSP charges/events=%d/%d, want 1/1", charges, events)
	}
	if err := proc.CapturePending(ctx, 10); err != nil {
		t.Fatal(err)
	}
	a, _ = repo.Get(ctx, "order-1")
	if a.Payment.Status != PaymentCaptured {
		t.Fatalf("payment=%s", a.Payment.Status)
	}
	p, ok, _ := status.Get(ctx, "order-1")
	if !ok || p.PaymentStatus != PaymentCaptured {
		t.Fatalf("projection %+v", p)
	}
}
func TestProcessorStandbyLossKeepsPAYINGAndDoesNotConfirm(t *testing.T) {
	gate := &StaticStandbyGate{Available: false}
	proc, repo, inv, _, status, ctx := setupProcessor(t, gate, psp.Faults{})
	checkoutHold(t, ctx, inv, "order-2", "unit-1")
	ev := inventory.Event{Type: "CheckoutRequested", HoldID: "order-2", ItemID: "item"}
	if err := proc.Process(ctx, ev); !errors.Is(err, ErrStandbyUnavailable) {
		t.Fatalf("got %v", err)
	}
	a, err := repo.Get(ctx, "order-2")
	if err != nil {
		t.Fatal(err)
	}
	if a.Order.Status != OrderPendingPayment || a.Payment.Status != PaymentAuthorized {
		t.Fatalf("aggregate %+v", a)
	}
	h, found, err := inv.Get(ctx, "item", "order-2")
	if err != nil || !found || h.State != inventory.HoldPaying {
		t.Fatalf("hold=%+v found=%v err=%v", h, found, err)
	}
	if projected, ok, _ := status.Get(ctx, "order-2"); !ok || projected.OrderStatus != OrderPendingPayment || projected.PaymentStatus != PaymentAuthorized {
		t.Fatalf("expected non-confirmed authorized projection, got %+v present=%v", projected, ok)
	}
}
func TestRepositoryUniqueGuardLoserIsCancelledNotAttempted(t *testing.T) {
	ctx := context.Background()
	r := NewMemoryRepository()
	r.SeedLease("r", "owner", 1, time.Minute)
	lease := LeaseToken{HomeRegion: "r", OwnerID: "owner", Epoch: 1}
	first, err := r.Begin(ctx, lease, CheckoutEvent{OrderID: "o1", ItemID: "i", UnitID: "u", UserID: "a", HomeRegion: "r", FencingEpoch: 1, AmountMinor: 1})
	if err != nil || first.Order.Status != OrderPendingPayment {
		t.Fatalf("first %+v %v", first, err)
	}
	second, err := r.Begin(ctx, lease, CheckoutEvent{OrderID: "o2", ItemID: "i", UnitID: "u", UserID: "b", HomeRegion: "r", FencingEpoch: 1, AmountMinor: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !second.UniqueConflict || second.Order.Status != OrderCancelled || second.Payment.Status != PaymentNotAttempted || second.Order.InventoryDisposition != DispositionForfeit {
		t.Fatalf("loser %+v", second)
	}
}
func TestLeaseRenewalAndStaleEpochFencing(t *testing.T) {
	ctx := context.Background()
	r := NewMemoryRepository()
	r.SeedLease("r", "r-primary", 7, time.Minute)
	old := LeaseToken{HomeRegion: "r", OwnerID: "r-primary", Epoch: 7}
	if _, err := r.RenewLease(ctx, old, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := r.TakeoverLease(ctx, "r", "r-secondary", false, time.Minute); err == nil {
		t.Fatal("takeover without old-primary fencing should fail")
	}
	if _, err := r.TakeoverLease(ctx, "r", "r-secondary", true, time.Minute); err != nil {
		t.Fatal(err)
	}
	_, err := r.Begin(ctx, old, CheckoutEvent{OrderID: "o", ItemID: "i", UnitID: "u", UserID: "x", HomeRegion: "r", FencingEpoch: 7, AmountMinor: 1})
	if !errors.Is(err, ErrLeaseLost) && !errors.Is(err, ErrFenced) {
		t.Fatalf("stale writer got %v", err)
	}
}
func TestProjectionRejectsStaleVersion(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStatusStore()
	now := time.Now()
	a := Aggregate{Order: Order{ID: "o", Status: OrderConfirmed, Version: 2, UpdatedAt: now}, Payment: Payment{OrderID: "o", Status: PaymentAuthorized}}
	if err := s.Project(ctx, a); err != nil {
		t.Fatal(err)
	}
	old := a
	old.Order.Version = 1
	old.Order.Status = OrderPendingPayment
	if err := s.Project(ctx, old); err == nil {
		t.Fatal("stale projection should be rejected")
	}
}

type fakeInventory struct {
	hold      inventory.Hold
	forfeited int
	released  int
	confirmed int
}

func (f *fakeInventory) Seed(context.Context, string, []string) error { return nil }
func (f *fakeInventory) Reserve(context.Context, string, string, string, string) (inventory.Hold, error) {
	return inventory.Hold{}, nil
}
func (f *fakeInventory) LockForPayment(context.Context, string, string) (inventory.Hold, error) {
	return f.hold, nil
}
func (f *fakeInventory) Confirm(context.Context, string, string) error { f.confirmed++; return nil }
func (f *fakeInventory) Release(context.Context, string, string) error { f.released++; return nil }
func (f *fakeInventory) ReleaseAfterPayment(context.Context, string, string, inventory.ForfeitProof) error {
	f.released++
	return nil
}
func (f *fakeInventory) Forfeit(context.Context, string, string, inventory.ForfeitProof) error {
	f.forfeited++
	return nil
}
func (f *fakeInventory) Reap(context.Context, string, int, int) (int, int, error) { return 0, 0, nil }
func (f *fakeInventory) Get(context.Context, string, string) (inventory.Hold, bool, error) {
	return f.hold, true, nil
}
func (f *fakeInventory) Availability(context.Context, string) (int, int, int, int, int, int, error) {
	return 0, 0, 0, 0, 0, 0, nil
}
func (f *fakeInventory) Events(context.Context, string, int, int) ([]inventory.Event, error) {
	return nil, nil
}
func TestCancelledUniqueGuardReplayNeverCallsPSP(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepository()
	repo.SeedLease("r", "owner", 1, time.Minute)
	lease := LeaseToken{HomeRegion: "r", OwnerID: "owner", Epoch: 1}
	_, _ = repo.Begin(ctx, lease, CheckoutEvent{OrderID: "owner-order", ItemID: "i", UnitID: "u", UserID: "owner", HomeRegion: "r", FencingEpoch: 1, AmountMinor: 1})
	loser, _ := repo.Begin(ctx, lease, CheckoutEvent{OrderID: "loser-order", ItemID: "i", UnitID: "u", UserID: "loser", HomeRegion: "r", FencingEpoch: 1, AmountMinor: 1})
	_ = loser
	mock := psp.NewMock()
	server := httptest.NewServer(mock)
	defer server.Close()
	fake := &fakeInventory{hold: inventory.Hold{ID: "loser-order", Item: "i", Unit: "u", User: "loser", State: inventory.HoldPaying}}
	proc := &Processor{Repo: repo, Inventory: fake, PSP: &psp.Client{BaseURL: server.URL}, Status: NewMemoryStatusStore(), Gate: &StaticStandbyGate{Available: true}, Lease: lease, HomeRegion: "r"}
	ev := inventory.Event{Type: "CheckoutRequested", HoldID: "loser-order", ItemID: "i"}
	if err := proc.Process(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if charges, events := mock.Stats(); charges != 0 || events != 0 {
		t.Fatalf("PSP called for cancelled unique loser: %d/%d", charges, events)
	}
	if fake.forfeited != 1 {
		t.Fatalf("forfeit calls=%d", fake.forfeited)
	}
}

func TestLateAuthorizationWebhookVoidsCancelledUniqueGuardLoser(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepository()
	repo.SeedLease("r", "r-primary", 9, time.Minute)
	lease := LeaseToken{HomeRegion: "r", OwnerID: "r-primary", Epoch: 9}
	if _, err := repo.Begin(ctx, lease, CheckoutEvent{OrderID: "owner", ItemID: "item", UnitID: "seat-1", UserID: "owner-user", HomeRegion: "r", FencingEpoch: 9, AmountMinor: 1000}); err != nil {
		t.Fatal(err)
	}
	loser, err := repo.Begin(ctx, lease, CheckoutEvent{OrderID: "loser", ItemID: "item", UnitID: "seat-1", UserID: "loser-user", HomeRegion: "r", FencingEpoch: 9, AmountMinor: 1000})
	if err != nil || !loser.UniqueConflict || loser.Order.InventoryDisposition != DispositionForfeit {
		t.Fatalf("expected unique guard loser: %+v err=%v", loser, err)
	}
	mock := psp.NewMock()
	metadata := psp.Metadata{OrderID: "loser", ItemID: "item", UnitID: "seat-1", UserID: "loser-user", HomeRegion: "r", FencingEpoch: 9}
	charge, _, err := mock.Authorize("loser", psp.Request{AmountMinor: 1000, Currency: "USD", Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeInventory{hold: inventory.Hold{ID: "loser", Item: "item", Unit: "seat-1", User: "loser-user", State: inventory.HoldPaying}}
	proc := &Processor{Repo: repo, Inventory: fake, PSP: psp.DirectClient{Mock: mock}, Status: NewMemoryStatusStore(), Gate: &StaticStandbyGate{Available: true}, Lease: lease, HomeRegion: "r"}
	if err := proc.HandleWebhook(ctx, charge); err != nil {
		t.Fatal(err)
	}
	current, err := mock.Get("loser")
	if err != nil || current.Status != "VOIDED" {
		t.Fatalf("late auth was not voided: %+v err=%v", current, err)
	}
	a, err := repo.Get(ctx, "loser")
	if err != nil {
		t.Fatal(err)
	}
	if a.Order.Status != OrderCancelled || a.Order.InventoryDisposition != DispositionForfeit || a.Payment.Status != PaymentVoided {
		t.Fatalf("cancelled loser changed incorrectly: %+v", a)
	}
	if fake.forfeited != 1 || fake.released != 0 {
		t.Fatalf("inventory disposition: forfeited=%d released=%d", fake.forfeited, fake.released)
	}
}

func TestWebhookRejectsWrongFencingEpoch(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepository()
	repo.SeedLease("r", "r-primary", 4, time.Minute)
	lease := LeaseToken{HomeRegion: "r", OwnerID: "r-primary", Epoch: 4}
	if _, err := repo.Begin(ctx, lease, CheckoutEvent{OrderID: "o", ItemID: "i", UnitID: "u", UserID: "x", HomeRegion: "r", FencingEpoch: 4, AmountMinor: 1}); err != nil {
		t.Fatal(err)
	}
	mock := psp.NewMock()
	charge, _, err := mock.Authorize("o", psp.Request{AmountMinor: 1, Currency: "USD", Metadata: psp.Metadata{OrderID: "o", ItemID: "i", UnitID: "u", UserID: "x", HomeRegion: "r", FencingEpoch: 3}})
	if err != nil {
		t.Fatal(err)
	}
	proc := &Processor{Repo: repo, Inventory: &fakeInventory{}, PSP: psp.DirectClient{Mock: mock}, Status: NewMemoryStatusStore(), Gate: &StaticStandbyGate{Available: true}, Lease: lease, HomeRegion: "r"}
	if err := proc.HandleWebhook(ctx, charge); !errors.Is(err, ErrFenced) {
		t.Fatalf("wrong webhook epoch should be rejected, got %v", err)
	}
}
