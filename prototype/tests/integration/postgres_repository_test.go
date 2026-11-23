package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"flashsale-platform/prototype/internal/orders"
	"flashsale-platform/prototype/internal/pgwire"
)

func postgresRepo(t *testing.T) (*orders.PostgresRepository, *pgwire.Pool) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not configured; run inside Compose integration service")
	}
	pool, err := pgwire.NewPool(dsn, 4)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	repo, err := orders.NewPostgresRepository(pool, false)
	if err != nil {
		_ = pool.Close()
		t.Fatal(err)
	}
	return repo, pool
}

func TestPostgresRepositoryIntentReplayUniqueGuardAndFencing(t *testing.T) {
	repo, pool := postgresRepo(t)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	home := "pg-it-" + suffix
	owner := home + "-primary"
	lease := orders.Lease{HomeRegion: home, OwnerID: owner, Epoch: 3, ExpiresAt: time.Now().Add(10 * time.Minute)}
	if err := repo.CreateLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	otherHome := home + "-other"
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM payments WHERE order_id LIKE $1`, "pg-order-"+suffix+"-%")
		_, _ = pool.Exec(context.Background(), `DELETE FROM orders WHERE order_id LIKE $1`, "pg-order-"+suffix+"-%")
		_, _ = pool.Exec(context.Background(), `DELETE FROM region_leases WHERE home_region=$1`, home)
		_, _ = pool.Exec(context.Background(), `DELETE FROM region_leases WHERE home_region=$1`, otherHome)
	}()
	otherOwner := otherHome + "-primary"
	otherLease := orders.Lease{HomeRegion: otherHome, OwnerID: otherOwner, Epoch: 9, ExpiresAt: time.Now().Add(10 * time.Minute)}
	if err := repo.CreateLease(ctx, otherLease); err != nil {
		t.Fatal(err)
	}
	token := orders.LeaseToken{HomeRegion: home, OwnerID: owner, Epoch: 3}
	base := orders.CheckoutEvent{OrderID: "pg-order-" + suffix + "-owner", ItemID: "pg-item-" + suffix, UnitID: "seat-1", UserID: "user-1", HomeRegion: home, FencingEpoch: 3, AmountMinor: 1200, Currency: "USD"}

	first, err := repo.Begin(ctx, token, base)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.Order.Status != orders.OrderPendingPayment || first.Payment.Status != orders.PaymentIntent {
		t.Fatalf("unexpected initial aggregate: %+v", first)
	}
	replay, err := repo.Begin(ctx, token, base)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Created || replay.Order.ID != base.OrderID || replay.Order.Version != first.Order.Version {
		t.Fatalf("same-identity replay was not idempotent: first=%+v replay=%+v", first, replay)
	}

	loser := base
	loser.OrderID = "pg-order-" + suffix + "-loser"
	loser.UserID = "user-2"
	conflict, err := repo.Begin(ctx, token, loser)
	if err != nil {
		t.Fatal(err)
	}
	if !conflict.UniqueConflict || conflict.Order.Status != orders.OrderCancelled || conflict.Order.InventoryDisposition != orders.DispositionForfeit || conflict.Payment.Status != orders.PaymentNotAttempted {
		t.Fatalf("unique-index loser must be cancelled/forfeited and NOT_ATTEMPTED: %+v", conflict)
	}
	loserReplay, err := repo.Begin(ctx, token, loser)
	if err != nil {
		t.Fatal(err)
	}
	if loserReplay.Order.Status != orders.OrderCancelled || !loserReplay.UniqueConflict && loserReplay.Created {
		t.Fatalf("cancelled loser replay changed state: %+v", loserReplay)
	}

	changed := base
	changed.HomeRegion = otherHome
	changed.FencingEpoch = otherLease.Epoch
	otherToken := orders.LeaseToken{HomeRegion: otherHome, OwnerID: otherOwner, Epoch: otherLease.Epoch}
	if _, err := repo.Begin(ctx, otherToken, changed); !errors.Is(err, orders.ErrIdentityMismatch) {
		t.Fatalf("home-region immutable replay should fail identity validation after a valid lease check, got %v", err)
	}

	if _, err := repo.TakeoverLease(ctx, home, home+"-standby", true, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Begin(ctx, token, orders.CheckoutEvent{OrderID: "pg-order-" + suffix + "-stale", ItemID: base.ItemID, UnitID: "seat-2", UserID: "user-3", HomeRegion: home, FencingEpoch: 3, AmountMinor: 1200}); !errors.Is(err, orders.ErrFenced) && !errors.Is(err, orders.ErrLeaseLost) {
		t.Fatalf("old writer should be fenced after takeover, got %v", err)
	}
}
