package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestForfeitProofValidation(t *testing.T) {
	tests := []struct {
		name  string
		state HoldState
		proof ForfeitProof
		want  bool
	}{
		{name: "held with no PSP attempt", state: HoldHeld, proof: ForfeitProofNotAttempted, want: true},
		{name: "held with decline is inconsistent", state: HoldHeld, proof: ForfeitProofPSPDeclined, want: false},
		{name: "paying unique guard loser before PSP", state: HoldPaying, proof: ForfeitProofNotAttempted, want: true},
		{name: "paying definitive decline", state: HoldPaying, proof: ForfeitProofPSPDeclined, want: true},
		{name: "paying definitive void", state: HoldPaying, proof: ForfeitProofPSPVoided, want: true},
		{name: "paying live authorization", state: HoldPaying, proof: ForfeitProof("AUTHORIZED"), want: false},
		{name: "paying unknown authorization", state: HoldPaying, proof: ForfeitProof("UNKNOWN"), want: false},
		{name: "paying missing proof", state: HoldPaying, proof: "", want: false},
		{name: "confirmed cannot be forfeited", state: HoldConfirmed, proof: ForfeitProofNotAttempted, want: false},
		{name: "released cannot be forfeited", state: HoldReleased, proof: ForfeitProofNotAttempted, want: false},
		{name: "forfeited replay is an idempotent no-op", state: HoldForfeited, proof: ForfeitProof("UNKNOWN"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateForfeitProof(tt.state, tt.proof)
			if tt.want && err != nil {
				t.Fatalf("ValidateForfeitProof() error = %v, want nil", err)
			}
			if !tt.want && err == nil {
				t.Fatal("ValidateForfeitProof() = nil, want unsafe-forfeit error")
			}
			if !tt.want && err != ErrUnsafeForfeit {
				t.Fatalf("ValidateForfeitProof() error = %v, want ErrUnsafeForfeit", err)
			}
		})
	}
}

func TestForfeit_PayingHoldRequiresVoidOrDecline(t *testing.T) {
	unsafe := []ForfeitProof{
		ForfeitProof("AUTHORIZED"),
		ForfeitProof("UNKNOWN"),
		ForfeitProof("INTENT"),
		ForfeitProof(""),
	}
	for _, proof := range unsafe {
		if err := ValidateForfeitProof(HoldPaying, proof); err != ErrUnsafeForfeit {
			t.Errorf("PAYING with proof %q: got %v, want ErrUnsafeForfeit", proof, err)
		}
	}
	for _, proof := range []ForfeitProof{
		// NOT_ATTEMPTED is the narrow unique-index-loser exception: the worker
		// persisted CANCELLED/NOT_ATTEMPTED and short-circuited before PSP.
		ForfeitProofNotAttempted,
		ForfeitProofPSPDeclined,
		ForfeitProofPSPVoided,
	} {
		if err := ValidateForfeitProof(HoldPaying, proof); err != nil {
			t.Errorf("PAYING with safe proof %q: got %v, want nil", proof, err)
		}
	}
}

func TestForfeit_HELDRequiresNotAttempted(t *testing.T) {
	if err := ValidateForfeitProof(HoldHeld, ForfeitProofNotAttempted); err != nil {
		t.Fatalf("HELD with NOT_ATTEMPTED: %v", err)
	}
	for _, proof := range []ForfeitProof{ForfeitProofPSPDeclined, ForfeitProofPSPVoided, ForfeitProof("UNKNOWN")} {
		if err := ValidateForfeitProof(HoldHeld, proof); err != ErrUnsafeForfeit {
			t.Errorf("HELD with proof %q: got %v, want ErrUnsafeForfeit", proof, err)
		}
	}
}

func TestMemoryStore_NoOversellUnderContention(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(time.Minute, time.Minute)
	units := make([]string, 100)
	for i := range units {
		units[i] = fmt.Sprintf("unit-%03d", i)
	}
	if err := store.Seed(ctx, "concert", units); err != nil {
		t.Fatal(err)
	}
	const racers = 5000
	var wg sync.WaitGroup
	var mu sync.Mutex
	holds := map[string]bool{}
	success := 0
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, err := store.Reserve(ctx, "concert", fmt.Sprintf("hold-%d", i), fmt.Sprintf("user-%d", i), "")
			if err == nil {
				mu.Lock()
				success++
				if holds[h.Unit] {
					t.Errorf("duplicate unit %s", h.Unit)
				}
				holds[h.Unit] = true
				mu.Unlock()
			} else if !errors.Is(err, ErrSoldOut) {
				t.Errorf("unexpected reserve error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if success != 100 {
		t.Fatalf("successful reservations = %d, want 100", success)
	}
	if len(holds) != 100 {
		t.Fatalf("unique units = %d, want 100", len(holds))
	}
}

func TestMemoryStore_TerminalHoldIDsCannotBeReused(t *testing.T) {
	ctx := context.Background()
	for _, terminal := range []string{"release", "forfeit", "confirm"} {
		t.Run(terminal, func(t *testing.T) {
			store := NewMemoryStore(time.Minute, time.Minute)
			base := time.Now()
			store.now = func() time.Time { return base }
			if err := store.Seed(ctx, "item", []string{"u1", "u2"}); err != nil {
				t.Fatal(err)
			}
			h, err := store.Reserve(ctx, "item", "h1", "user", "u1")
			if err != nil {
				t.Fatal(err)
			}
			switch terminal {
			case "release":
				base = base.Add(2 * time.Minute)
				if err := store.Release(ctx, "item", "h1"); err != nil {
					t.Fatal(err)
				}
				_, err = store.Reserve(ctx, "item", "h1", "user", "u1")
				if !errors.Is(err, ErrHoldReleased) {
					t.Fatalf("got %v, want ErrHoldReleased", err)
				}
			case "forfeit":
				if err := store.Forfeit(ctx, "item", "h1", ForfeitProofNotAttempted); err != nil {
					t.Fatal(err)
				}
				_, err = store.Reserve(ctx, "item", "h1", "user", "u1")
				if !errors.Is(err, ErrHoldForfeited) {
					t.Fatalf("got %v, want ErrHoldForfeited", err)
				}
			case "confirm":
				if _, err := store.LockForPayment(ctx, "item", "h1"); err != nil {
					t.Fatal(err)
				}
				if err := store.Confirm(ctx, "item", "h1"); err != nil {
					t.Fatal(err)
				}
				replayed, err := store.Reserve(ctx, "item", "h1", "user", "u1")
				if err != nil {
					t.Fatal(err)
				}
				if replayed.State != HoldConfirmed {
					t.Fatalf("got state %s", replayed.State)
				}
			}
			_ = h
		})
	}
}

func TestMemoryStore_ForfeitDoesNotReturnUnit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(time.Minute, time.Minute)
	if err := store.Seed(ctx, "i", []string{"u1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reserve(ctx, "i", "h1", "u1", ""); err != nil {
		t.Fatal(err)
	}
	if err := store.Forfeit(ctx, "i", "h1", ForfeitProofNotAttempted); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reserve(ctx, "i", "h2", "u2", ""); !errors.Is(err, ErrSoldOut) {
		t.Fatalf("got %v, want sold out", err)
	}
	if err := store.Forfeit(ctx, "i", "h1", ForfeitProof("UNKNOWN")); err != nil {
		t.Fatalf("forfeit replay should be idempotent: %v", err)
	}
}

func TestMemoryStore_PayingHoldRequiresDefinitiveProof(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(time.Minute, time.Minute)
	if err := store.Seed(ctx, "i", []string{"u1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reserve(ctx, "i", "h1", "u1", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LockForPayment(ctx, "i", "h1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Forfeit(ctx, "i", "h1", ForfeitProof("AUTHORIZED")); !errors.Is(err, ErrUnsafeForfeit) {
		t.Fatalf("got %v, want unsafe", err)
	}
	if err := store.Forfeit(ctx, "i", "h1", ForfeitProof("UNKNOWN")); !errors.Is(err, ErrUnsafeForfeit) {
		t.Fatalf("got %v, want unsafe", err)
	}
	if err := store.Forfeit(ctx, "i", "h1", ForfeitProofPSPVoided); err != nil {
		t.Fatal(err)
	}
}

func TestForfeitDuplicateGuardLoserPreservesConfirmedUnitOwner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(time.Minute, time.Minute)
	if err := store.Seed(ctx, "item", []string{"unit-1"}); err != nil {
		t.Fatal(err)
	}
	winner, err := store.Reserve(ctx, "item", "winner", "user-1", "unit-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LockForPayment(ctx, "item", winner.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Confirm(ctx, "item", winner.ID); err != nil {
		t.Fatal(err)
	}
	// Simulate a stale/rebuilt reservation record losing the database unique
	// guard for the same unit. The guard loser is terminally forfeited but
	// must not erase the already-confirmed inventory ownership.
	store.mu.Lock()
	it := store.item("item")
	it.holds["loser"] = Hold{ID: "loser", Item: "item", Unit: "unit-1", User: "user-2", State: HoldPaying}
	store.mu.Unlock()
	if err := store.Forfeit(ctx, "item", "loser", ForfeitProofNotAttempted); err != nil {
		t.Fatal(err)
	}
	available, held, paying, confirmed, forfeited, total, err := store.Availability(ctx, "item")
	if err != nil {
		t.Fatal(err)
	}
	if available != 0 || held != 0 || paying != 0 || confirmed != 1 || forfeited != 0 || total != 1 {
		t.Fatalf("forfeit erased or double-counted sold unit: available=%d held=%d paying=%d confirmed=%d forfeited=%d total=%d", available, held, paying, confirmed, forfeited, total)
	}
}
