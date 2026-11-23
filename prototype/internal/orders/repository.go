package orders

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrLeaseLost         = errors.New("regional writer lease lost or expired")
	ErrFenced            = errors.New("stale fencing epoch")
	ErrIdentityMismatch  = errors.New("order replay immutable identity mismatch")
	ErrUnitOwned         = errors.New("unit already has a live order owner")
	ErrInvalidTransition = errors.New("invalid order/payment state transition")
	ErrOrderNotFound     = errors.New("order not found")
)

type Repository interface {
	Begin(context.Context, LeaseToken, CheckoutEvent) (Aggregate, error)
	Get(context.Context, string) (Aggregate, error)
	MarkAuthorized(context.Context, LeaseToken, string, string) (Aggregate, error)
	MarkUnknown(context.Context, LeaseToken, string) (Aggregate, error)
	CancelDeclined(context.Context, LeaseToken, string, bool) (Aggregate, error)
	MarkPSPOutcome(context.Context, LeaseToken, string, PaymentStatus, string) (Aggregate, error)
	Confirm(context.Context, LeaseToken, string) (Aggregate, error)
	MarkCaptured(context.Context, LeaseToken, string, string) (Aggregate, error)
	PendingCapture(context.Context, int) ([]Aggregate, error)
}

type LeaseRepository interface {
	RenewLease(context.Context, LeaseToken, time.Duration) (Lease, error)
	TakeoverLease(context.Context, string, string, bool, time.Duration) (Lease, error)
	Lease(context.Context, string) (Lease, error)
}

type MemoryRepository struct {
	mu          sync.Mutex
	orders      map[string]Order
	payments    map[string]Payment
	unitOwners  map[string]string
	leases      map[string]Lease
	checkpoints map[string]LedgerCheckpoint
	now         func() time.Time
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{orders: map[string]Order{}, payments: map[string]Payment{}, unitOwners: map[string]string{}, leases: map[string]Lease{}, checkpoints: map[string]LedgerCheckpoint{}, now: time.Now}
}
func unitKey(item, unit string) string { return item + "\x00" + unit }
func (r *MemoryRepository) SeedLease(home, owner string, epoch int64, duration time.Duration) Lease {
	r.mu.Lock()
	defer r.mu.Unlock()
	l := Lease{HomeRegion: home, OwnerID: owner, Epoch: epoch, ExpiresAt: r.now().Add(duration)}
	r.leases[home] = l
	return l
}
func (r *MemoryRepository) validateLease(t LeaseToken) error {
	l, ok := r.leases[t.HomeRegion]
	if !ok || l.OwnerID != t.OwnerID {
		return ErrLeaseLost
	}
	if l.Epoch != t.Epoch {
		return ErrFenced
	}
	if !r.now().Before(l.ExpiresAt) {
		return ErrLeaseLost
	}
	return nil
}
func (r *MemoryRepository) RenewLease(ctx context.Context, t LeaseToken, d time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateLease(t); err != nil {
		return Lease{}, err
	}
	l := r.leases[t.HomeRegion]
	l.ExpiresAt = r.now().Add(d)
	r.leases[t.HomeRegion] = l
	return l, nil
}
func (r *MemoryRepository) TakeoverLease(ctx context.Context, home, newOwner string, oldPrimaryFenced bool, d time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if !oldPrimaryFenced {
		return Lease{}, fmt.Errorf("takeover refused: old primary is not confirmed unreachable or fenced")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.leases[home]
	if !ok {
		return Lease{}, ErrLeaseLost
	}
	l.OwnerID = newOwner
	l.Epoch++
	l.ExpiresAt = r.now().Add(d)
	r.leases[home] = l
	return l, nil
}
func (r *MemoryRepository) Lease(ctx context.Context, home string) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.leases[home]
	if !ok {
		return Lease{}, ErrLeaseLost
	}
	return l, nil
}

func (r *MemoryRepository) Begin(ctx context.Context, lease LeaseToken, e CheckoutEvent) (Aggregate, error) {
	if err := ctx.Err(); err != nil {
		return Aggregate{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateLease(lease); err != nil {
		return Aggregate{}, err
	}
	if e.OrderID == "" || e.ItemID == "" || e.UnitID == "" || e.UserID == "" || e.HomeRegion == "" || e.AmountMinor <= 0 {
		return Aggregate{}, fmt.Errorf("checkout event missing immutable identity or amount")
	}
	if e.HomeRegion != lease.HomeRegion || e.FencingEpoch != lease.Epoch {
		return Aggregate{}, ErrFenced
	}
	if prior, ok := r.orders[e.OrderID]; ok {
		if prior.ItemID != e.ItemID || prior.UnitID != e.UnitID || prior.UserID != e.UserID || prior.HomeRegion != e.HomeRegion {
			return Aggregate{}, ErrIdentityMismatch
		}
		return Aggregate{Order: prior, Payment: r.payments[e.OrderID]}, nil
	}
	key := unitKey(e.ItemID, e.UnitID)
	if owner, ok := r.unitOwners[key]; ok && owner != e.OrderID {
		now := r.now().UTC()
		o := Order{ID: e.OrderID, ItemID: e.ItemID, UnitID: e.UnitID, UserID: e.UserID, HomeRegion: e.HomeRegion, Status: OrderCancelled, InventoryDisposition: DispositionForfeit, Version: 1, FencingEpoch: lease.Epoch, CreatedAt: now, UpdatedAt: now}
		p := Payment{OrderID: e.OrderID, Status: PaymentNotAttempted, AmountMinor: e.AmountMinor, LastAttemptFencingEpoch: lease.Epoch, UpdatedAt: now}
		r.orders[e.OrderID] = o
		r.payments[e.OrderID] = p
		return Aggregate{Order: o, Payment: p, Created: true, UniqueConflict: true}, nil
	}
	now := r.now().UTC()
	o := Order{ID: e.OrderID, ItemID: e.ItemID, UnitID: e.UnitID, UserID: e.UserID, HomeRegion: e.HomeRegion, Status: OrderPendingPayment, InventoryDisposition: DispositionNone, Version: 1, FencingEpoch: lease.Epoch, CreatedAt: now, UpdatedAt: now}
	p := Payment{OrderID: e.OrderID, Status: PaymentIntent, AmountMinor: e.AmountMinor, LastAttemptFencingEpoch: lease.Epoch, UpdatedAt: now}
	r.orders[e.OrderID] = o
	r.payments[e.OrderID] = p
	r.unitOwners[key] = e.OrderID
	return Aggregate{Order: o, Payment: p, Created: true}, nil
}
func (r *MemoryRepository) Get(ctx context.Context, id string) (Aggregate, error) {
	if err := ctx.Err(); err != nil {
		return Aggregate{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.orders[id]
	if !ok {
		return Aggregate{}, ErrOrderNotFound
	}
	return Aggregate{Order: o, Payment: r.payments[id]}, nil
}
func (r *MemoryRepository) mutate(ctx context.Context, t LeaseToken, id string, fn func(*Order, *Payment) error) (Aggregate, error) {
	if err := ctx.Err(); err != nil {
		return Aggregate{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateLease(t); err != nil {
		return Aggregate{}, err
	}
	o, ok := r.orders[id]
	if !ok {
		return Aggregate{}, ErrOrderNotFound
	}
	p := r.payments[id]
	oldO, oldP := o, p
	if err := fn(&o, &p); err != nil {
		return Aggregate{}, err
	}
	if o != oldO || p != oldP {
		// A definitive decline/void releases relational ownership so a later
		// order may acquire the unit. FORFEIT intentionally retains no new owner;
		// SOLD remains owned by the confirmed order.
		if oldO.Status != OrderCancelled && o.Status == OrderCancelled && o.InventoryDisposition == DispositionRelease {
			key := unitKey(oldO.ItemID, oldO.UnitID)
			if r.unitOwners[key] == id {
				delete(r.unitOwners, key)
			}
		}
		o.Version++
		o.FencingEpoch = t.Epoch
		o.UpdatedAt = r.now().UTC()
		p.UpdatedAt = o.UpdatedAt
		r.orders[id] = o
		r.payments[id] = p
	}
	return Aggregate{Order: o, Payment: p}, nil
}
func (r *MemoryRepository) MarkAuthorized(ctx context.Context, t LeaseToken, id, charge string) (Aggregate, error) {
	return r.mutate(ctx, t, id, func(o *Order, p *Payment) error {
		if o.Status == OrderCancelled {
			return ErrInvalidTransition
		}
		if p.Status == PaymentAuthorized || p.Status == PaymentCaptured {
			return nil
		}
		if o.Status != OrderPendingPayment || (p.Status != PaymentIntent && p.Status != PaymentUnknown) {
			return ErrInvalidTransition
		}
		p.Status = PaymentAuthorized
		p.PSPChargeID = charge
		p.Attempts++
		p.LastAttemptFencingEpoch = t.Epoch
		return nil
	})
}
func (r *MemoryRepository) MarkUnknown(ctx context.Context, t LeaseToken, id string) (Aggregate, error) {
	return r.mutate(ctx, t, id, func(o *Order, p *Payment) error {
		if o.Status == OrderCancelled || o.Status == OrderConfirmed || p.Status == PaymentCaptured {
			return ErrInvalidTransition
		}
		if p.Status == PaymentUnknown {
			return nil
		}
		p.Status = PaymentUnknown
		p.LastAttemptFencingEpoch = t.Epoch
		return nil
	})
}
func (r *MemoryRepository) CancelDeclined(ctx context.Context, t LeaseToken, id string, voided bool) (Aggregate, error) {
	return r.mutate(ctx, t, id, func(o *Order, p *Payment) error {
		if o.Status == OrderConfirmed {
			return ErrInvalidTransition
		}
		if o.Status == OrderCancelled {
			return nil
		}
		o.Status = OrderCancelled
		o.InventoryDisposition = DispositionRelease
		if voided {
			p.Status = PaymentVoided
		} else {
			p.Status = PaymentFailed
		}
		return nil
	})
}
func (r *MemoryRepository) MarkPSPOutcome(ctx context.Context, t LeaseToken, id string, status PaymentStatus, charge string) (Aggregate, error) {
	if status != PaymentFailed && status != PaymentVoided {
		return Aggregate{}, ErrInvalidTransition
	}
	return r.mutate(ctx, t, id, func(o *Order, p *Payment) error {
		if o.Status == OrderConfirmed {
			return ErrInvalidTransition
		}
		if status == PaymentVoided && p.Status == PaymentVoided && p.PSPChargeID == charge {
			return nil
		}
		if status == PaymentFailed && p.Status == PaymentFailed && p.PSPChargeID == charge {
			return nil
		}
		if o.Status != OrderCancelled {
			o.Status = OrderCancelled
			if o.InventoryDisposition == DispositionNone {
				o.InventoryDisposition = DispositionRelease
			}
		}
		p.Status = status
		p.PSPChargeID = charge
		p.LastAttemptFencingEpoch = t.Epoch
		return nil
	})
}

func (r *MemoryRepository) Confirm(ctx context.Context, t LeaseToken, id string) (Aggregate, error) {
	return r.mutate(ctx, t, id, func(o *Order, p *Payment) error {
		if o.Status == OrderConfirmed {
			return nil
		}
		if o.Status != OrderPendingPayment || p.Status != PaymentAuthorized {
			return ErrInvalidTransition
		}
		o.Status = OrderConfirmed
		o.InventoryDisposition = DispositionSold
		return nil
	})
}
func (r *MemoryRepository) MarkCaptured(ctx context.Context, t LeaseToken, id, charge string) (Aggregate, error) {
	return r.mutate(ctx, t, id, func(o *Order, p *Payment) error {
		if o.Status != OrderConfirmed || (p.Status != PaymentAuthorized && p.Status != PaymentCaptured) {
			return ErrInvalidTransition
		}
		if p.Status == PaymentCaptured {
			return nil
		}
		p.Status = PaymentCaptured
		if charge != "" {
			p.PSPChargeID = charge
		}
		return nil
	})
}
func (r *MemoryRepository) PendingCapture(ctx context.Context, limit int) ([]Aggregate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Aggregate, 0)
	for id, o := range r.orders {
		p := r.payments[id]
		if o.Status == OrderConfirmed && p.Status == PaymentAuthorized {
			out = append(out, Aggregate{Order: o, Payment: p})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}
