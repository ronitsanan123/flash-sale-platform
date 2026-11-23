package orders

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/psp"
)

var ErrStandbyUnavailable = errors.New("remote standby acknowledgement unavailable")

type PSP interface {
	Authorize(string, psp.Request) (psp.Charge, error)
	Get(string) (psp.Charge, error)
	Capture(string) (psp.Charge, error)
	Void(string) (psp.Charge, error)
}
type StandbyAckGate interface {
	Wait(context.Context, string, int64) error
}
type StaticStandbyGate struct {
	mu        sync.RWMutex
	Available bool
}

func (g *StaticStandbyGate) SetAvailable(available bool) {
	g.mu.Lock()
	g.Available = available
	g.mu.Unlock()
}

func (g *StaticStandbyGate) Wait(ctx context.Context, home string, epoch int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.RLock()
	available := g.Available
	g.mu.RUnlock()
	if !available {
		return ErrStandbyUnavailable
	}
	return nil
}

type Processor struct {
	Repo        Repository
	Inventory   inventory.Store
	PSP         PSP
	Status      StatusStore
	Gate        StandbyAckGate
	Lease       LeaseToken
	HomeRegion  string
	AmountMinor int64
	Currency    string
	OnPause     func(string)
}

func (p *Processor) Process(ctx context.Context, event inventory.Event) error {
	if p.Repo == nil || p.Inventory == nil || p.PSP == nil || p.Status == nil || p.Gate == nil {
		return fmt.Errorf("processor dependencies incomplete")
	}
	if event.Type != "CheckoutRequested" {
		return nil
	}
	if event.HoldID == "" || event.ItemID == "" {
		return fmt.Errorf("checkout event is missing hold/item identity")
	}

	hold, found, err := p.Inventory.Get(ctx, event.ItemID, event.HoldID)
	if err != nil {
		return err
	}
	if !found {
		// A terminal Redis tombstone may have been cleaned after its retention
		// window. A durable terminal order is still safe to project on replay.
		a, getErr := p.Repo.Get(ctx, event.HoldID)
		if getErr == nil && (a.Order.Status == OrderCancelled || a.Order.Status == OrderConfirmed) {
			return p.Status.Project(ctx, a)
		}
		return inventory.ErrHoldNotFound
	}
	if hold.State == inventory.HoldForfeited || hold.State == inventory.HoldReleased {
		a, getErr := p.Repo.Get(ctx, event.HoldID)
		if getErr == nil && a.Order.Status == OrderCancelled {
			// Terminal inventory disposition is already applied. Never re-enter PSP.
			return p.Status.Project(ctx, a)
		}
		return fmt.Errorf("checkout event hold is terminal (%s) without a cancelled durable order", hold.State)
	}
	if hold.State != inventory.HoldPaying && hold.State != inventory.HoldConfirmed {
		return fmt.Errorf("checkout event hold state is %s", hold.State)
	}
	if event.UnitID != "" && event.UnitID != hold.Unit {
		return inventory.ErrReplayMismatch
	}
	if event.UserID != "" && event.UserID != hold.User {
		return inventory.ErrReplayMismatch
	}
	amount := p.AmountMinor
	if amount <= 0 {
		amount = 1000
	}
	currency := p.Currency
	if currency == "" {
		currency = "USD"
	}
	ce := CheckoutEvent{OrderID: event.HoldID, ItemID: event.ItemID, UnitID: hold.Unit, UserID: hold.User, HomeRegion: p.HomeRegion, FencingEpoch: p.Lease.Epoch, AmountMinor: amount, Currency: currency}
	agg, err := p.Repo.Begin(ctx, p.Lease, ce)
	if err != nil {
		return err
	}
	if agg.Order.Status == OrderCancelled {
		return p.applyCancelled(ctx, agg)
	}
	if agg.Order.Status == OrderConfirmed {
		if err := p.Inventory.Confirm(ctx, agg.Order.ItemID, agg.Order.ID); err != nil {
			return err
		}
		return p.Status.Project(ctx, agg)
	}
	if agg.Payment.Status == PaymentCaptured {
		return p.Status.Project(ctx, agg)
	}
	var charge psp.Charge
	switch agg.Payment.Status {
	case PaymentAuthorized:
		// A replay never calls authorize again.
		charge = psp.Charge{IdempotencyKey: agg.Order.ID, ChargeID: agg.Payment.PSPChargeID, Status: "AUTHORIZED"}
	case PaymentUnknown:
		charge, err = p.PSP.Get(agg.Order.ID)
		if err != nil {
			if errors.Is(err, psp.ErrNotFound) {
				charge, err = p.authorize(ctx, ce)
			} else {
				_, _ = p.Repo.MarkUnknown(ctx, p.Lease, agg.Order.ID)
				return fmt.Errorf("resolve unknown PSP outcome: %w", err)
			}
		}
	case PaymentIntent:
		charge, err = p.authorize(ctx, ce)
	default:
		// NOT_ATTEMPTED, FAILED and VOIDED are terminal/non-authorizable.
		return fmt.Errorf("payment state %s cannot be authorized", agg.Payment.Status)
	}
	if err != nil {
		return err
	}
	if charge.Status == "DECLINED" {
		agg, err = p.Repo.CancelDeclined(ctx, p.Lease, agg.Order.ID, false)
		if err != nil {
			return err
		}
		if err = p.Inventory.ReleaseAfterPayment(ctx, agg.Order.ItemID, agg.Order.ID, inventory.ForfeitProofPSPDeclined); err != nil {
			return err
		}
		return p.Status.Project(ctx, agg)
	}
	if charge.Status == "VOIDED" {
		agg, err = p.Repo.CancelDeclined(ctx, p.Lease, agg.Order.ID, true)
		if err != nil {
			return err
		}
		if err = p.Inventory.ReleaseAfterPayment(ctx, agg.Order.ItemID, agg.Order.ID, inventory.ForfeitProofPSPVoided); err != nil {
			return err
		}
		return p.Status.Project(ctx, agg)
	}
	if charge.Status != "AUTHORIZED" && charge.Status != "CAPTURED" {
		_, _ = p.Repo.MarkUnknown(ctx, p.Lease, agg.Order.ID)
		return fmt.Errorf("PSP outcome remains unknown: %s", charge.Status)
	}
	if agg.Payment.Status != PaymentAuthorized && agg.Payment.Status != PaymentCaptured {
		agg, err = p.Repo.MarkAuthorized(ctx, p.Lease, agg.Order.ID, charge.ChargeID)
		if err != nil {
			return err
		}
		if err = p.Status.Project(ctx, agg); err != nil {
			return err
		}
	}
	if err = p.Gate.Wait(ctx, p.HomeRegion, p.Lease.Epoch); err != nil {
		if p.OnPause != nil {
			p.OnPause("remote standby ACK unavailable")
		}
		return ErrStandbyUnavailable
	}
	agg, err = p.Repo.Confirm(ctx, p.Lease, agg.Order.ID)
	if err != nil {
		return err
	}
	if err = p.Status.Project(ctx, agg); err != nil {
		return err
	}
	// If Redis confirmation fails after the DB commit, redelivery repairs it.
	return p.Inventory.Confirm(ctx, agg.Order.ItemID, agg.Order.ID)
}

func errOrFound(h inventory.Hold, err error) bool { return err == nil && h.ID != "" }
func (p *Processor) authorize(ctx context.Context, e CheckoutEvent) (psp.Charge, error) {
	req := psp.Request{AmountMinor: e.AmountMinor, Currency: e.Currency, Metadata: psp.Metadata{OrderID: e.OrderID, ItemID: e.ItemID, UnitID: e.UnitID, UserID: e.UserID, HomeRegion: e.HomeRegion, FencingEpoch: e.FencingEpoch}}
	c, err := p.PSP.Authorize(e.OrderID, req)
	if err == nil {
		return c, nil
	} // Lost response is resolved by the same idempotency key.
	lookup, lookupErr := p.PSP.Get(e.OrderID)
	if lookupErr == nil {
		return lookup, nil
	}
	_, _ = p.Repo.MarkUnknown(ctx, p.Lease, e.OrderID)
	return psp.Charge{}, fmt.Errorf("authorize outcome unknown (authorize: %v; lookup: %v)", err, lookupErr)
}
func (p *Processor) applyCancelled(ctx context.Context, a Aggregate) error {
	switch a.Order.InventoryDisposition {
	case DispositionForfeit:
		proof := inventory.ForfeitProofNotAttempted
		if a.Payment.Status == PaymentVoided {
			proof = inventory.ForfeitProofPSPVoided
		}
		if a.Payment.Status == PaymentFailed {
			proof = inventory.ForfeitProofPSPDeclined
		}
		if err := p.Inventory.Forfeit(ctx, a.Order.ItemID, a.Order.ID, proof); err != nil {
			return err
		}
	case DispositionRelease:
		proof := inventory.ForfeitProofPSPDeclined
		if a.Payment.Status == PaymentVoided {
			proof = inventory.ForfeitProofPSPVoided
		}
		if err := p.Inventory.ReleaseAfterPayment(ctx, a.Order.ItemID, a.Order.ID, proof); err != nil {
			return err
		}
	}
	return p.Status.Project(ctx, a)
}

// HandleWebhook applies authenticated PSP callbacks through the same fenced,
// idempotent state machine as the Kafka worker. The HTTP layer verifies the
// signature and timestamp before calling this method.
func (p *Processor) HandleWebhook(ctx context.Context, charge psp.Charge) error {
	if charge.EventID == "" || charge.IdempotencyKey == "" || charge.Metadata.OrderID != charge.IdempotencyKey || charge.Metadata.HomeRegion != p.HomeRegion || charge.Metadata.ItemID == "" || charge.Metadata.UnitID == "" || charge.Metadata.UserID == "" || charge.Metadata.FencingEpoch <= 0 {
		return fmt.Errorf("PSP webhook has incomplete immutable metadata")
	}
	a, err := p.Repo.Get(ctx, charge.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("webhook order lookup: %w", err)
	}
	if a.Order.ItemID != charge.Metadata.ItemID || a.Order.UnitID != charge.Metadata.UnitID || a.Order.UserID != charge.Metadata.UserID || a.Order.HomeRegion != charge.Metadata.HomeRegion {
		return ErrIdentityMismatch
	}
	if a.Payment.LastAttemptFencingEpoch != charge.Metadata.FencingEpoch {
		return ErrFenced
	}

	switch charge.EventType {
	case "AUTHORIZATION":
		switch charge.Status {
		case "AUTHORIZED":
			if a.Order.Status == OrderCancelled {
				// A late authorization for a cancelled/forfeited order must be
				// tombstoned at the PSP before the terminal disposition is projected.
				voided, voidErr := p.PSP.Void(a.Order.ID)
				if voidErr != nil {
					return fmt.Errorf("void late authorization: %w", voidErr)
				}
				if voided.Status != "VOIDED" {
					return fmt.Errorf("PSP did not confirm late authorization void")
				}
				a, err = p.Repo.MarkPSPOutcome(ctx, p.Lease, a.Order.ID, PaymentVoided, voided.ChargeID)
				if err != nil {
					return err
				}
				return p.applyCancelled(ctx, a)
			}
			if a.Order.Status == OrderConfirmed {
				if err := p.Inventory.Confirm(ctx, a.Order.ItemID, a.Order.ID); err != nil {
					return err
				}
				return p.Status.Project(ctx, a)
			}
			if a.Payment.Status != PaymentAuthorized && a.Payment.Status != PaymentCaptured {
				a, err = p.Repo.MarkAuthorized(ctx, p.Lease, a.Order.ID, charge.ChargeID)
				if err != nil {
					return err
				}
			}
			if err := p.Gate.Wait(ctx, p.HomeRegion, p.Lease.Epoch); err != nil {
				if p.OnPause != nil {
					p.OnPause("remote standby ACK unavailable")
				}
				return ErrStandbyUnavailable
			}
			a, err = p.Repo.Confirm(ctx, p.Lease, a.Order.ID)
			if err != nil {
				return err
			}
			if err = p.Status.Project(ctx, a); err != nil {
				return err
			}
			return p.Inventory.Confirm(ctx, a.Order.ItemID, a.Order.ID)
		case "DECLINED":
			a, err = p.Repo.MarkPSPOutcome(ctx, p.Lease, a.Order.ID, PaymentFailed, charge.ChargeID)
			if err != nil {
				return err
			}
			return p.applyCancelled(ctx, a)
		case "VOIDED":
			a, err = p.Repo.MarkPSPOutcome(ctx, p.Lease, a.Order.ID, PaymentVoided, charge.ChargeID)
			if err != nil {
				return err
			}
			return p.applyCancelled(ctx, a)
		default:
			return fmt.Errorf("unsupported authorization webhook status %q", charge.Status)
		}
	case "CAPTURE":
		if charge.Status != "CAPTURED" {
			return fmt.Errorf("unsupported capture webhook status %q", charge.Status)
		}
		if a.Order.Status != OrderConfirmed {
			return ErrCapturedOrphan
		}
		updated, err := p.Repo.MarkCaptured(ctx, p.Lease, a.Order.ID, charge.ChargeID)
		if err != nil {
			return err
		}
		return p.Status.Project(ctx, updated)
	default:
		return fmt.Errorf("unsupported PSP webhook event type %q", charge.EventType)
	}
}

func (p *Processor) CapturePending(ctx context.Context, limit int) error {
	items, err := p.Repo.PendingCapture(ctx, limit)
	if err != nil {
		return err
	}
	for _, a := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.Gate.Wait(ctx, p.HomeRegion, p.Lease.Epoch); err != nil {
			return err
		}
		charge, err := p.PSP.Capture(a.Order.ID)
		if err != nil {
			return err
		}
		updated, err := p.Repo.MarkCaptured(ctx, p.Lease, a.Order.ID, charge.ChargeID)
		if err != nil {
			return err
		}
		if err := p.Status.Project(ctx, updated); err != nil {
			return err
		}
	}
	return nil
}

// ResolveUnknownOrphan uses the provider ledger-discovery API when a region
// loss erased the order ID. It is deliberately separate from hot-path Process.
func ResolveUnknownOrphan(page psp.LedgerPage, durableOrderIDs map[string]bool) (orphans []psp.Charge) {
	for _, e := range page.Entries {
		if e.EventType == "AUTHORIZATION" && e.Status == "AUTHORIZED" && !durableOrderIDs[e.IdempotencyKey] {
			orphans = append(orphans, e)
		}
	}
	return
}
