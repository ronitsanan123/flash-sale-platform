package orders

import (
	"context"
	"fmt"
	"time"

	"flashsale-platform/prototype/internal/psp"
)

func (r *MemoryRepository) LoadLedgerCheckpoint(ctx context.Context, home string) (LedgerCheckpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return LedgerCheckpoint{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cp, ok := r.checkpoints[home]
	return cp, ok, nil
}

func (r *MemoryRepository) SaveLedgerCheckpoint(ctx context.Context, lease LeaseToken, cp LedgerCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateLease(lease); err != nil {
		return err
	}
	if cp.HomeRegion != lease.HomeRegion || cp.FencingEpoch != lease.Epoch || cp.SnapshotID == "" || cp.CompleteThrough.Before(cp.ScanTo) {
		return fmt.Errorf("invalid or incomplete PSP ledger checkpoint")
	}
	if prior, ok := r.checkpoints[cp.HomeRegion]; ok && cp.CompleteThrough.Before(prior.CompleteThrough) {
		return fmt.Errorf("PSP ledger checkpoint regression")
	}
	cp.UpdatedAt = r.now().UTC()
	r.checkpoints[cp.HomeRegion] = cp
	return nil
}

func (r *MemoryRepository) HasDurableConfirmation(ctx context.Context, orderID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.orders[orderID]
	if !ok || o.Status != OrderConfirmed {
		return false, nil
	}
	p := r.payments[orderID]
	return p.Status == PaymentAuthorized || p.Status == PaymentCaptured, nil
}

func (r *MemoryRepository) RecordOrphanAuthorization(ctx context.Context, lease LeaseToken, charge psp.Charge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if charge.IdempotencyKey == "" || charge.Metadata.OrderID != charge.IdempotencyKey || charge.Metadata.HomeRegion != lease.HomeRegion || charge.Metadata.ItemID == "" || charge.Metadata.UnitID == "" || charge.Metadata.UserID == "" {
		return fmt.Errorf("orphan PSP record has incomplete or mismatched metadata")
	}
	if charge.Status != "VOIDED" && charge.Status != "DECLINED" {
		return fmt.Errorf("orphan must be definitively voided/declined before persistence")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateLease(lease); err != nil {
		return err
	}
	id := charge.IdempotencyKey
	if existing, ok := r.orders[id]; ok {
		if existing.ItemID != charge.Metadata.ItemID || existing.UnitID != charge.Metadata.UnitID || existing.UserID != charge.Metadata.UserID || existing.HomeRegion != charge.Metadata.HomeRegion {
			return ErrIdentityMismatch
		}
		if existing.Status == OrderConfirmed {
			return nil
		}
		targetPayment := PaymentVoided
		if charge.Status == "DECLINED" {
			targetPayment = PaymentFailed
		}
		priorPayment := r.payments[id]
		if existing.Status == OrderCancelled && existing.InventoryDisposition == DispositionRelease && priorPayment.Status == targetPayment && priorPayment.PSPChargeID == charge.ChargeID {
			return nil // overlapping scans are idempotent
		}
		old := existing
		existing.Status = OrderCancelled
		if existing.InventoryDisposition != DispositionForfeit {
			existing.InventoryDisposition = DispositionRelease
		}
		existing.Version++
		existing.FencingEpoch = lease.Epoch
		existing.UpdatedAt = r.now().UTC()
		p := r.payments[id]
		if charge.Status == "VOIDED" {
			p.Status = PaymentVoided
		} else {
			p.Status = PaymentFailed
		}
		p.PSPChargeID = charge.ChargeID
		p.LastAttemptFencingEpoch = charge.Metadata.FencingEpoch
		p.UpdatedAt = existing.UpdatedAt
		r.orders[id], r.payments[id] = existing, p
		key := unitKey(old.ItemID, old.UnitID)
		if r.unitOwners[key] == id {
			delete(r.unitOwners, key)
		}
		return nil
	}
	now := r.now().UTC()
	o := Order{ID: id, ItemID: charge.Metadata.ItemID, UnitID: charge.Metadata.UnitID, UserID: charge.Metadata.UserID, HomeRegion: charge.Metadata.HomeRegion, Status: OrderCancelled, InventoryDisposition: DispositionRelease, Version: 1, FencingEpoch: lease.Epoch, CreatedAt: now, UpdatedAt: now}
	status := PaymentVoided
	if charge.Status == "DECLINED" {
		status = PaymentFailed
	}
	p := Payment{OrderID: id, Status: status, PSPChargeID: charge.ChargeID, AmountMinor: charge.AmountMinor, Attempts: 1, LastAttemptFencingEpoch: charge.Metadata.FencingEpoch, UpdatedAt: now}
	r.orders[id], r.payments[id] = o, p
	return nil
}

var _ RecoveryRepository = (*MemoryRepository)(nil)
var _ = time.Time{}
