package orders

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"flashsale-platform/prototype/internal/pgwire"
	"flashsale-platform/prototype/internal/psp"
)

func (r *PostgresRepository) LoadLedgerCheckpoint(ctx context.Context, home string) (LedgerCheckpoint, bool, error) {
	res, err := r.DB.Query(ctx, `SELECT home_region,extract(epoch from scan_from)::text,extract(epoch from scan_to)::text,
snapshot_id,extract(epoch from complete_through)::text,fencing_epoch::text,extract(epoch from updated_at)::text
FROM psp_ledger_scan_checkpoints WHERE home_region=$1`, home)
	if err != nil {
		return LedgerCheckpoint{}, false, err
	}
	if res.RowCount() == 0 {
		return LedgerCheckpoint{}, false, nil
	}
	if res.RowCount() != 1 {
		return LedgerCheckpoint{}, false, fmt.Errorf("duplicate PSP ledger checkpoint for %s", home)
	}
	c := func(i int) string { return col(res, 0, i) }
	epoch, _ := strconv.ParseInt(c(5), 10, 64)
	return LedgerCheckpoint{HomeRegion: c(0), ScanFrom: parsePGTime(c(1)), ScanTo: parsePGTime(c(2)), SnapshotID: c(3), CompleteThrough: parsePGTime(c(4)), FencingEpoch: epoch, UpdatedAt: parsePGTime(c(6))}, true, nil
}

func (r *PostgresRepository) SaveLedgerCheckpoint(ctx context.Context, lease LeaseToken, cp LedgerCheckpoint) error {
	if cp.HomeRegion != lease.HomeRegion || cp.FencingEpoch != lease.Epoch || cp.SnapshotID == "" || cp.CompleteThrough.Before(cp.ScanTo) {
		return fmt.Errorf("invalid or incomplete PSP ledger checkpoint")
	}
	return r.DB.WithTx(ctx, func(tx *pgwire.Tx) error {
		if err := verifyLease(tx, lease); err != nil {
			return err
		}
		if _, err := tx.Exec("SET LOCAL synchronous_commit = 'on'"); err != nil {
			return err
		}
		res, err := tx.Exec(`INSERT INTO psp_ledger_scan_checkpoints(home_region,scan_from,scan_to,snapshot_id,next_cursor,complete_through,fencing_epoch,updated_at)
VALUES($1,$2,$3,$4,NULL,$5,$6,clock_timestamp())
ON CONFLICT(home_region) DO UPDATE SET scan_from=EXCLUDED.scan_from,scan_to=EXCLUDED.scan_to,
snapshot_id=EXCLUDED.snapshot_id,next_cursor=NULL,complete_through=EXCLUDED.complete_through,
fencing_epoch=EXCLUDED.fencing_epoch,updated_at=clock_timestamp()
WHERE psp_ledger_scan_checkpoints.complete_through <= EXCLUDED.complete_through
RETURNING home_region`, lease.HomeRegion, cp.ScanFrom.UTC().Format(time.RFC3339Nano), cp.ScanTo.UTC().Format(time.RFC3339Nano), cp.SnapshotID, cp.CompleteThrough.UTC().Format(time.RFC3339Nano), strconv.FormatInt(lease.Epoch, 10))
		if err != nil {
			return err
		}
		if res.RowCount() != 1 {
			return fmt.Errorf("PSP ledger checkpoint would regress")
		}
		return nil
	})
}

func (r *PostgresRepository) HasDurableConfirmation(ctx context.Context, orderID string) (bool, error) {
	res, err := r.DB.Query(ctx, `SELECT (o.status='CONFIRMED' AND p.status IN ('AUTHORIZED','CAPTURED'))::text
FROM orders o JOIN payments p USING(order_id) WHERE o.order_id=$1`, orderID)
	if err != nil {
		return false, err
	}
	if res.RowCount() == 0 {
		return false, nil
	}
	if res.RowCount() != 1 {
		return false, fmt.Errorf("duplicate order id %s", orderID)
	}
	return col(res, 0, 0) == "t", nil
}

func (r *PostgresRepository) RecordOrphanAuthorization(ctx context.Context, lease LeaseToken, charge psp.Charge) error {
	if charge.IdempotencyKey == "" || charge.Metadata.OrderID != charge.IdempotencyKey || charge.Metadata.HomeRegion != lease.HomeRegion || charge.Metadata.ItemID == "" || charge.Metadata.UnitID == "" || charge.Metadata.UserID == "" {
		return fmt.Errorf("orphan PSP record has incomplete or mismatched metadata")
	}
	if charge.Status != "VOIDED" && charge.Status != "DECLINED" {
		return fmt.Errorf("orphan must be definitively voided/declined before persistence")
	}
	return r.DB.WithTx(ctx, func(tx *pgwire.Tx) error {
		if err := verifyLease(tx, lease); err != nil {
			return err
		}
		if _, err := tx.Exec("SET LOCAL synchronous_commit = 'on'"); err != nil {
			return err
		}
		a, err := getAggregateTx(tx, charge.IdempotencyKey, true)
		if err != nil && err != ErrOrderNotFound {
			return err
		}
		status := "VOIDED"
		if charge.Status == "DECLINED" {
			status = "FAILED"
		}
		if err == ErrOrderNotFound {
			if _, err := tx.Exec(`INSERT INTO orders(order_id,item_id,unit_id,user_id,home_region,status,inventory_disposition,version,fencing_epoch)
VALUES($1,$2,$3,$4,$5,'CANCELLED','RELEASE',1,$6)
ON CONFLICT(order_id) DO NOTHING`, charge.IdempotencyKey, charge.Metadata.ItemID, charge.Metadata.UnitID, charge.Metadata.UserID, charge.Metadata.HomeRegion, strconv.FormatInt(lease.Epoch, 10)); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO payments(order_id,status,psp_charge_id,amount_minor,attempts,last_attempt_fencing_epoch)
VALUES($1,$2,NULLIF($3,''),$4,1,$5)
ON CONFLICT(order_id) DO NOTHING`, charge.IdempotencyKey, status, charge.ChargeID, strconv.FormatInt(charge.AmountMinor, 10), strconv.FormatInt(charge.Metadata.FencingEpoch, 10)); err != nil {
				return err
			}
		} else {
			if a.Order.ItemID != charge.Metadata.ItemID || a.Order.UnitID != charge.Metadata.UnitID || a.Order.UserID != charge.Metadata.UserID || a.Order.HomeRegion != charge.Metadata.HomeRegion {
				return ErrIdentityMismatch
			}
			if a.Order.Status == OrderConfirmed {
				// A durable confirmed order is never rewritten by orphan repair.
				return nil
			}
			if a.Order.Status == OrderCancelled && a.Order.InventoryDisposition == DispositionRelease && a.Payment.Status == PaymentStatus(status) && a.Payment.PSPChargeID == charge.ChargeID {
				return nil // overlapping recovery scans are idempotent
			}
			if _, err := tx.Exec(`UPDATE orders SET status='CANCELLED',
inventory_disposition=CASE WHEN inventory_disposition='FORFEIT' THEN 'FORFEIT' ELSE 'RELEASE' END,
version=version+1,fencing_epoch=$1,updated_at=clock_timestamp() WHERE order_id=$2 AND status<>'CONFIRMED'`, strconv.FormatInt(lease.Epoch, 10), charge.IdempotencyKey); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE payments SET status=$1,psp_charge_id=NULLIF($2,''),last_attempt_fencing_epoch=$3,updated_at=clock_timestamp() WHERE order_id=$4`, status, charge.ChargeID, strconv.FormatInt(charge.Metadata.FencingEpoch, 10), charge.IdempotencyKey); err != nil {
				return err
			}
		}
		action := "ORPHAN_VOIDED"
		if charge.Status == "DECLINED" {
			action = "ORPHAN_FAILED"
		}
		_, err = tx.Exec(`INSERT INTO regional_recovery_records(home_region,idempotency_key,psp_event_id,action,source_epoch,recovery_epoch,created_at)
VALUES($1,$2,$3,$4,$5,$6,clock_timestamp())
ON CONFLICT(home_region,idempotency_key) DO UPDATE SET psp_event_id=EXCLUDED.psp_event_id,action=EXCLUDED.action,recovery_epoch=EXCLUDED.recovery_epoch,created_at=clock_timestamp()`, lease.HomeRegion, charge.IdempotencyKey, charge.EventID, action, strconv.FormatInt(charge.Metadata.FencingEpoch, 10), strconv.FormatInt(lease.Epoch, 10))
		return err
	})
}

var _ RecoveryRepository = (*PostgresRepository)(nil)
