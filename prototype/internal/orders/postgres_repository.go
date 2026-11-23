package orders

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"flashsale-platform/prototype/internal/pgwire"
)

// PostgresRepository persists authoritative orders, payments and regional
// leases. A region_lease FOR SHARE lock is held for each write transaction;
// takeover uses FOR UPDATE and increments the epoch. Confirm sets local
// synchronous_commit=on, which waits for a remote flush only when PostgreSQL
// is configured with synchronous_standby_names and a healthy sync standby.
type PostgresRepository struct {
	DB                        *pgwire.Pool
	RequireSynchronousStandby bool
}

func NewPostgresRepository(db *pgwire.Pool, requireSync bool) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres pool required")
	}
	return &PostgresRepository{DB: db, RequireSynchronousStandby: requireSync}, nil
}

func (r *PostgresRepository) CheckSynchronousStandby(ctx context.Context) error {
	if !r.RequireSynchronousStandby {
		return nil
	}
	v, err := r.DB.Query(ctx, "SHOW synchronous_standby_names")
	if err != nil {
		return err
	}
	if v.RowCount() != 1 || strings.TrimSpace(col(v, 0, 0)) == "" {
		return fmt.Errorf("synchronous_standby_names must be configured for confirmed-order durability")
	}
	// Configuration alone is insufficient: require a currently streaming
	// synchronous standby. The 2-of-2 commit will still block/fail closed if
	// that standby disappears after this health check.
	state, err := r.DB.Query(ctx, `SELECT count(*)::text FROM pg_stat_replication WHERE state='streaming' AND sync_state='sync'`)
	if err != nil {
		return err
	}
	if state.RowCount() != 1 {
		return fmt.Errorf("unable to inspect synchronous standby state")
	}
	n, _ := strconv.Atoi(col(state, 0, 0))
	if n < 1 {
		return fmt.Errorf("no streaming synchronous standby is currently available")
	}
	return nil
}

func (r *PostgresRepository) CreateLease(ctx context.Context, l Lease) error {
	if l.HomeRegion == "" || l.OwnerID == "" || l.Epoch <= 0 || !l.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("invalid initial lease")
	}
	_, err := r.DB.Exec(ctx, `INSERT INTO region_leases(home_region,owner_id,epoch,lease_expires_at)
VALUES($1,$2,$3,$4) ON CONFLICT(home_region) DO NOTHING`, l.HomeRegion, l.OwnerID, strconv.FormatInt(l.Epoch, 10), l.ExpiresAt.UTC().Format(time.RFC3339Nano))
	return err
}

func (r *PostgresRepository) Begin(ctx context.Context, lease LeaseToken, e CheckoutEvent) (Aggregate, error) {
	if e.OrderID == "" || e.ItemID == "" || e.UnitID == "" || e.UserID == "" || e.HomeRegion == "" || e.AmountMinor <= 0 {
		return Aggregate{}, fmt.Errorf("checkout event missing immutable identity or amount")
	}
	if e.HomeRegion != lease.HomeRegion || e.FencingEpoch != lease.Epoch {
		return Aggregate{}, ErrFenced
	}
	var out Aggregate
	err := r.DB.WithTx(ctx, func(tx *pgwire.Tx) error {
		if err := verifyLease(tx, lease); err != nil {
			return err
		}
		if _, err := tx.Exec("SET LOCAL synchronous_commit = 'local'"); err != nil {
			return err
		}
		if prior, err := getAggregateTx(tx, e.OrderID, true); err == nil {
			if !sameIdentity(prior.Order, e) {
				return ErrIdentityMismatch
			}
			out = prior
			return nil
		} else if !errors.Is(err, ErrOrderNotFound) {
			return err
		}

		if _, err := tx.Exec("SAVEPOINT order_intent_insert"); err != nil {
			return err
		}
		inserted, err := tx.Exec(`INSERT INTO orders(order_id,item_id,unit_id,user_id,home_region,status,inventory_disposition,version,fencing_epoch)
VALUES($1,$2,$3,$4,$5,'PENDING_PAYMENT','NONE',1,$6)
ON CONFLICT(order_id) DO NOTHING RETURNING order_id`, e.OrderID, e.ItemID, e.UnitID, e.UserID, e.HomeRegion, strconv.FormatInt(lease.Epoch, 10))
		if err != nil {
			var pe *pgwire.Error
			if !errors.As(err, &pe) || pe.SQLState != "23505" || pe.Constraint != "orders_one_owner_per_unit" {
				return err
			}
			if _, rbErr := tx.Exec("ROLLBACK TO SAVEPOINT order_intent_insert"); rbErr != nil {
				return rbErr
			}
			if _, err := tx.Exec(`INSERT INTO orders(order_id,item_id,unit_id,user_id,home_region,status,inventory_disposition,version,fencing_epoch)
VALUES($1,$2,$3,$4,$5,'CANCELLED','FORFEIT',1,$6)`, e.OrderID, e.ItemID, e.UnitID, e.UserID, e.HomeRegion, strconv.FormatInt(lease.Epoch, 10)); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO payments(order_id,status,amount_minor,attempts,last_attempt_fencing_epoch)
VALUES($1,'NOT_ATTEMPTED',$2,0,$3)`, e.OrderID, strconv.FormatInt(e.AmountMinor, 10), strconv.FormatInt(lease.Epoch, 10)); err != nil {
				return err
			}
			out, err = getAggregateTx(tx, e.OrderID, false)
			out.UniqueConflict = true
			out.Created = true
			return err
		}
		if inserted.RowCount() == 0 {
			prior, err := getAggregateTx(tx, e.OrderID, true)
			if err != nil {
				return err
			}
			if !sameIdentity(prior.Order, e) {
				return ErrIdentityMismatch
			}
			out = prior
			return nil
		}
		if _, err := tx.Exec(`INSERT INTO payments(order_id,status,amount_minor,attempts,last_attempt_fencing_epoch)
VALUES($1,'INTENT',$2,0,$3)`, e.OrderID, strconv.FormatInt(e.AmountMinor, 10), strconv.FormatInt(lease.Epoch, 10)); err != nil {
			return err
		}
		out, err = getAggregateTx(tx, e.OrderID, false)
		out.Created = true
		return err
	})
	return out, err
}

func sameIdentity(o Order, e CheckoutEvent) bool {
	return o.ID == e.OrderID && o.ItemID == e.ItemID && o.UnitID == e.UnitID && o.UserID == e.UserID && o.HomeRegion == e.HomeRegion
}
func verifyLease(tx *pgwire.Tx, lease LeaseToken) error {
	if lease.HomeRegion == "" || lease.OwnerID == "" || lease.Epoch <= 0 {
		return ErrLeaseLost
	}
	r, err := tx.Query(`SELECT owner_id,epoch::text,(lease_expires_at > clock_timestamp())::text
FROM region_leases WHERE home_region=$1 FOR SHARE`, lease.HomeRegion)
	if err != nil {
		return err
	}
	if r.RowCount() != 1 {
		return ErrLeaseLost
	}
	if col(r, 0, 0) != lease.OwnerID {
		return ErrLeaseLost
	}
	epoch, _ := strconv.ParseInt(col(r, 0, 1), 10, 64)
	if epoch != lease.Epoch {
		return ErrFenced
	}
	valid, parseErr := strconv.ParseBool(col(r, 0, 2))
	if parseErr != nil || !valid {
		return ErrLeaseLost
	}
	return nil
}

const aggregateSelect = `SELECT o.order_id,o.item_id,o.unit_id,o.user_id,o.home_region,o.status,o.inventory_disposition,
o.version::text,o.fencing_epoch::text,extract(epoch from o.created_at)::text,extract(epoch from o.updated_at)::text,
p.status,p.psp_charge_id,p.amount_minor::text,p.attempts::text,p.last_attempt_fencing_epoch::text,extract(epoch from p.updated_at)::text
FROM orders o JOIN payments p USING(order_id) WHERE o.order_id=$1`

func getAggregateTx(tx *pgwire.Tx, id string, lock bool) (Aggregate, error) {
	q := aggregateSelect
	if lock {
		q += " FOR UPDATE OF o,p"
	}
	r, err := tx.Query(q, id)
	if err != nil {
		return Aggregate{}, err
	}
	return parseAggregate(r)
}
func parseAggregate(r *pgwire.Result) (Aggregate, error) {
	if r == nil || r.RowCount() != 1 {
		return Aggregate{}, ErrOrderNotFound
	}
	row := r.Rows[0]
	get := func(i int) string {
		if i >= len(row) || row[i] == nil {
			return ""
		}
		return *row[i]
	}
	version, _ := strconv.ParseInt(get(7), 10, 64)
	epoch, _ := strconv.ParseInt(get(8), 10, 64)
	amount, _ := strconv.ParseInt(get(13), 10, 64)
	attempts, _ := strconv.Atoi(get(14))
	payEpoch, _ := strconv.ParseInt(get(15), 10, 64)
	o := Order{ID: get(0), ItemID: get(1), UnitID: get(2), UserID: get(3), HomeRegion: get(4), Status: OrderStatus(get(5)), InventoryDisposition: InventoryDisposition(get(6)), Version: version, FencingEpoch: epoch, CreatedAt: parsePGTime(get(9)), UpdatedAt: parsePGTime(get(10))}
	p := Payment{OrderID: get(0), Status: PaymentStatus(get(11)), PSPChargeID: get(12), AmountMinor: amount, Attempts: attempts, LastAttemptFencingEpoch: payEpoch, UpdatedAt: parsePGTime(get(16))}
	return Aggregate{Order: o, Payment: p}, nil
}
func (r *PostgresRepository) Get(ctx context.Context, id string) (Aggregate, error) {
	res, err := r.DB.Query(ctx, aggregateSelect, id)
	if err != nil {
		return Aggregate{}, err
	}
	return parseAggregate(res)
}

func (r *PostgresRepository) mutate(ctx context.Context, lease LeaseToken, id string, requireSync bool, mutate func(*Order, *Payment) error) (Aggregate, error) {
	var out Aggregate
	err := r.DB.WithTx(ctx, func(tx *pgwire.Tx) error {
		if err := verifyLease(tx, lease); err != nil {
			return err
		}
		commitMode := "local"
		if requireSync {
			commitMode = "on"
		}
		if _, err := tx.Exec("SET LOCAL synchronous_commit = '" + commitMode + "'"); err != nil {
			return err
		}
		a, err := getAggregateTx(tx, id, true)
		if err != nil {
			return err
		}
		oldO, oldP := a.Order, a.Payment
		if err := mutate(&a.Order, &a.Payment); err != nil {
			return err
		}
		if a.Order == oldO && a.Payment == oldP {
			out = a
			return nil
		}
		a.Order.Version++ // one aggregate-version increment per committed transition
		a.Order.FencingEpoch = lease.Epoch
		// All changes in one aggregate transition receive the same DB timestamp.
		updated, err := tx.Query(`UPDATE orders SET status=$1,inventory_disposition=$2,version=$3,fencing_epoch=$4,updated_at=clock_timestamp()
WHERE order_id=$5 AND version=$6 RETURNING extract(epoch from updated_at)::text`, string(a.Order.Status), string(a.Order.InventoryDisposition), strconv.FormatInt(a.Order.Version, 10), strconv.FormatInt(a.Order.FencingEpoch, 10), id, strconv.FormatInt(oldO.Version, 10))
		if err != nil {
			return err
		}
		if updated.RowCount() != 1 {
			return fmt.Errorf("order CAS failed: %w", ErrInvalidTransition)
		}
		a.Order.UpdatedAt = parsePGTime(col(updated, 0, 0))
		if _, err := tx.Exec(`UPDATE payments SET status=$1,psp_charge_id=NULLIF($2,''),attempts=$3,last_attempt_fencing_epoch=$4,updated_at=to_timestamp($5::double precision)
WHERE order_id=$6`, string(a.Payment.Status), a.Payment.PSPChargeID, strconv.Itoa(a.Payment.Attempts), strconv.FormatInt(a.Payment.LastAttemptFencingEpoch, 10), strconv.FormatFloat(float64(a.Order.UpdatedAt.UnixNano())/1e9, 'f', 9, 64), id); err != nil {
			return err
		}
		a.Payment.UpdatedAt = a.Order.UpdatedAt
		out = a
		return nil
	})
	return out, err
}
func (r *PostgresRepository) MarkAuthorized(ctx context.Context, l LeaseToken, id, charge string) (Aggregate, error) {
	return r.mutate(ctx, l, id, false, func(o *Order, p *Payment) error {
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
		p.LastAttemptFencingEpoch = l.Epoch
		return nil
	})
}
func (r *PostgresRepository) MarkUnknown(ctx context.Context, l LeaseToken, id string) (Aggregate, error) {
	return r.mutate(ctx, l, id, false, func(o *Order, p *Payment) error {
		if o.Status == OrderCancelled || o.Status == OrderConfirmed || p.Status == PaymentCaptured {
			return ErrInvalidTransition
		}
		if p.Status == PaymentUnknown {
			return nil
		}
		p.Status = PaymentUnknown
		p.LastAttemptFencingEpoch = l.Epoch
		return nil
	})
}
func (r *PostgresRepository) CancelDeclined(ctx context.Context, l LeaseToken, id string, voided bool) (Aggregate, error) {
	return r.mutate(ctx, l, id, false, func(o *Order, p *Payment) error {
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
func (r *PostgresRepository) MarkPSPOutcome(ctx context.Context, l LeaseToken, id string, status PaymentStatus, charge string) (Aggregate, error) {
	if status != PaymentFailed && status != PaymentVoided {
		return Aggregate{}, ErrInvalidTransition
	}
	return r.mutate(ctx, l, id, false, func(o *Order, p *Payment) error {
		if o.Status == OrderConfirmed {
			return ErrInvalidTransition
		}
		if o.Status != OrderCancelled {
			o.Status = OrderCancelled
			if o.InventoryDisposition == DispositionNone {
				o.InventoryDisposition = DispositionRelease
			}
		}
		if p.Status == status && p.PSPChargeID == charge {
			return nil
		}
		p.Status = status
		p.PSPChargeID = charge
		p.LastAttemptFencingEpoch = l.Epoch
		return nil
	})
}

func (r *PostgresRepository) Confirm(ctx context.Context, l LeaseToken, id string) (Aggregate, error) {
	return r.mutate(ctx, l, id, true, func(o *Order, p *Payment) error {
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
func (r *PostgresRepository) MarkCaptured(ctx context.Context, l LeaseToken, id, charge string) (Aggregate, error) {
	return r.mutate(ctx, l, id, false, func(o *Order, p *Payment) error {
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
func (r *PostgresRepository) PendingCapture(ctx context.Context, limit int) ([]Aggregate, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := r.DB.Query(ctx, `SELECT o.order_id,o.item_id,o.unit_id,o.user_id,o.home_region,o.status,o.inventory_disposition,
o.version::text,o.fencing_epoch::text,extract(epoch from o.created_at)::text,extract(epoch from o.updated_at)::text,
p.status,p.psp_charge_id,p.amount_minor::text,p.attempts::text,p.last_attempt_fencing_epoch::text,extract(epoch from p.updated_at)::text
FROM orders o JOIN payments p USING(order_id) WHERE o.status='CONFIRMED' AND p.status='AUTHORIZED'
ORDER BY o.updated_at LIMIT $1`, strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	out := make([]Aggregate, 0, res.RowCount())
	for i := 0; i < res.RowCount(); i++ {
		a, e := parseAggregateRow(res, i)
		if e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, nil
}

func (r *PostgresRepository) RenewLease(ctx context.Context, l LeaseToken, d time.Duration) (Lease, error) {
	var out Lease
	res, err := r.DB.Query(ctx, `UPDATE region_leases SET lease_expires_at=clock_timestamp()+($4::bigint*interval '1 microsecond'),updated_at=clock_timestamp()
WHERE home_region=$1 AND owner_id=$2 AND epoch=$3 AND lease_expires_at>clock_timestamp()
RETURNING home_region,owner_id,epoch::text,extract(epoch from lease_expires_at)::text`, l.HomeRegion, l.OwnerID, strconv.FormatInt(l.Epoch, 10), strconv.FormatInt(d.Microseconds(), 10))
	if err != nil {
		return out, err
	}
	if res.RowCount() != 1 {
		return out, ErrLeaseLost
	}
	out = parseLease(res, 0)
	return out, nil
}
func (r *PostgresRepository) TakeoverLease(ctx context.Context, home, newOwner string, oldPrimaryFenced bool, d time.Duration) (Lease, error) {
	var out Lease
	if !oldPrimaryFenced {
		return out, fmt.Errorf("takeover refused: old primary is not unreachable or infrastructure-fenced")
	}
	err := r.DB.WithTx(ctx, func(tx *pgwire.Tx) error {
		res, err := tx.Query(`SELECT home_region,owner_id,epoch::text FROM region_leases WHERE home_region=$1 FOR UPDATE`, home)
		if err != nil {
			return err
		}
		if res.RowCount() != 1 {
			return ErrLeaseLost
		}
		epoch, _ := strconv.ParseInt(col(res, 0, 2), 10, 64)
		updated, err := tx.Query(`UPDATE region_leases SET owner_id=$2,epoch=epoch+1,lease_expires_at=clock_timestamp()+($3::bigint*interval '1 microsecond'),updated_at=clock_timestamp()
WHERE home_region=$1 RETURNING home_region,owner_id,epoch::text,extract(epoch from lease_expires_at)::text`, home, newOwner, strconv.FormatInt(d.Microseconds(), 10))
		if err != nil {
			return err
		}
		if updated.RowCount() != 1 {
			return ErrLeaseLost
		}
		out = parseLease(updated, 0)
		if out.Epoch != epoch+1 {
			return ErrFenced
		}
		return nil
	})
	return out, err
}
func (r *PostgresRepository) Lease(ctx context.Context, home string) (Lease, error) {
	res, err := r.DB.Query(ctx, `SELECT home_region,owner_id,epoch::text,extract(epoch from lease_expires_at)::text FROM region_leases WHERE home_region=$1`, home)
	if err != nil {
		return Lease{}, err
	}
	if res.RowCount() != 1 {
		return Lease{}, ErrLeaseLost
	}
	return parseLease(res, 0), nil
}
func parseLease(r *pgwire.Result, row int) Lease {
	epoch, _ := strconv.ParseInt(col(r, row, 2), 10, 64)
	return Lease{HomeRegion: col(r, row, 0), OwnerID: col(r, row, 1), Epoch: epoch, ExpiresAt: parsePGTime(col(r, row, 3))}
}
func col(r *pgwire.Result, row, col int) string {
	if r == nil || row < 0 || row >= len(r.Rows) || col < 0 || col >= len(r.Rows[row]) || r.Rows[row][col] == nil {
		return ""
	}
	return *r.Rows[row][col]
}
func parsePGTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return time.Unix(0, int64(f*1e9)).UTC()
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999-07", "2006-01-02 15:04:05-07"} {
		if t, e := time.Parse(layout, s); e == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
func parseAggregateRow(r *pgwire.Result, row int) (Aggregate, error) {
	if row < 0 || row >= r.RowCount() {
		return Aggregate{}, ErrOrderNotFound
	}
	single := &pgwire.Result{Columns: r.Columns, Rows: [][]*string{r.Rows[row]}}
	return parseAggregate(single)
}

var _ Repository = (*PostgresRepository)(nil)
var _ LeaseRepository = (*PostgresRepository)(nil)
