package orders

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/psp"
)

var (
	ErrLedgerIncomplete = errors.New("PSP ledger is not complete through requested recovery window")
	ErrLedgerSnapshot   = errors.New("PSP ledger pagination changed snapshot")
	ErrRecoveryMetadata = errors.New("PSP ledger entry is missing required recovery metadata")
	ErrCapturedOrphan   = errors.New("PSP reports captured payment without durable confirmed order")
)

type LedgerCheckpoint struct {
	HomeRegion      string
	ScanFrom        time.Time
	ScanTo          time.Time
	SnapshotID      string
	CompleteThrough time.Time
	FencingEpoch    int64
	UpdatedAt       time.Time
}

type RecoveryLedger interface {
	ScanLedger(from, to time.Time, homeRegion, eventType, cursor string, limit int) (psp.LedgerPage, error)
	Get(idempotencyKey string) (psp.Charge, error)
	Void(idempotencyKey string) (psp.Charge, error)
}

type RecoveryRepository interface {
	LoadLedgerCheckpoint(context.Context, string) (LedgerCheckpoint, bool, error)
	SaveLedgerCheckpoint(context.Context, LeaseToken, LedgerCheckpoint) error
	HasDurableConfirmation(context.Context, string) (bool, error)
	RecordOrphanAuthorization(context.Context, LeaseToken, psp.Charge) error
	Get(context.Context, string) (Aggregate, error)
}

type RecoveryInventory interface {
	Confirm(context.Context, string, string) error
	ReleaseAfterPayment(context.Context, string, string, inventory.ForfeitProof) error
	Forfeit(context.Context, string, string, inventory.ForfeitProof) error
}

type RecoveryResult struct {
	ScannedEvents   int
	DiscoveredIDs   int
	Preserved       int
	Voided          int
	Declined        int
	From            time.Time
	To              time.Time
	CompleteThrough time.Time
	SnapshotID      string
}

type RecoveryScanner struct {
	PSP             RecoveryLedger
	Repo            RecoveryRepository
	Inventory       RecoveryInventory
	PageSize        int
	Overlap         time.Duration
	SettlementGrace time.Duration
}

// Run discovers PSP effects even when the source order ID and the asynchronous
// event-log record were both lost with the failed home region. The caller must
// have already fenced the old writer and acquired the promoted region lease.
// Admissions remain closed until this method and subsequent reconciliation pass.
func (s *RecoveryScanner) Run(ctx context.Context, lease LeaseToken, requestedFrom, failoverDetectedAt time.Time) (RecoveryResult, error) {
	var result RecoveryResult
	if s.PSP == nil || s.Repo == nil || lease.HomeRegion == "" || lease.OwnerID == "" || lease.Epoch <= 0 {
		return result, fmt.Errorf("recovery scanner dependencies and promoted lease are required")
	}
	if requestedFrom.IsZero() || failoverDetectedAt.IsZero() || s.SettlementGrace <= 0 {
		return result, fmt.Errorf("scan start, failover time and positive settlement grace are required")
	}
	pageSize := s.PageSize
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 100
	}
	checkpoint, exists, err := s.Repo.LoadLedgerCheckpoint(ctx, lease.HomeRegion)
	if err != nil {
		return result, err
	}
	from := requestedFrom
	if exists {
		if checkpoint.HomeRegion != lease.HomeRegion || checkpoint.FencingEpoch > lease.Epoch {
			return result, ErrFenced
		}
		resume := checkpoint.CompleteThrough.Add(-s.Overlap)
		if resume.After(from) {
			from = resume
		}
	}
	to := failoverDetectedAt.Add(s.SettlementGrace)
	if !to.After(from) {
		return result, fmt.Errorf("recovery scan window is empty")
	}
	result.From, result.To = from, to

	cursor := ""
	snapshotID := ""
	completeThrough := time.Time{}
	eventIDs := map[string]struct{}{}
	byKey := map[string]psp.Charge{}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		page, err := s.PSP.ScanLedger(from, to, lease.HomeRegion, "AUTHORIZATION", cursor, pageSize)
		if err != nil {
			return result, fmt.Errorf("read PSP ledger page: %w", err)
		}
		if page.SnapshotID == "" {
			return result, fmt.Errorf("PSP ledger page has no snapshot id")
		}
		if snapshotID == "" {
			snapshotID = page.SnapshotID
		} else if snapshotID != page.SnapshotID {
			return result, ErrLedgerSnapshot
		}
		if page.CompleteThrough.After(completeThrough) {
			completeThrough = page.CompleteThrough
		}
		for _, entry := range page.Entries {
			result.ScannedEvents++
			if entry.EventID != "" {
				if _, duplicate := eventIDs[entry.EventID]; duplicate {
					continue
				}
				eventIDs[entry.EventID] = struct{}{}
			}
			if entry.EventType != "AUTHORIZATION" || entry.IdempotencyKey == "" {
				continue
			}
			if entry.Metadata.OrderID != entry.IdempotencyKey || entry.Metadata.HomeRegion != lease.HomeRegion || entry.Metadata.ItemID == "" || entry.Metadata.UnitID == "" || entry.Metadata.UserID == "" || entry.Metadata.FencingEpoch <= 0 {
				return result, ErrRecoveryMetadata
			}
			prior, ok := byKey[entry.IdempotencyKey]
			if !ok || entry.UpdatedAt.After(prior.UpdatedAt) {
				byKey[entry.IdempotencyKey] = entry
			}
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			return result, fmt.Errorf("PSP ledger cursor did not advance")
		}
		cursor = page.NextCursor
	}
	result.SnapshotID = snapshotID
	result.CompleteThrough = completeThrough
	if completeThrough.Before(to) {
		return result, ErrLedgerIncomplete
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		confirmed, err := s.Repo.HasDurableConfirmation(ctx, key)
		if err != nil {
			return result, err
		}
		if confirmed {
			if s.Inventory != nil {
				a, getErr := s.Repo.Get(ctx, key)
				if getErr != nil {
					return result, fmt.Errorf("load durable confirmed order %s: %w", key, getErr)
				}
				if a.Order.Status != OrderConfirmed || a.Order.InventoryDisposition != DispositionSold {
					return result, fmt.Errorf("durability query and order aggregate disagree for %s", key)
				}
				if err := s.Inventory.Confirm(ctx, a.Order.ItemID, a.Order.ID); err != nil {
					return result, fmt.Errorf("repair Redis confirmed unit %s: %w", key, err)
				}
			}
			result.Preserved++
			continue
		}
		current, err := s.PSP.Get(key)
		if err != nil {
			return result, fmt.Errorf("lookup discovered PSP key %s: %w", key, err)
		}
		switch current.Status {
		case "AUTHORIZED":
			current, err = s.PSP.Void(key)
			if err != nil {
				return result, fmt.Errorf("void orphan authorization %s: %w", key, err)
			}
			if current.Status != "VOIDED" {
				return result, fmt.Errorf("PSP did not confirm void for %s", key)
			}
			result.Voided++
		case "DECLINED", "VOIDED":
			result.Declined++
		case "CAPTURED":
			return result, fmt.Errorf("%w: %s", ErrCapturedOrphan, key)
		default:
			return result, fmt.Errorf("PSP outcome for %s remains ambiguous: %s", key, current.Status)
		}
		if err := s.Repo.RecordOrphanAuthorization(ctx, lease, current); err != nil {
			return result, fmt.Errorf("persist recovered orphan %s: %w", key, err)
		}
		if s.Inventory != nil {
			a, getErr := s.Repo.Get(ctx, key)
			if getErr != nil {
				return result, fmt.Errorf("load recovered orphan %s: %w", key, getErr)
			}
			var proof inventory.ForfeitProof
			if current.Status == "DECLINED" {
				proof = inventory.ForfeitProofPSPDeclined
			} else {
				proof = inventory.ForfeitProofPSPVoided
			}
			switch a.Order.InventoryDisposition {
			case DispositionRelease:
				if err := s.Inventory.ReleaseAfterPayment(ctx, a.Order.ItemID, a.Order.ID, proof); err != nil {
					return result, fmt.Errorf("repair released orphan unit %s: %w", key, err)
				}
			case DispositionForfeit:
				if err := s.Inventory.Forfeit(ctx, a.Order.ItemID, a.Order.ID, proof); err != nil {
					return result, fmt.Errorf("repair forfeited orphan unit %s: %w", key, err)
				}
			default:
				return result, fmt.Errorf("recovered orphan %s has unsafe inventory disposition %s", key, a.Order.InventoryDisposition)
			}
		}
		result.DiscoveredIDs++
	}
	checkpoint = LedgerCheckpoint{HomeRegion: lease.HomeRegion, ScanFrom: from, ScanTo: to, SnapshotID: snapshotID, CompleteThrough: completeThrough, FencingEpoch: lease.Epoch, UpdatedAt: time.Now().UTC()}
	if err := s.Repo.SaveLedgerCheckpoint(ctx, lease, checkpoint); err != nil {
		return result, fmt.Errorf("persist complete PSP ledger checkpoint: %w", err)
	}
	return result, nil
}
