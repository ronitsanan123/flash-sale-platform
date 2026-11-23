package inventory

import (
	"context"
	"time"
)

type HoldState string

const (
	HoldHeld      HoldState = "HELD"
	HoldPaying    HoldState = "PAYING"
	HoldConfirmed HoldState = "CONFIRMED"
	HoldReleased  HoldState = "RELEASED"
	HoldForfeited HoldState = "FORFEITED"
)

type Hold struct {
	ID       string    `json:"id"`
	Item     string    `json:"item_id"`
	Unit     string    `json:"unit_id"`
	User     string    `json:"user_id"`
	State    HoldState `json:"state"`
	Deadline time.Time `json:"deadline"`
}

type ForfeitProof string

const (
	ForfeitProofNotAttempted ForfeitProof = "NOT_ATTEMPTED"
	ForfeitProofPSPDeclined  ForfeitProof = "PSP_DECLINED"
	ForfeitProofPSPVoided    ForfeitProof = "PSP_VOIDED"
)

func ValidateForfeitProof(state HoldState, proof ForfeitProof) error {
	if state == HoldForfeited {
		return nil
	}
	if state == HoldHeld {
		if proof == ForfeitProofNotAttempted {
			return nil
		}
		return ErrUnsafeForfeit
	}
	if state != HoldPaying {
		return ErrUnsafeForfeit
	}
	switch proof {
	case ForfeitProofNotAttempted, ForfeitProofPSPDeclined, ForfeitProofPSPVoided:
		return nil
	default:
		return ErrUnsafeForfeit
	}
}

type Store interface {
	Seed(context.Context, string, []string) error
	Reserve(context.Context, string, string, string, string) (Hold, error)
	LockForPayment(context.Context, string, string) (Hold, error)
	Confirm(context.Context, string, string) error
	Release(context.Context, string, string) error
	ReleaseAfterPayment(context.Context, string, string, ForfeitProof) error
	Forfeit(context.Context, string, string, ForfeitProof) error
	Reap(context.Context, string, int, int) (released, overdue int, err error)
	Get(context.Context, string, string) (Hold, bool, error)
	Availability(context.Context, string) (available, held, paying, confirmed, forfeited, total int, err error)
	Events(context.Context, string, int, int) ([]Event, error)
}

// Event is the inventory outbox record. See Event fields in memory_store.go.
type Event struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	HoldID    string            `json:"hold_id,omitempty"`
	ItemID    string            `json:"item_id,omitempty"`
	UnitID    string            `json:"unit_id,omitempty"`
	UserID    string            `json:"user_id,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	Fields    map[string]string `json:"fields,omitempty"`
}
