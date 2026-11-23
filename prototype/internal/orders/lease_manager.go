package orders

import (
	"context"
	"fmt"
	"time"
)

// LeaseManager renews the logical regional-writer lease independently of
// worker requests. It never holds the region lease transaction across a PSP
// network call. Any uncertain renewal self-fences the process.
type LeaseManager struct {
	Repo        LeaseRepository
	Token       LeaseToken
	Duration    time.Duration
	RenewalLead time.Duration
	OnSelfFence func(error)
}

func (m *LeaseManager) Run(ctx context.Context) error {
	if m.Repo == nil || m.Token.HomeRegion == "" || m.Token.OwnerID == "" || m.Token.Epoch <= 0 {
		return fmt.Errorf("lease manager configuration incomplete")
	}
	d := m.Duration
	if d <= 0 {
		d = 15 * time.Second
	}
	lead := m.RenewalLead
	if lead <= 0 || lead >= d {
		lead = d / 3
	}
	lease, err := m.Repo.Lease(ctx, m.Token.HomeRegion)
	if err != nil {
		return m.selfFence(err)
	}
	if lease.OwnerID != m.Token.OwnerID || lease.Epoch != m.Token.Epoch || !time.Now().Before(lease.ExpiresAt) {
		return m.selfFence(ErrLeaseLost)
	}
	for {
		delay := time.Until(lease.ExpiresAt.Add(-lead))
		if delay < 0 {
			return m.selfFence(ErrLeaseLost)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		// owner_id is the logical regional writer identity, shared by all
		// stateless workers in the active region; it is never a pod hostname.
		lease, err = m.Repo.RenewLease(ctx, m.Token, d)
		if err != nil {
			return m.selfFence(err)
		}
	}
}

func (m *LeaseManager) selfFence(err error) error {
	if m.OnSelfFence != nil {
		m.OnSelfFence(err)
	}
	return fmt.Errorf("lease self-fenced: %w", err)
}
