package psp

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeOperations struct {
	failures atomic.Int64
	calls    atomic.Int64
}

func (f *fakeOperations) Authorize(string, Request) (Charge, error) {
	f.calls.Add(1)
	if f.failures.Load() > 0 {
		f.failures.Add(-1)
		return Charge{}, ErrUnknown
	}
	return Charge{Status: "AUTHORIZED"}, nil
}
func (f *fakeOperations) Get(string) (Charge, error) {
	f.calls.Add(1)
	return Charge{Status: "AUTHORIZED"}, nil
}
func (f *fakeOperations) Capture(string) (Charge, error) {
	f.calls.Add(1)
	return Charge{Status: "CAPTURED"}, nil
}
func (f *fakeOperations) Void(string) (Charge, error) {
	f.calls.Add(1)
	return Charge{Status: "VOIDED"}, nil
}

func TestGuardOpensAfterTransientFailuresAndRecovers(t *testing.T) {
	base := &fakeOperations{}
	base.failures.Store(2)
	g, err := NewGuardedClient(base, GuardConfig{RequestsPerSecond: 1000, Burst: 10, MaxConcurrent: 2, FailureThreshold: 2, OpenDuration: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = g.Authorize("a", Request{AmountMinor: 1, Metadata: Metadata{OrderID: "a"}})
	_, _ = g.Authorize("b", Request{AmountMinor: 1, Metadata: Metadata{OrderID: "b"}})
	if _, err = g.Authorize("c", Request{AmountMinor: 1, Metadata: Metadata{OrderID: "c"}}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected open breaker, got %v", err)
	}
	time.Sleep(25 * time.Millisecond)
	if _, err = g.Get("c"); err != nil {
		t.Fatalf("half-open probe did not recover: %v", err)
	}
	if !g.Snapshot().Healthy {
		t.Fatal("breaker should be closed")
	}
}

func TestGuardDoesNotOpenOnDefinitiveBusinessOutcome(t *testing.T) {
	base := &fakeOperations{}
	g, _ := NewGuardedClient(base, GuardConfig{RequestsPerSecond: 1000, Burst: 10, FailureThreshold: 1})
	// Use an adapter with a definitive decline.
	g.base = declineOperations{}
	_, err := g.Authorize("a", Request{AmountMinor: 1, Metadata: Metadata{OrderID: "a"}})
	if !errors.Is(err, ErrDeclined) {
		t.Fatalf("got %v", err)
	}
	if !g.Snapshot().Healthy {
		t.Fatal("business decline must not open breaker")
	}
}

type declineOperations struct{}

func (declineOperations) Authorize(string, Request) (Charge, error) { return Charge{}, ErrDeclined }
func (declineOperations) Get(string) (Charge, error)                { return Charge{}, ErrNotFound }
func (declineOperations) Capture(string) (Charge, error)            { return Charge{}, ErrDeclined }
func (declineOperations) Void(string) (Charge, error)               { return Charge{}, ErrVoided }
