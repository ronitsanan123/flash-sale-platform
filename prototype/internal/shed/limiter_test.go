package shed

import (
	"context"
	"errors"
	"testing"
)

func TestAvailabilityCannotConsumeReservedCheckoutCapacity(t *testing.T) {
	l, err := New(Config{Total: 4, Checkout: 2, Reserve: 2, Availability: 2, NonEssential: 1})
	if err != nil {
		t.Fatal(err)
	}
	p1, err := l.Acquire(context.Background(), Availability)
	if err != nil {
		t.Fatal(err)
	}
	defer p1.Release()
	p2, err := l.Acquire(context.Background(), Availability)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Release()
	if _, err := l.Acquire(context.Background(), Availability); !errors.Is(err, ErrShed) {
		t.Fatalf("expected availability shed, got %v", err)
	}
	c1, err := l.Acquire(context.Background(), Checkout)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Release()
	c2, err := l.Acquire(context.Background(), Checkout)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Release()
	if l.Active(Checkout) != 2 {
		t.Fatal(l.Active(Checkout))
	}
}
