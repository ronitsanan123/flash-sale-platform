package admission

import (
	"context"
	"flashsale-platform/prototype/internal/token"
	"testing"
	"time"
)

func testSigner(t *testing.T) *token.Signer {
	t.Helper()
	s, e := token.NewSigner("k1", map[string][]byte{"k1": []byte("01234567890123456789012345678901")})
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func healthy(now time.Time) Health {
	return Health{PSPHealthy: true, PSPCapacityPerSecond: 100, StandbyACKHealthy: true, StandbyACKAt: now, ControlPlaneHealthy: true, Now: now}
}
func TestControllerSeparatesPausedFromSoldOut(t *testing.T) {
	now := time.Now()
	c := NewController(time.Second, 2*time.Second)
	w := c.Evaluate("i", 0, 100, Stock{Known: true, Available: 0, Held: 5, Total: 10}, now, healthy(now))
	if w.Inventory != InventoryTemporary || w.Gate != GateActive {
		t.Fatalf("got %+v", w)
	}
	h := healthy(now)
	h.StandbyACKHealthy = false
	w = c.Evaluate("i", 0, 100, Stock{Known: true, Available: 5, Total: 10}, now, h)
	if w.Gate != GatePaused || w.Inventory != InventoryOpen {
		t.Fatalf("got %+v", w)
	}
	w = c.Evaluate("i", 0, 100, Stock{Known: true, Confirmed: 10, Total: 10}, now, healthy(now))
	if w.Inventory != InventorySoldOut || w.Gate != GatePaused {
		t.Fatalf("got %+v", w)
	}
}
func TestQueuePausePreservesPlaceAndIssuesNoToken(t *testing.T) {
	ctx := context.Background()
	q := NewQueue(testSigner(t), time.Minute)
	e, err := q.Join(ctx, "i", "u")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	q.PublishWatermark(Watermark{Item: "i", Version: 1, Head: 10, Rate: 10, Inventory: InventoryOpen, Gate: GatePaused, UpdatedAt: now})
	st, err := q.Poll(ctx, "i", "u")
	if err != nil {
		t.Fatal(err)
	}
	if st.Position != e.Position || st.Admitted || st.AdmissionToken != "" {
		t.Fatalf("unexpected status %+v", st)
	}
	w := Watermark{Item: "i", Version: 2, Head: 10, Inventory: InventoryOpen, Gate: GateActive, UpdatedAt: now}
	q.PublishWatermark(w)
	st, err = q.Poll(ctx, "i", "u")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Admitted || st.AdmissionToken == "" {
		t.Fatalf("expected admission: %+v", st)
	}
}
