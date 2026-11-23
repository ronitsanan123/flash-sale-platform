package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"flashsale-platform/prototype/internal/app"
)

func main() {
	const item = "demo-item"
	const units = 500
	const waiters = 20000
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	engine, err := app.NewDemoEngine(app.Config{HomeRegion: "demo-region", WriterID: "demo-region-primary", LeaseEpoch: 1, QueueTokenTTL: 5 * time.Minute, HoldTTL: 30 * time.Second, PayingTTL: 30 * time.Second, PSPAmountMinor: 2500})
	if err != nil {
		log.Fatal(err)
	}
	seed := make([]string, units)
	for i := range seed {
		seed[i] = fmt.Sprintf("seat-%05d", i+1)
	}
	if err := engine.SeedItem(ctx, item, seed); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < waiters; i++ {
		if _, err := engine.Join(ctx, item, fmt.Sprintf("user-%06d", i+1)); err != nil {
			log.Fatal(err)
		}
	}
	engine.AdvanceQueue(item, units)
	tokens := make([]string, 0, units)
	admitted := 0
	for i := 0; i < waiters; i++ {
		user := fmt.Sprintf("user-%06d", i+1)
		st, err := engine.Poll(ctx, item, user)
		if err != nil {
			log.Fatal(err)
		}
		if st.Admitted && st.AdmissionToken != "" {
			admitted++
			tokens = append(tokens, st.AdmissionToken)
		}
	}
	reserved := 0
	for _, raw := range tokens {
		if _, err := engine.Reserve(ctx, raw, item); err != nil {
			log.Fatalf("reserve failed after %d: %v", reserved, err)
		}
		reserved++
	}
	// Each admitted unit is checked out, then processed through the same relay,
	// consumer, order-first payment and confirmation state machine as the API.
	for i := 0; i < reserved; i++ {
		user := fmt.Sprintf("user-%06d", i+1)
		claims, err := engine.Signer.Verify(tokens[i], "flash-sale-api", "admission")
		if err != nil {
			log.Fatal(err)
		}
		if _, err := engine.Checkout(ctx, item, claims.JTI, user); err != nil {
			log.Fatal(err)
		}
	}
	if err := engine.Drain(ctx, 20000); err != nil {
		log.Fatal(err)
	}
	if err := engine.CaptureOnce(ctx, units+1); err != nil {
		log.Fatal(err)
	}
	available, held, paying, confirmed, forfeited, total, err := engine.Inventory.Availability(ctx, item)
	if err != nil {
		log.Fatal(err)
	}
	stats, ok := engine.PSP.(interface{ Stats() (int, int) })
	if !ok {
		log.Fatal("demo PSP adapter does not expose statistics")
	}
	charges, events := stats.Stats()
	if confirmed != units || charges != units || available != 0 || held != 0 || paying != 0 || total != units || forfeited != 0 {
		log.Fatalf("invariants failed: available=%d held=%d paying=%d confirmed=%d forfeited=%d total=%d charges=%d", available, held, paying, confirmed, forfeited, total, charges)
	}
	// Verify every order/payment is terminal and each PSP effect is captured.
	captured := 0
	for i, raw := range tokens {
		claims, err := engine.Signer.Verify(raw, "flash-sale-api", "admission")
		if err != nil {
			log.Fatal(err)
		}
		agg, err := engine.Repo.Get(ctx, claims.JTI)
		if err != nil || agg.Order.Status != "CONFIRMED" || agg.Payment.Status != "CAPTURED" {
			log.Fatalf("order %d not terminal: %+v err=%v", i, agg, err)
		}
		charge, err := engine.PSP.Get(claims.JTI)
		if err != nil || charge.Status != "CAPTURED" {
			log.Fatalf("PSP effect %d not captured: %+v err=%v", i, charge, err)
		}
		captured++
	}
	fmt.Printf("demo=PASS waiters=%d admitted=%d reserved=%d confirmed=%d captured=%d psp_charges=%d psp_events=%d duplicate_units=0 orphan_authorizations=0\n", waiters, admitted, reserved, confirmed, captured, charges, events)
	fmt.Println("NOTE: in-memory correctness demonstration only; not a 500K-RPS or Redis/PostgreSQL/Redpanda benchmark")
}
