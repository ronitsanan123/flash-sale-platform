package psp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAuthorizeIdempotencyLostResponseAndVoidTombstone(t *testing.T) {
	m := NewMock()
	m.SetFaults(Faults{LostResponseRate: 1})
	srv := httptest.NewServer(m)
	defer srv.Close()
	client := &Client{BaseURL: srv.URL}
	req := Request{AmountMinor: 100, Currency: "USD", Metadata: Metadata{OrderID: "o1", ItemID: "i", UnitID: "u", UserID: "x", HomeRegion: "r1", FencingEpoch: 1}}
	if _, err := client.Authorize("o1", req); err == nil {
		t.Fatal("expected simulated lost response")
	}
	m.SetFaults(Faults{})
	c, err := client.Get("o1")
	if err != nil {
		t.Fatal(err)
	}
	if c.Status != "AUTHORIZED" {
		t.Fatalf("status %s", c.Status)
	}
	c2, err := client.Authorize("o1", req)
	if err != nil {
		t.Fatal(err)
	}
	if c2.ChargeID != c.ChargeID {
		t.Fatal("duplicate PSP effect")
	}
	if _, err := client.Void("o1"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Authorize("o1", req); err == nil {
		t.Fatal("authorization after void tombstone must fail")
	}
}
func TestLedgerDiscoverUnknownKeyAndPagination(t *testing.T) {
	m := NewMock()
	now := time.Now().UTC()
	m.now = func() time.Time { return now }
	for i := 0; i < 5; i++ {
		_, _, err := m.Authorize("order-"+string(rune('a'+i)), Request{AmountMinor: 1, Currency: "USD", Metadata: Metadata{OrderID: "order-" + string(rune('a'+i)), HomeRegion: "r1"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	from := now.Add(-time.Minute)
	to := now.Add(time.Minute)
	p1, err := m.Ledger(from, to, "r1", "AUTHORIZATION", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Entries) != 2 || p1.NextCursor == "" {
		t.Fatalf("page1 %+v", p1)
	}
	seen := map[string]bool{}
	for _, e := range p1.Entries {
		seen[e.IdempotencyKey] = true
	}
	p2, err := m.Ledger(from, to, "r1", "AUTHORIZATION", p1.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range p2.Entries {
		if seen[e.IdempotencyKey] {
			t.Fatal("duplicate page entry")
		}
		seen[e.IdempotencyKey] = true
	}
	p3, err := m.Ledger(from, to, "r1", "AUTHORIZATION", p2.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range p3.Entries {
		if seen[e.IdempotencyKey] {
			t.Fatal("duplicate page entry")
		}
		seen[e.IdempotencyKey] = true
	}
	if len(seen) != 5 {
		t.Fatalf("found %d, want 5", len(seen))
	}
	if p3.CompleteThrough.Before(now) {
		t.Fatalf("unexpected completeness watermark %v", p3.CompleteThrough)
	}
}
func TestLedgerDelayDoesNotClaimCompleteFuture(t *testing.T) {
	m := NewMock()
	now := time.Now().UTC()
	m.now = func() time.Time { return now }
	m.SetFaults(Faults{LedgerIndexDelayMS: 5000})
	_, _, err := m.Authorize("o", Request{AmountMinor: 10, Currency: "USD", Metadata: Metadata{OrderID: "o", HomeRegion: "r"}})
	if err != nil {
		t.Fatal(err)
	}
	page, err := m.Ledger(now.Add(-time.Minute), now.Add(time.Minute), "r", "AUTHORIZATION", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 0 {
		t.Fatal("event should not be indexed yet")
	}
	if !page.CompleteThrough.Before(now) {
		t.Fatalf("watermark %v should trail now %v", page.CompleteThrough, now)
	}
}
func TestHTTPAuthorizeAndLedger(t *testing.T) {
	m := NewMock()
	srv := httptest.NewServer(m)
	defer srv.Close()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/authorize", jsonReader(t, Request{AmountMinor: 1, Currency: "USD", Metadata: Metadata{OrderID: "o", HomeRegion: "r"}}))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "o")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
}

func jsonReader(t *testing.T, v any) *strings.Reader {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return strings.NewReader(string(b))
}
