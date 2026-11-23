package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"flashsale-platform/prototype/internal/admission"
	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/psp"
	"time"
)

func TestHTTPFullCheckoutFlowAndImmediateStatus(t *testing.T) {
	engine, err := NewDemoEngine(Config{HomeRegion: "region-a", WriterID: "region-a-primary", LeaseEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := engine.SeedItem(ctx, "concert", []string{"seat-1", "seat-2"}); err != nil {
		t.Fatal(err)
	}
	server := &HTTPServer{Engine: engine, AdminToken: "admin-test"}
	h := server.Handler()
	call := func(method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
		var b bytes.Buffer
		if body != nil {
			_ = json.NewEncoder(&b).Encode(body)
		}
		req := httptest.NewRequest(method, path, &b)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	join := call(http.MethodPost, "/queue/concert/join", map[string]string{"user_id": "alice"}, nil)
	if join.Code != 200 {
		t.Fatalf("join %d %s", join.Code, join.Body.String())
	}
	engine.AdvanceQueue("concert", 10)
	poll := call(http.MethodGet, "/queue/status?item=concert&user=alice", nil, nil)
	if poll.Code != 200 {
		t.Fatalf("poll %d %s", poll.Code, poll.Body.String())
	}
	var status map[string]any
	if err := json.Unmarshal(poll.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	admissionToken, _ := status["admission_token"].(string)
	if admissionToken == "" {
		t.Fatalf("no admission token: %v", status)
	}
	reserve := call(http.MethodPost, "/items/concert/reserve", nil, map[string]string{"Authorization": "Bearer " + admissionToken})
	if reserve.Code != 201 {
		t.Fatalf("reserve %d %s", reserve.Code, reserve.Body.String())
	}
	var hold map[string]any
	_ = json.Unmarshal(reserve.Body.Bytes(), &hold)
	holdID := hold["id"].(string)
	checkout := call(http.MethodPost, "/holds/"+holdID+"/checkout", nil, map[string]string{"X-Item-ID": "concert", "X-User-ID": "alice"})
	if checkout.Code != 202 {
		t.Fatalf("checkout %d %s", checkout.Code, checkout.Body.String())
	}
	immediate := call(http.MethodGet, "/orders/"+holdID+"?item=concert", nil, map[string]string{"X-User-ID": "alice"})
	if immediate.Code != 200 {
		t.Fatalf("immediate status %d %s", immediate.Code, immediate.Body.String())
	}
	var immediateBody map[string]any
	_ = json.Unmarshal(immediate.Body.Bytes(), &immediateBody)
	if immediateBody["status"] != "PROCESSING" {
		t.Fatalf("expected pre-order PROCESSING, got %v", immediateBody)
	}
	if err := engine.Drain(ctx, 100); err != nil {
		t.Fatal(err)
	}
	final := call(http.MethodGet, "/orders/"+holdID+"?item=concert", nil, map[string]string{"X-User-ID": "alice"})
	if final.Code != 200 {
		t.Fatalf("final status %d %s", final.Code, final.Body.String())
	}
	var finalBody map[string]any
	_ = json.Unmarshal(final.Body.Bytes(), &finalBody)
	if finalBody["status"] != "CONFIRMED" {
		t.Fatalf("expected CONFIRMED, got %v", finalBody)
	}
	if err := engine.CaptureOnce(ctx, 10); err != nil {
		t.Fatal(err)
	}
	captureStatus := call(http.MethodGet, "/orders/"+holdID+"?item=concert", nil, map[string]string{"X-User-ID": "alice"})
	_ = json.Unmarshal(captureStatus.Body.Bytes(), &finalBody)
	if finalBody["payment_status"] != "CAPTURED" {
		t.Fatalf("expected CAPTURED, got %v", finalBody)
	}
}

func TestPausedQueuePreservesPlaceAndDoesNotIssueToken(t *testing.T) {
	engine, err := NewDemoEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := engine.SeedItem(ctx, "i", []string{"u"}); err != nil {
		t.Fatal(err)
	}
	entry, err := engine.Join(ctx, "i", "alice")
	if err != nil {
		t.Fatal(err)
	}
	engine.AdvanceQueue("i", 10)
	engine.SetPaused("i", true)
	st, err := engine.Poll(ctx, "i", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if st.Position != entry.Position || st.Admitted || st.AdmissionToken != "" {
		t.Fatalf("paused status %+v", st)
	}
}

func TestStandbyUnavailablePausesNewAdmissions(t *testing.T) {
	engine, err := NewDemoEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := engine.SeedItem(ctx, "i", []string{"u"}); err != nil {
		t.Fatal(err)
	}
	_, _ = engine.Join(ctx, "i", "alice")
	engine.AdvanceQueue("i", 10)
	engine.SetStandbyAvailable(false)
	st, err := engine.Poll(ctx, "i", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if st.Admitted || st.AdmissionToken != "" || st.Watermark.Gate != "PAUSED" {
		t.Fatalf("got %+v", st)
	}
}

func TestAvailabilityCache(t *testing.T) {
	engine, err := NewDemoEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = engine.SeedItem(ctx, "i", []string{"u1", "u2"})
	first, err := engine.Availability(ctx, "i")
	if err != nil {
		t.Fatal(err)
	}
	if first["available"] != 2 {
		t.Fatal(first)
	}
	_, _ = engine.Inventory.Reserve(ctx, "i", "h", "u", "")
	cached, _ := engine.Availability(ctx, "i")
	if cached["available"] != 2 {
		t.Fatal("cache should remain for one second")
	}
	time.Sleep(time.Second + 20*time.Millisecond)
	fresh, _ := engine.Availability(ctx, "i")
	if fresh["available"] != 1 {
		t.Fatal(fresh)
	}
}

type countingInventory struct {
	inventory.Store
	count   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (c *countingInventory) Availability(ctx context.Context, item string) (int, int, int, int, int, int, error) {
	c.count.Add(1)
	select {
	case c.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return 0, 0, 0, 0, 0, 0, ctx.Err()
	case <-c.release:
	}
	return c.Store.Availability(ctx, item)
}

func TestAvailabilityCoalescesConcurrentReads(t *testing.T) {
	engine, err := NewDemoEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := engine.SeedItem(ctx, "coalesce", []string{"u1", "u2"}); err != nil {
		t.Fatal(err)
	}
	wrapped := &countingInventory{Store: engine.Inventory, entered: make(chan struct{}, 1), release: make(chan struct{})}
	engine.Inventory = wrapped
	var wg sync.WaitGroup
	results := make(chan error, 100)
	wg.Add(1)
	go func() { defer wg.Done(); _, e := engine.Availability(ctx, "coalesce"); results <- e }()
	<-wrapped.entered
	for i := 0; i < 99; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := engine.Availability(ctx, "coalesce"); results <- e }()
	}
	time.Sleep(25 * time.Millisecond)
	close(wrapped.release)
	wg.Wait()
	close(results)
	for e := range results {
		if e != nil {
			t.Fatal(e)
		}
	}
	if got := wrapped.count.Load(); got != 1 {
		t.Fatalf("backend reads=%d, want one coalesced read", got)
	}
}

func TestAdmissionTickDoesNotAdvanceWhileWorkersAreUnhealthy(t *testing.T) {
	engine, err := NewDemoEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := engine.SeedItem(ctx, "worker-gated", []string{"u1"}); err != nil {
		t.Fatal(err)
	}
	entry, err := engine.Join(ctx, "worker-gated", "alice")
	if err != nil {
		t.Fatal(err)
	}
	engine.SetHealth(admission.Health{PSPHealthy: true, PSPCapacityPerSecond: 100, StandbyACKHealthy: true, StandbyACKAt: time.Now(), ControlPlaneHealthy: true, WorkersMonitored: true, WorkersHealthy: false, WorkersSignalAt: time.Now(), Now: time.Now()})
	if err := engine.AdmissionTick(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := engine.Poll(ctx, "worker-gated", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if status.Admitted || status.AdmissionToken != "" || !status.Paused {
		t.Fatalf("worker outage must pause admissions: %+v", status)
	}
	if status.Position != entry.Position || status.Watermark.Head != 0 {
		t.Fatalf("worker outage advanced queue position: %+v", status)
	}
}

func TestPSPWebhookHTTPAuthenticatesAndRoutesRecoveryPending(t *testing.T) {
	engine, err := NewDemoEngine(Config{HomeRegion: "region-a", WriterID: "region-a-primary", LeaseEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("unit-test-webhook-secret-with-at-least-32-bytes")
	h := (&HTTPServer{Engine: engine, WebhookSecret: secret, WebhookMaxSkew: time.Minute}).Handler()
	charge := psp.Charge{
		EventID: "evt-missing-order", EventType: "AUTHORIZATION", ChargeID: "ch-1",
		IdempotencyKey: "missing-order", Status: "AUTHORIZED", AmountMinor: 1000, Currency: "USD",
		Metadata:  psp.Metadata{OrderID: "missing-order", ItemID: "item-a", UnitID: "unit-a", UserID: "user-a", HomeRegion: "region-a", FencingEpoch: 1},
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	body, err := json.Marshal(charge)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	bad := httptest.NewRequest(http.MethodPost, "/webhooks/psp", bytes.NewReader(body))
	bad.Header.Set("X-PSP-Timestamp", timestamp)
	bad.Header.Set("X-PSP-Signature", "invalid")
	badResp := httptest.NewRecorder()
	h.ServeHTTP(badResp, bad)
	if badResp.Code != http.StatusUnauthorized {
		t.Fatalf("invalid signature status=%d body=%s", badResp.Code, badResp.Body.String())
	}

	good := httptest.NewRequest(http.MethodPost, "/webhooks/psp", bytes.NewReader(body))
	good.Header.Set("X-PSP-Timestamp", timestamp)
	good.Header.Set("X-PSP-Signature", psp.SignWebhook(secret, timestamp, body))
	goodResp := httptest.NewRecorder()
	h.ServeHTTP(goodResp, good)
	if goodResp.Code != http.StatusAccepted {
		t.Fatalf("unknown order should be queued for recovery, status=%d body=%s", goodResp.Code, goodResp.Body.String())
	}
}
