package psp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound    = errors.New("PSP charge not found")
	ErrDeclined    = errors.New("PSP authorization declined")
	ErrVoided      = errors.New("PSP idempotency key is voided")
	ErrUnknown     = errors.New("PSP outcome unknown")
	ErrRateLimited = errors.New("PSP rate limited")
)

type Metadata struct {
	OrderID      string `json:"order_id"`
	ItemID       string `json:"item_id"`
	UnitID       string `json:"unit_id"`
	UserID       string `json:"user_id"`
	HomeRegion   string `json:"home_region"`
	FencingEpoch int64  `json:"fencing_epoch"`
}
type Request struct {
	AmountMinor int64    `json:"amount_minor"`
	Currency    string   `json:"currency"`
	Metadata    Metadata `json:"metadata"`
}
type Charge struct {
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	ChargeID       string    `json:"charge_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Status         string    `json:"status"`
	AmountMinor    int64     `json:"amount_minor"`
	Currency       string    `json:"currency"`
	Metadata       Metadata  `json:"metadata"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
type Faults struct {
	LatencyMS          int     `json:"latency_ms"`
	TimeoutRate        float64 `json:"timeout_rate"`
	LostResponseRate   float64 `json:"lost_response_rate"`
	ErrorRate          float64 `json:"error_rate"`
	DeclineRate        float64 `json:"decline_rate"`
	RateLimitRPS       int     `json:"rate_limit_rps"`
	LedgerIndexDelayMS int     `json:"ledger_index_delay_ms"`
}
type LedgerPage struct {
	SnapshotID      string    `json:"snapshot_id"`
	NextCursor      string    `json:"next_cursor,omitempty"`
	CompleteThrough time.Time `json:"complete_through"`
	Entries         []Charge  `json:"entries"`
}
type snapshot struct {
	id       string
	entries  []Charge
	complete time.Time
}

type Mock struct {
	mu              sync.Mutex
	charges         map[string]Charge
	tombstones      map[string]time.Time
	events          []Charge
	snapshots       map[string]snapshot
	faults          Faults
	second          int64
	callsThisSecond int
	rng             *rand.Rand
	now             func() time.Time
	seq             uint64
	webhookURL      string
	webhookSecret   []byte
	webhookHTTP     *http.Client
}

func NewMock() *Mock {
	return &Mock{charges: map[string]Charge{}, tombstones: map[string]time.Time{}, snapshots: map[string]snapshot{}, rng: rand.New(rand.NewSource(time.Now().UnixNano())), now: time.Now, webhookHTTP: &http.Client{Timeout: time.Second}}
}

// SetWebhook configures authenticated PSP callbacks. Configuration must be
// completed before the mock begins serving concurrent requests.
func (m *Mock) SetWebhook(endpoint string, secret []byte) error {
	if endpoint == "" && len(secret) == 0 {
		m.webhookURL = ""
		m.webhookSecret = nil
		return nil
	}
	if endpoint == "" || len(secret) < 32 {
		return fmt.Errorf("webhook URL and a secret of at least 32 bytes are required")
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return fmt.Errorf("invalid webhook URL: %w", err)
	}
	m.webhookURL = endpoint
	m.webhookSecret = append([]byte(nil), secret...)
	return nil
}

func (m *Mock) sendWebhook(charge Charge) error {
	m.mu.Lock()
	endpoint := m.webhookURL
	secret := append([]byte(nil), m.webhookSecret...)
	client := m.webhookHTTP
	m.mu.Unlock()
	if endpoint == "" {
		return nil
	}
	body, err := json.Marshal(charge)
	if err != nil {
		return err
	}
	if client == nil {
		client = &http.Client{Timeout: time.Second}
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		req, reqErr := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(body)))
		if reqErr != nil {
			return reqErr
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-PSP-Timestamp", timestamp)
		req.Header.Set("X-PSP-Signature", SignWebhook(secret, timestamp, body))
		resp, callErr := client.Do(req)
		if callErr == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			callErr = fmt.Errorf("webhook receiver returned %s", resp.Status)
		}
		lastErr = callErr
		if attempt < 2 {
			time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond)
		}
	}
	return fmt.Errorf("PSP webhook delivery failed after retries: %w", lastErr)
}

// SetClock installs a deterministic clock for recovery-window and ledger-watermark tests.
// It should be configured before concurrent use.
func (m *Mock) SetClock(now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if now == nil {
		m.now = time.Now
	} else {
		m.now = now
	}
}
func (m *Mock) SetFaults(f Faults) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f.TimeoutRate < 0 {
		f.TimeoutRate = 0
	}
	if f.TimeoutRate > 1 {
		f.TimeoutRate = 1
	}
	if f.LostResponseRate < 0 {
		f.LostResponseRate = 0
	}
	if f.LostResponseRate > 1 {
		f.LostResponseRate = 1
	}
	if f.ErrorRate < 0 {
		f.ErrorRate = 0
	}
	if f.ErrorRate > 1 {
		f.ErrorRate = 1
	}
	if f.DeclineRate < 0 {
		f.DeclineRate = 0
	}
	if f.DeclineRate > 1 {
		f.DeclineRate = 1
	}
	m.faults = f
}
func (m *Mock) getFaults() Faults     { m.mu.Lock(); defer m.mu.Unlock(); return m.faults }
func (m *Mock) should(p float64) bool { m.mu.Lock(); defer m.mu.Unlock(); return m.rng.Float64() < p }
func (m *Mock) beforeCall(w http.ResponseWriter, r *http.Request) bool {
	f := m.getFaults()
	if f.LatencyMS > 0 {
		timer := time.NewTimer(time.Duration(f.LatencyMS) * time.Millisecond)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return false
		}
	}
	if f.RateLimitRPS > 0 {
		m.mu.Lock()
		sec := m.now().Unix()
		if m.second != sec {
			m.second = sec
			m.callsThisSecond = 0
		}
		m.callsThisSecond++
		limited := m.callsThisSecond > f.RateLimitRPS
		m.mu.Unlock()
		if limited {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, 429, map[string]string{"error": "rate_limited"})
			return false
		}
	}
	if m.should(f.TimeoutRate) {
		writeJSON(w, 504, map[string]string{"error": "injected_timeout_before_effect"})
		return false
	}
	if m.should(f.ErrorRate) {
		writeJSON(w, 503, map[string]string{"error": "injected_error"})
		return false
	}
	return true
}
func (m *Mock) recordEvent(c Charge) {
	m.seq++
	c.EventID = fmt.Sprintf("evt-%d", m.seq)
	m.events = append(m.events, c)
}
func (m *Mock) Authorize(key string, req Request) (Charge, bool, error) {
	if key == "" || req.Metadata.OrderID == "" || req.AmountMinor <= 0 {
		return Charge{}, false, fmt.Errorf("idempotency key, order id and positive amount required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dead := m.tombstones[key]; dead {
		return Charge{}, false, ErrVoided
	}
	if c, ok := m.charges[key]; ok {
		return c, false, nil
	}
	now := m.now().UTC()
	m.seq++
	status := "AUTHORIZED"
	if m.rng.Float64() < m.faults.DeclineRate {
		status = "DECLINED"
	}
	c := Charge{EventID: fmt.Sprintf("evt-%d", m.seq), ChargeID: fmt.Sprintf("ch_%d", m.seq), IdempotencyKey: key, EventType: "AUTHORIZATION", Status: status, AmountMinor: req.AmountMinor, Currency: req.Currency, Metadata: req.Metadata, CreatedAt: now, UpdatedAt: now}
	m.charges[key] = c
	m.events = append(m.events, c)
	return c, true, nil
}
func (m *Mock) Capture(key string) (Charge, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.charges[key]
	if !ok {
		return Charge{}, ErrNotFound
	}
	if c.Status == "CAPTURED" {
		return c, nil
	}
	if c.Status != "AUTHORIZED" {
		return Charge{}, ErrDeclined
	}
	c.Status = "CAPTURED"
	c.UpdatedAt = m.now().UTC()
	m.charges[key] = c
	m.seq++
	ev := c
	ev.EventID = fmt.Sprintf("evt-%d", m.seq)
	ev.EventType = "CAPTURE"
	m.events = append(m.events, ev)
	return ev, nil
}
func (m *Mock) Void(key string) (Charge, error) {
	if key == "" {
		return Charge{}, fmt.Errorf("idempotency key required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if deadAt, ok := m.tombstones[key]; ok {
		if c, exists := m.charges[key]; exists {
			return c, nil
		}
		return Charge{IdempotencyKey: key, Status: "VOIDED", UpdatedAt: deadAt}, nil
	}
	c, ok := m.charges[key]
	now := m.now().UTC()
	if !ok {
		m.tombstones[key] = now
		m.seq++
		ev := Charge{EventID: fmt.Sprintf("evt-%d", m.seq), EventType: "AUTHORIZATION", IdempotencyKey: key, Status: "VOIDED", UpdatedAt: now}
		m.events = append(m.events, ev)
		return ev, nil
	}
	if c.Status == "CAPTURED" {
		return Charge{}, fmt.Errorf("captured charge cannot be voided")
	}
	c.Status = "VOIDED"
	c.UpdatedAt = now
	m.charges[key] = c
	m.tombstones[key] = now
	m.seq++
	ev := c
	ev.EventID = fmt.Sprintf("evt-%d", m.seq)
	ev.EventType = "AUTHORIZATION"
	m.events = append(m.events, ev)
	return ev, nil
}
func (m *Mock) Get(key string) (Charge, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.charges[key]; ok {
		return c, nil
	}
	if _, ok := m.tombstones[key]; ok {
		return Charge{IdempotencyKey: key, Status: "VOIDED"}, nil
	}
	return Charge{}, ErrNotFound
}

// ScanLedger exposes the common recovery-scanner interface.
func (m *Mock) ScanLedger(from, to time.Time, homeRegion, eventType, cursor string, limit int) (LedgerPage, error) {
	return m.Ledger(from, to, homeRegion, eventType, cursor, limit)
}

func (m *Mock) Ledger(from, to time.Time, homeRegion, eventType, cursor string, limit int) (LedgerPage, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var snap snapshot
	index := 0
	if cursor != "" {
		parts := strings.Split(cursor, ":")
		if len(parts) != 2 {
			return LedgerPage{}, fmt.Errorf("invalid cursor")
		}
		var ok bool
		snap, ok = m.snapshots[parts[0]]
		if !ok {
			return LedgerPage{}, fmt.Errorf("snapshot expired")
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil || n < 0 || n > len(snap.entries) {
			return LedgerPage{}, fmt.Errorf("invalid cursor offset")
		}
		index = n
	} else {
		complete := m.now().Add(-time.Duration(m.faults.LedgerIndexDelayMS) * time.Millisecond).UTC()
		entries := make([]Charge, 0)
		for _, e := range m.events {
			at := e.CreatedAt
			if e.UpdatedAt.After(at) {
				at = e.UpdatedAt
			}
			if at.Before(from) || at.After(to) || at.After(complete) {
				continue
			}
			if homeRegion != "" && e.Metadata.HomeRegion != homeRegion {
				continue
			}
			if eventType != "" && e.EventType != eventType {
				continue
			}
			entries = append(entries, e)
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
				return entries[i].EventID < entries[j].EventID
			}
			return entries[i].CreatedAt.Before(entries[j].CreatedAt)
		})
		id := snapshotID(from, to, homeRegion, m.seq)
		snap = snapshot{id: id, entries: entries, complete: complete}
		m.snapshots[id] = snap
	}
	end := index + limit
	if end > len(snap.entries) {
		end = len(snap.entries)
	}
	page := LedgerPage{SnapshotID: snap.id, CompleteThrough: snap.complete, Entries: append([]Charge(nil), snap.entries[index:end]...)}
	if end < len(snap.entries) {
		page.NextCursor = snap.id + ":" + strconv.Itoa(end)
	}
	return page, nil
}
func snapshotID(from, to time.Time, region string, seq uint64) string {
	sum := sha256.Sum256([]byte(from.String() + to.String() + region + strconv.FormatUint(seq, 10)))
	return hex.EncodeToString(sum[:8])
}

func (m *Mock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writeJSON(w, 200, map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path == "/admin/faults" && r.Method == http.MethodPost {
		var f Faults
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		m.SetFaults(f)
		writeJSON(w, 200, f)
		return
	}
	if !m.beforeCall(w, r) {
		return
	}
	switch {
	case r.URL.Path == "/authorize" && r.Method == http.MethodPost:
		key := r.Header.Get("Idempotency-Key")
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		c, created, err := m.Authorize(key, req)
		if err != nil {
			status := 409
			if errors.Is(err, ErrVoided) {
				status = 409
			}
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
		if created {
			_ = m.sendWebhook(c)
		}
		if created && m.should(m.getFaults().LostResponseRate) {
			writeJSON(w, 504, map[string]string{"error": "response_lost_after_effect"})
			return
		}
		writeJSON(w, 200, c)
	case strings.HasPrefix(r.URL.Path, "/charges/") && r.Method == http.MethodGet:
		key := strings.TrimPrefix(r.URL.Path, "/charges/")
		c, err := m.Get(key)
		if err != nil {
			writeJSON(w, 404, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, c)
	case r.URL.Path == "/capture" && r.Method == http.MethodPost:
		var b struct {
			Key string `json:"idempotency_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		c, err := m.Capture(b.Key)
		if err != nil {
			writeJSON(w, 409, map[string]string{"error": err.Error()})
			return
		}
		_ = m.sendWebhook(c)
		writeJSON(w, 200, c)
	case r.URL.Path == "/void" && r.Method == http.MethodPost:
		var b struct {
			Key string `json:"idempotency_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		c, err := m.Void(b.Key)
		if err != nil {
			writeJSON(w, 409, map[string]string{"error": err.Error()})
			return
		}
		_ = m.sendWebhook(c)
		writeJSON(w, 200, c)
	case r.URL.Path == "/ledger" && r.Method == http.MethodGet:
		from, err1 := time.Parse(time.RFC3339Nano, r.URL.Query().Get("from"))
		to, err2 := time.Parse(time.RFC3339Nano, r.URL.Query().Get("to"))
		if err1 != nil || err2 != nil || to.Before(from) {
			writeJSON(w, 400, map[string]string{"error": "valid from/to RFC3339 times are required"})
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		page, err := m.Ledger(from, to, r.URL.Query().Get("home_region"), r.URL.Query().Get("event_type"), r.URL.Query().Get("cursor"), limit)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, page)
	default:
		http.NotFound(w, r)
	}
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Client is the production-facing HTTP contract used by workers.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 2 * time.Second}
}
func (c *Client) do(method, path, key string, body any, out any) error {
	var rd *strings.Reader
	if body == nil {
		rd = strings.NewReader("")
	} else {
		b, e := json.Marshal(body)
		if e != nil {
			return e
		}
		rd = strings.NewReader(string(b))
	}
	req, e := http.NewRequest(method, strings.TrimRight(c.BaseURL, "/")+path, rd)
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, e := c.httpClient().Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			return ErrRateLimited
		}
		if resp.StatusCode == http.StatusGatewayTimeout {
			return ErrUnknown
		}
		return fmt.Errorf("PSP %s %s: %s", method, path, resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
func (c *Client) Authorize(key string, req Request) (Charge, error) {
	var out Charge
	err := c.do(http.MethodPost, "/authorize", key, req, &out)
	if err != nil {
		return Charge{}, err
	}
	return out, nil
}
func (c *Client) Capture(key string) (Charge, error) {
	var out Charge
	err := c.do(http.MethodPost, "/capture", "", map[string]string{"idempotency_key": key}, &out)
	return out, err
}
func (c *Client) Void(key string) (Charge, error) {
	var out Charge
	err := c.do(http.MethodPost, "/void", "", map[string]string{"idempotency_key": key}, &out)
	return out, err
}
func (c *Client) Get(key string) (Charge, error) {
	var out Charge
	err := c.do(http.MethodGet, "/charges/"+key, "", nil, &out)
	return out, err
}
func (c *Client) Ledger(from, to time.Time, region, cursor string, limit int) (LedgerPage, error) {
	path := fmt.Sprintf("/ledger?from=%s&to=%s&event_type=AUTHORIZATION&home_region=%s&cursor=%s&limit=%d", urlEscape(from.Format(time.RFC3339Nano)), urlEscape(to.Format(time.RFC3339Nano)), urlEscape(region), urlEscape(cursor), limit)
	var out LedgerPage
	err := c.do(http.MethodGet, path, "", nil, &out)
	return out, err
}

// ScanLedger implements orders.RecoveryLedger with an explicit event-type filter.
func (c *Client) ScanLedger(from, to time.Time, region, eventType, cursor string, limit int) (LedgerPage, error) {
	path := fmt.Sprintf("/ledger?from=%s&to=%s&event_type=%s&home_region=%s&cursor=%s&limit=%d", urlEscape(from.Format(time.RFC3339Nano)), urlEscape(to.Format(time.RFC3339Nano)), urlEscape(eventType), urlEscape(region), urlEscape(cursor), limit)
	var out LedgerPage
	err := c.do(http.MethodGet, path, "", nil, &out)
	return out, err
}
func urlEscape(s string) string         { return url.QueryEscape(s) }
func KeyForOrder(orderID string) string { return base64.RawURLEncoding.EncodeToString([]byte(orderID)) }

// Stats returns aggregate ledger counts for tests and observability.
func (m *Mock) Stats() (charges, events int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.charges), len(m.events)
}

// DirectClient adapts the in-process mock to the worker PSP interface used by
// the single-process demo. Production-shaped services use Client over HTTP.
type DirectClient struct{ Mock *Mock }

func (d DirectClient) Authorize(key string, req Request) (Charge, error) {
	if d.Mock == nil {
		return Charge{}, fmt.Errorf("mock PSP unavailable")
	}
	f := d.Mock.getFaults()
	if f.TimeoutRate > 0 && d.Mock.should(f.TimeoutRate) {
		return Charge{}, ErrUnknown
	}
	c, created, err := d.Mock.Authorize(key, req)
	if err != nil {
		return Charge{}, err
	}
	if created && d.Mock.should(f.LostResponseRate) {
		return Charge{}, ErrUnknown
	}
	return c, nil
}
func (d DirectClient) Get(key string) (Charge, error) {
	if d.Mock == nil {
		return Charge{}, fmt.Errorf("mock PSP unavailable")
	}
	return d.Mock.Get(key)
}
func (d DirectClient) Capture(key string) (Charge, error) {
	if d.Mock == nil {
		return Charge{}, fmt.Errorf("mock PSP unavailable")
	}
	return d.Mock.Capture(key)
}
func (d DirectClient) Void(key string) (Charge, error) {
	if d.Mock == nil {
		return Charge{}, fmt.Errorf("mock PSP unavailable")
	}
	return d.Mock.Void(key)
}

// Stats exposes mock-ledger counters through the demo adapter without making
// telemetry methods part of the worker PSP contract.
func (d DirectClient) Stats() (charges, events int) {
	if d.Mock == nil {
		return 0, 0
	}
	return d.Mock.Stats()
}
