package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"flashsale-platform/prototype/internal/admission"
	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/orders"
	"flashsale-platform/prototype/internal/psp"
	"flashsale-platform/prototype/internal/shed"
	"flashsale-platform/prototype/internal/token"
)

type HTTPServer struct {
	Engine         *Engine
	AdminToken     string
	RequestTimeout time.Duration
	Limiter        *shed.Limiter
	WebhookSecret  []byte
	WebhookMaxSkew time.Duration
}

func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/queue/", s.queue)
	mux.HandleFunc("/admin/queue/", s.adminQueue)
	mux.HandleFunc("/items/", s.items)
	mux.HandleFunc("/holds/", s.holds)
	mux.HandleFunc("/orders/", s.orderStatus)
	mux.HandleFunc("/webhooks/psp", s.pspWebhook)
	return s.withMiddleware(mux)
}
func (s *HTTPServer) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Engine == nil {
			writeError(w, 503, "engine_unavailable")
			return
		}
		s.Engine.requests.Add(1)
		class := requestClass(r)
		if class != "" && s.Limiter != nil {
			permit, err := s.Limiter.Acquire(r.Context(), class)
			if err != nil {
				status := http.StatusTooManyRequests
				if class == shed.Checkout {
					status = http.StatusServiceUnavailable
				}
				w.Header().Set("Retry-After", "1")
				writeError(w, status, "origin_concurrency_budget_exhausted")
				return
			}
			defer permit.Release()
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Allow", "GET, POST, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func requestClass(r *http.Request) shed.Class {
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, "/holds/") && strings.HasSuffix(p, "/checkout"), strings.HasPrefix(p, "/orders/"):
		return shed.Checkout
	case strings.HasPrefix(p, "/items/") && strings.HasSuffix(p, "/reserve"):
		return shed.Reserve
	case strings.HasPrefix(p, "/items/") && strings.HasSuffix(p, "/availability"):
		return shed.Availability
	case strings.HasPrefix(p, "/queue/"):
		return shed.NonEssential
	default:
		return ""
	}
}

func (s *HTTPServer) pspWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if len(s.WebhookSecret) < 32 {
		writeError(w, 503, "psp_webhook_not_configured")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		writeError(w, 400, "invalid_webhook_body")
		return
	}
	if err := psp.VerifyWebhook(s.WebhookSecret, r.Header.Get("X-PSP-Timestamp"), r.Header.Get("X-PSP-Signature"), body, time.Now(), s.WebhookMaxSkew); err != nil {
		writeError(w, 401, "invalid_psp_webhook_signature")
		return
	}
	var charge psp.Charge
	if err := json.Unmarshal(body, &charge); err != nil {
		writeError(w, 400, "invalid_psp_webhook_json")
		return
	}
	if err := s.Engine.HandlePSPWebhook(r.Context(), charge); err != nil {
		if errors.Is(err, orders.ErrOrderNotFound) {
			writeError(w, 202, "recovery_pending")
			return
		}
		if errors.Is(err, orders.ErrStandbyUnavailable) {
			writeError(w, 503, "confirmation_paused")
			return
		}
		writeError(w, 409, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *HTTPServer) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	mode := s.Engine.Mode
	if mode == "" {
		mode = "unknown"
	}
	writeJSON(w, 200, map[string]string{"status": "ok", "mode": mode})
}
func (s *HTTPServer) metrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	m := s.Engine.Metrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for k, v := range m {
		fmt.Fprintf(w, "flashsale_%s %d\n", k, v)
	}
}
func (s *HTTPServer) queue(w http.ResponseWriter, r *http.Request) {
	parts := splitPath(r.URL.Path)
	if len(parts) == 3 && parts[0] == "queue" && parts[2] == "join" && r.Method == http.MethodPost {
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := decodeBody(w, r, &body); err != nil {
			writeError(w, 400, err.Error())
			return
		}
		e, err := s.Engine.Join(r.Context(), parts[1], body.UserID)
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		writeJSON(w, 200, e)
		return
	}
	if r.URL.Path == "/queue/status" && r.Method == http.MethodGet {
		item, user := r.URL.Query().Get("item"), r.URL.Query().Get("user")
		st, err := s.Engine.Poll(r.Context(), item, user)
		if err != nil {
			writeError(w, 404, err.Error())
			return
		}
		writeJSON(w, 200, st)
		return
	}
	methodNotAllowed(w)
}
func (s *HTTPServer) adminQueue(w http.ResponseWriter, r *http.Request) {
	if s.AdminToken == "" || r.Header.Get("X-Admin-Token") != s.AdminToken {
		writeError(w, 401, "admin_unauthorized")
		return
	}
	parts := splitPath(r.URL.Path)
	if len(parts) < 4 {
		writeError(w, 404, "not_found")
		return
	}
	item, action := parts[2], parts[3]
	switch action {
	case "pause":
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		var b struct {
			Paused bool `json:"paused"`
		}
		if err := decodeBody(w, r, &b); err != nil {
			writeError(w, 400, err.Error())
			return
		}
		writeJSON(w, 200, s.Engine.SetPaused(item, b.Paused))
	case "rate":
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		var b struct {
			Rate int64 `json:"rate"`
		}
		if err := decodeBody(w, r, &b); err != nil {
			writeError(w, 400, err.Error())
			return
		}
		wmark := s.Engine.SetAdmissionRate(item, b.Rate)
		writeJSON(w, 200, wmark)
	default:
		writeError(w, 404, "unknown_admin_action")
	}
}
func (s *HTTPServer) items(w http.ResponseWriter, r *http.Request) {
	parts := splitPath(r.URL.Path)
	if len(parts) < 2 {
		writeError(w, 404, "not_found")
		return
	}
	item := parts[1]
	if len(parts) == 3 && parts[2] == "reserve" && r.Method == http.MethodPost {
		auth := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if auth == "" {
			writeError(w, 401, "admission_token_required")
			return
		}
		hold, err := s.Engine.Reserve(r.Context(), auth, item)
		if err != nil {
			status := 400
			if errors.Is(err, token.ErrExpired) || errors.Is(err, token.ErrSignature) || errors.Is(err, token.ErrAudience) || errors.Is(err, token.ErrPurpose) || errors.Is(err, token.ErrMalformed) {
				status = 401
			}
			if errors.Is(err, inventory.ErrSoldOut) || strings.Contains(err.Error(), "admissions paused") {
				status = 409
			}
			writeError(w, status, err.Error())
			return
		}
		writeJSON(w, 201, hold)
		return
	}
	if len(parts) == 3 && parts[2] == "availability" && r.Method == http.MethodGet {
		v, err := s.Engine.Availability(r.Context(), item)
		if err != nil {
			writeError(w, 503, err.Error())
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=5")
		writeJSON(w, 200, v)
		return
	}
	methodNotAllowed(w)
}
func (s *HTTPServer) holds(w http.ResponseWriter, r *http.Request) {
	parts := splitPath(r.URL.Path)
	if len(parts) != 3 || parts[0] != "holds" || parts[2] != "checkout" || r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	holdID := parts[1]
	item := r.Header.Get("X-Item-ID")
	user := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if user == "" {
		writeError(w, 401, "X-User-ID required")
		return
	}
	if item == "" {
		writeError(w, 400, "X-Item-ID required")
		return
	}
	h, err := s.Engine.Checkout(r.Context(), item, holdID, user)
	if err != nil {
		status := 409
		if errors.Is(err, inventory.ErrExpired) {
			status = 410
		}
		if errors.Is(err, inventory.ErrHoldNotFound) {
			status = 404
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"order_id": h.ID, "status": "PAYING", "status_url": "/orders/" + h.ID})
}
func (s *HTTPServer) orderStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	parts := splitPath(r.URL.Path)
	if len(parts) != 2 {
		writeError(w, 404, "not_found")
		return
	}
	item := r.URL.Query().Get("item")
	user := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if user == "" {
		writeError(w, 401, "X-User-ID required")
		return
	}
	v, err := s.Engine.StatusFor(r.Context(), item, parts[1], user)
	if err != nil {
		if errors.Is(err, orders.ErrOrderNotFound) {
			writeError(w, 404, err.Error())
		} else {
			writeError(w, 403, err.Error())
		}
		return
	}
	writeJSON(w, 200, v)
}
func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("request body must contain one JSON object")
	}
	return nil
}
func splitPath(s string) []string {
	trim := strings.Trim(s, "/")
	if trim == "" {
		return nil
	}
	return strings.Split(trim, "/")
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
}

func (e *Engine) RunWorker(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := e.RelayOnce(ctx); err != nil {
				return err
			}
			if _, err := e.Consumer.Once(ctx); err != nil && !errors.Is(err, orders.ErrStandbyUnavailable) {
				continue
			}
		}
	}
}

// PollingStatus helps tests verify a user never gets a token while paused.
func (e *Engine) PollingStatus(ctx context.Context, item, user string) (admission.Status, error) {
	return e.Poll(ctx, item, user)
}

var _ = strconv.Itoa
