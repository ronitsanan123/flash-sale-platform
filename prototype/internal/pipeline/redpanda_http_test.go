package pipeline

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRedpandaCommitUsesOffsetsPayloadAndNextOffset(t *testing.T) {
	var got map[string]any
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/consumers/orders":
			w.Header().Set("Content-Type", "application/vnd.kafka.v2+json")
			_, _ = io.WriteString(w, `{"instance_id":"instance-1","base_uri":"`+"http://"+r.Host+`/consumers/orders/instances/instance-1"}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/subscription"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/offsets"):
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Errorf("decode commit body: %v", err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	log, err := NewRedpandaHTTPLog(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Commit(context.Background(), "checkout-events", "orders", 7, 41); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 {
		t.Fatalf("calls=%v, want create/subscription/offset commit", calls)
	}
	if _, ok := got["partitions"]; ok {
		t.Fatalf("unexpected partitions payload: %#v", got)
	}
	offsets, ok := got["offsets"].([]any)
	if !ok || len(offsets) != 1 {
		t.Fatalf("offsets payload missing: %#v", got)
	}
	entry, ok := offsets[0].(map[string]any)
	if !ok || entry["topic"] != "checkout-events" || entry["partition"] != float64(7) || entry["offset"] != float64(42) {
		t.Fatalf("wrong next-offset commit: %#v", got)
	}
}
