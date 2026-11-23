package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/pipeline"
)

func TestRedpandaHTTPProxyProduceFetchAndManualCommit(t *testing.T) {
	base := os.Getenv("REDPANDA_HTTP_URL")
	if base == "" {
		t.Skip("REDPANDA_HTTP_URL not configured; run inside Compose integration service")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logAdapter, err := pipeline.NewRedpandaHTTPLog(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logAdapter.Close(context.Background()) }()
	topic := os.Getenv("REDPANDA_TEST_TOPIC")
	if topic == "" {
		topic = "checkout-events"
	}
	group := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	event := inventory.Event{ID: fmt.Sprintf("event-%d", time.Now().UnixNano()), Type: "IntegrationProbe", HoldID: fmt.Sprintf("hold-%d", time.Now().UnixNano()), ItemID: "integration-item", UnitID: "integration-unit", UserID: "integration-user", CreatedAt: time.Now().UTC()}
	produced, err := logAdapter.Publish(ctx, topic, event, 0)
	if err != nil {
		t.Fatalf("publish to Redpanda: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		messages, fetchErr := logAdapter.Fetch(ctx, topic, group, 100)
		if fetchErr != nil {
			t.Fatalf("fetch from Redpanda: %v", fetchErr)
		}
		for _, message := range messages {
			if message.Event.ID == event.ID {
				if message.Partition != produced.Partition || message.Offset != produced.Offset {
					t.Fatalf("fetched position differs: produced=%+v fetched=%+v", produced, message)
				}
				if err := logAdapter.Commit(ctx, topic, group, message.Partition, message.Offset); err != nil {
					t.Fatalf("manual commit: %v", err)
				}
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !found {
		t.Fatal("timed out waiting for published Redpanda event")
	}
}
