package psp

import (
	"testing"
	"time"
)

func TestWebhookSignatureAndTimestamp(t *testing.T) {
	secret := []byte("test-webhook-secret-that-is-at-least-32-bytes")
	body := []byte(`{"event_id":"evt-1"}`)
	now := time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)
	ts := "1795392000"
	sig := SignWebhook(secret, ts, body)
	if err := VerifyWebhook(secret, ts, sig, body, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := VerifyWebhook(secret, ts, sig, []byte(`{"event_id":"evt-2"}`), now, time.Minute); err == nil {
		t.Fatal("tampered body accepted")
	}
	if err := VerifyWebhook(secret, ts, sig, body, now.Add(10*time.Minute), time.Minute); err == nil {
		t.Fatal("stale webhook accepted")
	}
}
