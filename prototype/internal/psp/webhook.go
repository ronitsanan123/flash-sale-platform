package psp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// SignWebhook signs timestamp + '.' + body. The timestamp is included in the
// MAC so a captured request cannot be replayed with a fresh timestamp.
func SignWebhook(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func VerifyWebhook(secret []byte, timestamp, signature string, body []byte, now time.Time, maxSkew time.Duration) error {
	if len(secret) < 32 {
		return fmt.Errorf("PSP webhook secret must be at least 32 bytes")
	}
	unix, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid PSP webhook timestamp")
	}
	if maxSkew <= 0 {
		maxSkew = 5 * time.Minute
	}
	at := time.Unix(unix, 0)
	delta := now.Sub(at)
	if delta < 0 {
		delta = -delta
	}
	if delta > maxSkew {
		return fmt.Errorf("PSP webhook timestamp outside allowed skew")
	}
	want := SignWebhook(secret, timestamp, body)
	if !hmac.Equal([]byte(want), []byte(signature)) {
		return fmt.Errorf("invalid PSP webhook signature")
	}
	return nil
}
