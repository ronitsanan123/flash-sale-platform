package orders

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// RedisCommander is satisfied by rediswire.Cluster and allows deterministic
// health-gate tests without a Redis process.
type RedisCommander interface {
	Do(context.Context, string, ...string) (any, error)
}

type StandbySignal struct {
	HomeRegion string    `json:"home_region"`
	Epoch      int64     `json:"epoch"`
	Healthy    bool      `json:"healthy"`
	CheckedAt  time.Time `json:"checked_at"`
	Reason     string    `json:"reason,omitempty"`
}

type RedisStandbyGate struct {
	Client RedisCommander
	MaxAge time.Duration
	Now    func() time.Time
}

func standbySignalKey(region string) string {
	return "ctrl:standby-ack:{" + base64.RawURLEncoding.EncodeToString([]byte(region)) + "}"
}

func (g *RedisStandbyGate) Publish(ctx context.Context, signal StandbySignal) error {
	if g == nil || g.Client == nil || signal.HomeRegion == "" || signal.Epoch <= 0 || signal.CheckedAt.IsZero() {
		return fmt.Errorf("standby signal publisher configuration invalid")
	}
	payload, err := json.Marshal(signal)
	if err != nil {
		return err
	}
	key := standbySignalKey(signal.HomeRegion)
	_, err = g.Client.Do(ctx, key, "SET", key, string(payload))
	return err
}

func (g *RedisStandbyGate) Wait(ctx context.Context, region string, epoch int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g == nil || g.Client == nil || region == "" || epoch <= 0 {
		return ErrStandbyUnavailable
	}
	key := standbySignalKey(region)
	v, err := g.Client.Do(ctx, key, "GET", key)
	if err != nil || v == nil {
		return ErrStandbyUnavailable
	}
	raw, ok := v.(string)
	if !ok {
		return ErrStandbyUnavailable
	}
	var signal StandbySignal
	if err := json.Unmarshal([]byte(raw), &signal); err != nil {
		return ErrStandbyUnavailable
	}
	now := time.Now()
	if g.Now != nil {
		now = g.Now()
	}
	maxAge := g.MaxAge
	if maxAge <= 0 {
		maxAge = 3 * time.Second
	}
	if signal.HomeRegion != region || signal.Epoch != epoch || !signal.Healthy || signal.CheckedAt.IsZero() {
		return ErrStandbyUnavailable
	}
	age := now.Sub(signal.CheckedAt)
	if age < 0 || age > maxAge {
		return ErrStandbyUnavailable
	}
	return nil
}

var _ StandbyAckGate = (*RedisStandbyGate)(nil)
