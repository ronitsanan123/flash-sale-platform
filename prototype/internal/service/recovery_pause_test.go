package service

import (
	"context"
	"testing"
)

type fakeRedis struct{ values map[string]string }

func (f *fakeRedis) Do(_ context.Context, _ string, args ...string) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	key := args[1]
	switch args[0] {
	case "SET":
		if len(args) >= 3 {
			f.values[key] = args[2]
		}
		return "OK", nil
	case "GET":
		if value, ok := f.values[key]; ok {
			return value, nil
		}
		return nil, nil
	case "DEL":
		delete(f.values, key)
		return int64(1), nil
	default:
		return nil, nil
	}
}

func TestRegionalRecoveryPauseFailsClosedUntilExplicitlyCleared(t *testing.T) {
	ctx := context.Background()
	redis := &fakeRedis{values: map[string]string{}}
	paused, err := IsRecoveryPaused(ctx, redis, "region-a")
	if err != nil || paused {
		t.Fatalf("initial gate paused=%v err=%v", paused, err)
	}
	if err := SetRecoveryPauseSignal(ctx, redis, "region-a", true); err != nil {
		t.Fatal(err)
	}
	paused, err = IsRecoveryPaused(ctx, redis, "region-a")
	if err != nil || !paused {
		t.Fatalf("gate should remain paused: paused=%v err=%v", paused, err)
	}
	if err := SetRecoveryPauseSignal(ctx, redis, "region-a", false); err != nil {
		t.Fatal(err)
	}
	paused, err = IsRecoveryPaused(ctx, redis, "region-a")
	if err != nil || paused {
		t.Fatalf("gate should clear only explicitly: paused=%v err=%v", paused, err)
	}
}
