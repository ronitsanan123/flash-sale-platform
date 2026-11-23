package service

import (
	"context"
	"encoding/base64"
	"fmt"

	"flashsale-platform/prototype/internal/orders"
)

// recoveryPauseKey isolates the recovery gate by home region in the dedicated
// CTRL Redis service, never in the hot inventory keyspace.
func recoveryPauseKey(homeRegion string) string {
	return "ctrl:regional-recovery-paused:{" + base64.RawURLEncoding.EncodeToString([]byte(homeRegion)) + "}"
}

// SetRecoveryPauseSignal sets or clears a persistent fail-closed admission/confirmation
// gate. It intentionally has no TTL: a crashed/incomplete recovery must not
// silently reopen admissions. Only a successful recovery clears the key.
func SetRecoveryPauseSignal(ctx context.Context, client orders.RedisCommander, homeRegion string, paused bool) error {
	if client == nil || homeRegion == "" {
		return fmt.Errorf("recovery pause client and home region are required")
	}
	key := recoveryPauseKey(homeRegion)
	if !paused {
		_, err := client.Do(ctx, key, "DEL", key)
		return err
	}
	_, err := client.Do(ctx, key, "SET", key, "1")
	return err
}

// IsRecoveryPaused returns true while a regional recovery run has not completed
// its full PSP-ledger scan, DB/PSP reconciliation and inventory repair.
func IsRecoveryPaused(ctx context.Context, client orders.RedisCommander, homeRegion string) (bool, error) {
	if client == nil || homeRegion == "" {
		return false, fmt.Errorf("recovery pause client and home region are required")
	}
	key := recoveryPauseKey(homeRegion)
	v, err := client.Do(ctx, key, "GET", key)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}
