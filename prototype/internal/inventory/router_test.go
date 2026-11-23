package inventory

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func newTestRouter(t *testing.T, shardCount int) (*Router, *MemoryClaimRepository) {
	t.Helper()
	claims := NewMemoryClaimRepository()
	router, err := NewRouter(shardCount, claims)
	if err != nil {
		t.Fatal(err)
	}
	return router, claims
}

func TestRouter_SpecificSeatUsesUnitHashAndPinsClaim(t *testing.T) {
	router, _ := newTestRouter(t, 16)
	want, err := ShardForSpecificSeat("seat-A-101", 16)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := router.Resolve(RouteRequest{
		HoldID: "hold-seat-1", ItemID: "concert", UserID: "alice", RequestedUnitID: "seat-A-101",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Shard != want {
		t.Fatalf("specific seat routed to %d, want %d", claim.Shard, want)
	}
}

func TestRouter_AnyUnitWalksFromUserHash(t *testing.T) {
	router, _ := newTestRouter(t, 8)
	start, err := StartingShardForUser("alice", 8)
	if err != nil {
		t.Fatal(err)
	}
	probed := make([]int, 0, 8)
	claim, err := router.Resolve(RouteRequest{
		HoldID: "hold-any-1", ItemID: "concert", UserID: "alice",
	}, func(_ string, shard int) (bool, error) {
		probed = append(probed, shard)
		return shard == (start+3)%8, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Shard != (start+3)%8 {
		t.Fatalf("selected shard %d, want %d", claim.Shard, (start+3)%8)
	}
	wantProbeOrder := []int{start, (start + 1) % 8, (start + 2) % 8, (start + 3) % 8}
	if fmt.Sprint(probed) != fmt.Sprint(wantProbeOrder) {
		t.Fatalf("probe order %v, want %v", probed, wantProbeOrder)
	}
}

func TestRouter_ClaimedShardDoesNotMoveWhenShardDrains(t *testing.T) {
	router, _ := newTestRouter(t, 8)
	first, err := router.Resolve(RouteRequest{HoldID: "hold-drain", ItemID: "concert", UserID: "alice"},
		func(_ string, _ int) (bool, error) { return true, nil })
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the selected shard draining between the stock hint and the
	// atomic reserve script. The reserve operation would now return ErrSoldOut.
	// A retry resolves the existing claim and does not probe another shard.
	probes := 0
	retry, err := router.Resolve(RouteRequest{HoldID: "hold-drain", ItemID: "concert", UserID: "alice"},
		func(_ string, _ int) (bool, error) {
			probes++
			return false, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if retry.Shard != first.Shard {
		t.Fatalf("claim moved from shard %d to %d", first.Shard, retry.Shard)
	}
	if probes != 0 {
		t.Fatalf("retry reprobed %d shards; existing claim must pin routing", probes)
	}
}

func TestRouter_NoStockReturnsSoldOutAndDoesNotCreateClaim(t *testing.T) {
	router, claims := newTestRouter(t, 4)
	_, err := router.Resolve(RouteRequest{HoldID: "hold-none", ItemID: "concert", UserID: "alice"},
		func(_ string, _ int) (bool, error) { return false, nil })
	if !errors.Is(err, ErrSoldOut) {
		t.Fatalf("got %v, want ErrSoldOut", err)
	}
	if _, ok, _ := claims.Get("hold-none"); ok {
		t.Fatal("claim created despite no candidate shard")
	}
}

func TestRouter_ProbeErrorFailsClosed(t *testing.T) {
	router, claims := newTestRouter(t, 4)
	_, err := router.Resolve(RouteRequest{HoldID: "hold-unknown", ItemID: "concert", UserID: "alice"},
		func(_ string, _ int) (bool, error) { return false, errors.New("inventory shard unavailable") })
	if err == nil {
		t.Fatal("expected probe error")
	}
	if _, ok, _ := claims.Get("hold-unknown"); ok {
		t.Fatal("claim must not be created from unknown stock")
	}
}

func TestRouter_ReplayMismatchRejected(t *testing.T) {
	router, _ := newTestRouter(t, 4)
	request := RouteRequest{HoldID: "hold-1", ItemID: "concert", UserID: "alice"}
	if _, err := router.Resolve(request, func(string, int) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	request.UserID = "bob"
	if _, err := router.Resolve(request, func(string, int) (bool, error) { return true, nil }); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("got %v, want ErrReplayMismatch", err)
	}
}

func TestRouter_ConcurrentDuplicatesConvergeOnOneClaim(t *testing.T) {
	const racers = 128
	router, _ := newTestRouter(t, 16)
	request := RouteRequest{HoldID: "hold-race", ItemID: "concert", UserID: "alice"}
	results := make(chan ShardClaim, racers)
	errs := make(chan error, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			claim, err := router.Resolve(request, func(_ string, shard int) (bool, error) {
				// Different racing requests can observe different candidate
				// shards; atomic PutIfAbsent must still choose one winner.
				return shard == i%16, nil
			})
			if err != nil {
				errs <- err
				return
			}
			results <- claim
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("Resolve returned error: %v", err)
	}
	selected := -1
	count := 0
	for claim := range results {
		count++
		if selected == -1 {
			selected = claim.Shard
		} else if claim.Shard != selected {
			t.Fatalf("duplicate requests observed shards %d and %d", selected, claim.Shard)
		}
	}
	if count != racers {
		t.Fatalf("got %d successful results, want %d", count, racers)
	}
}

func TestRedisClaimKeyIsPerHoldAndSafeHashTag(t *testing.T) {
	keyA, err := RedisClaimKey("hold-{a}")
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := RedisClaimKey("hold-{b}")
	if err != nil {
		t.Fatal(err)
	}
	if keyA == keyB {
		t.Fatalf("different holds share claim key %q", keyA)
	}
	if keyA != "hold-claim:{aG9sZC17YX0}" {
		t.Fatalf("unexpected encoded key %q", keyA)
	}
}

func TestRouter_InvalidConfigAndRequests(t *testing.T) {
	if _, err := NewRouter(0, NewMemoryClaimRepository()); err == nil {
		t.Fatal("expected invalid shard count")
	}
	if _, err := NewRouter(2, nil); err == nil {
		t.Fatal("expected nil claim repository error")
	}
	router, _ := newTestRouter(t, 2)
	if _, err := router.Resolve(RouteRequest{HoldID: "h", ItemID: "i"}, nil); err == nil {
		t.Fatal("expected missing user error")
	}
}
