package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"flashsale-platform/prototype/internal/inventory"
	"flashsale-platform/prototype/internal/rediswire"
)

func redisStore(t *testing.T, terminalRetention time.Duration) *inventory.RedisStore {
	t.Helper()
	seeds := strings.Split(os.Getenv("REDIS_CLUSTER_SEEDS"), ",")
	if strings.TrimSpace(seeds[0]) == "" { t.Skip("REDIS_CLUSTER_SEEDS not configured; run inside Compose integration service") }
	client, err := rediswire.NewCluster(seeds, 3*time.Second); if err != nil { t.Fatal(err) }
	store, err := inventory.NewRedisStore(client, 3, 5*time.Second, 5*time.Second, 30*time.Minute, terminalRetention)
	if err != nil { t.Fatal(err) }
	return store
}

func TestRedisClusterReserveNoOversell(t *testing.T) {
	store:=redisStore(t, time.Minute)
	ctx,cancel:=context.WithTimeout(context.Background(),90*time.Second); defer cancel()
	item:=fmt.Sprintf("it-%d",time.Now().UnixNano())
	units:=make([]string,100); for i:=range units { units[i]=fmt.Sprintf("unit-%03d",i) }
	if err:=store.Seed(ctx,item,units); err!=nil { t.Fatal(err) }
	var successes atomic.Int64
	var wg sync.WaitGroup
	errCh:=make(chan error,5000)
	for i:=0;i<5000;i++ { wg.Add(1); go func(i int){ defer wg.Done(); unit:=units[i%len(units)]; _,err:=store.Reserve(ctx,item,fmt.Sprintf("%s-hold-%d",item,i),fmt.Sprintf("user-%d",i),unit); if err==nil { successes.Add(1); return }; if err!=inventory.ErrTaken { errCh<-err } }(i) }
	wg.Wait(); close(errCh)
	for err:=range errCh { t.Errorf("unexpected reserve error: %v",err) }
	if got:=successes.Load(); got!=100 { t.Fatalf("successful holds=%d, want 100",got) }
	available,held,paying,confirmed,forfeited,total,err:=store.Availability(ctx,item)
	if err!=nil { t.Fatal(err) }
	if total!=100 || held!=100 || available!=0 || paying+confirmed+forfeited!=0 { t.Fatalf("inventory invariant: available=%d held=%d paying=%d confirmed=%d forfeited=%d total=%d",available,held,paying,confirmed,forfeited,total) }
}

func TestRedisClusterForfeitDoesNotReturnUnitAndTombstonesID(t *testing.T) {
	store:=redisStore(t,time.Minute)
	ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second); defer cancel()
	item:=fmt.Sprintf("ft-%d",time.Now().UnixNano()); unit:="fixed-seat"
	if err:=store.Seed(ctx,item,[]string{unit}); err!=nil { t.Fatal(err) }
	h,err:=store.Reserve(ctx,item,item+"-hold-forfeit","user-a",unit); if err!=nil { t.Fatal(err) }
	if _,err:=store.LockForPayment(ctx,item,h.ID); err!=nil { t.Fatal(err) }
	if err:=store.Forfeit(ctx,item,h.ID,inventory.ForfeitProofNotAttempted); err!=nil { t.Fatal(err) }
	if _,err:=store.Reserve(ctx,item,item+"-hold-second","user-b",unit); err!=inventory.ErrTaken && err!=inventory.ErrSoldOut { t.Fatalf("forfeited unit was reusable: %v",err) }
	if _,err:=store.Reserve(ctx,item,h.ID,"user-a",unit); err!=inventory.ErrHoldForfeited { t.Fatalf("forfeit replay=%v",err) }
	_,_,_,_,forfeited,total,err:=store.Availability(ctx,item); if err!=nil { t.Fatal(err) }
	if forfeited!=1 || total!=1 { t.Fatalf("forfeited=%d total=%d",forfeited,total) }
}

func TestRedisClusterReleasedHoldTombstoneSurvivesCleanup(t *testing.T) {
	store:=redisStore(t,20*time.Millisecond)
	ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second); defer cancel()
	item:=fmt.Sprintf("rel-%d",time.Now().UnixNano()); unit:="unit-release"
	if err:=store.Seed(ctx,item,[]string{unit}); err!=nil { t.Fatal(err) }
	h,err:=store.Reserve(ctx,item,item+"-hold-release","user-a",unit); if err!=nil { t.Fatal(err) }
	// Force expiry with the shard script's Redis TIME; tests wait past its short TTL.
	time.Sleep(5100*time.Millisecond)
	shard,_:=inventory.ShardForSpecificSeat(unit,3)
	released,_,err:=store.Reap(ctx,item,shard,10); if err!=nil { t.Fatal(err) }; if released!=1 { t.Fatalf("released=%d want=1",released) }
	time.Sleep(30*time.Millisecond)
	if _,err:=store.CleanupTerminals(ctx,item,shard,10); err!=nil { t.Fatal(err) }
	if _,err:=store.Reserve(ctx,item,h.ID,"user-a",unit); err!=inventory.ErrHoldReleased { t.Fatalf("terminal ID reuse result=%v",err) }
}
