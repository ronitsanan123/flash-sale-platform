package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// This intentionally broken read-then-write model demonstrates a lost-update
// oversell. Atomics avoid a Go data race while preserving the logical race.
func main() {
	const stock = int64(10)
	const buyers = 100
	var available atomic.Int64
	available.Store(stock)
	start := make(chan struct{})
	var done sync.WaitGroup
	done.Add(buyers)
	var ready sync.WaitGroup
	ready.Add(buyers)
	var sold atomic.Int64
	for i := 0; i < buyers; i++ {
		go func() {
			observed := available.Load()
			ready.Done()
			<-start
			if observed > 0 {
				available.Store(observed - 1)
				sold.Add(1)
			}
			done.Done()
		}()
	}
	ready.Wait()
	// Release all contenders after every goroutine has observed the same stock.
	close(start)
	done.Wait()
	fmt.Printf("naive_demo=DETECTED stock=%d successful_reservations=%d remaining_counter=%d oversell=%d reconciler_exit=1_expected\n", stock, sold.Load(), available.Load(), max(0, sold.Load()-stock))
	fmt.Println("naive payment ordering can also charge before order persistence; injected order-write failures produce orphan authorizations")
}
