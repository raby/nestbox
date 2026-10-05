package example

import (
	"context"
	"sync"
	"testing"

	"github.com/raby/nestbox"
)

// TestConcurrentRelaysDeliverEachEventExactlyOnce backs the library's headline claim: several relays
// draining the same outbox concurrently share the backlog via FOR UPDATE SKIP LOCKED without
// double-delivering. It seeds a batch of events, drains them from many goroutines at once, and asserts
// every event was delivered exactly once and nothing is left unpublished.
func TestConcurrentRelaysDeliverEachEventExactlyOnce(t *testing.T) {
	db := openTestDB(t)
	const (
		events  = 200
		workers = 4
		batch   = 10
	)
	appendN(t, db, events)

	store := nestbox.NewPostgresStore(db)

	var mu sync.Mutex
	seen := make(map[string]int, events)
	h := nestbox.HandlerFunc(func(_ context.Context, e nestbox.Event) error {
		mu.Lock()
		seen[e.ID]++
		mu.Unlock()
		return nil
	})

	// Each worker drains until the outbox is empty. With SKIP LOCKED, concurrent drains claim disjoint
	// rows, so no row is handed to two workers.
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := store.Drain(context.Background(), batch, h)
				if err != nil {
					t.Errorf("drain: %v", err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != events {
		t.Fatalf("delivered %d distinct events, want %d", len(seen), events)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("event %s delivered %d times, want exactly once", id, count)
		}
	}
	if c := unpublishedCount(t, db); c != 0 {
		t.Fatalf("%d rows still unpublished, want 0", c)
	}
}
