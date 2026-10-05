package nestbox

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeStore delivers queued events through the handler, like a real store but in memory, so the relay
// scheduling can be tested without Postgres.
type fakeStore struct {
	mu    sync.Mutex
	queue []Event
}

func (s *fakeStore) Drain(ctx context.Context, batch int, h Handler) (int, error) {
	s.mu.Lock()
	n := batch
	if n > len(s.queue) {
		n = len(s.queue)
	}
	take := append([]Event(nil), s.queue[:n]...)
	s.queue = s.queue[n:]
	s.mu.Unlock()

	for _, e := range take {
		if err := h.Handle(ctx, e); err != nil {
			return 0, err
		}
	}
	return len(take), nil
}

func TestRelayDeliversAllQueuedEventsInOrderThenStopsOnCancel(t *testing.T) {
	events := []Event{{ID: "1", Type: "A"}, {ID: "2", Type: "B"}, {ID: "3", Type: "C"}}
	store := &fakeStore{queue: append([]Event(nil), events...)}

	var mu sync.Mutex
	var got []Event
	done := make(chan struct{})
	handler := HandlerFunc(func(_ context.Context, e Event) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, e)
		if len(got) == len(events) {
			close(done)
		}
		return nil
	})

	// BatchSize 2 vs 3 events exercises the catch-up loop: the first drain returns a full batch, so
	// the relay drains again immediately rather than waiting for the next tick.
	relay := NewRelay(store, handler, WithInterval(5*time.Millisecond), WithBatchSize(2))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- relay.Run(ctx) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not receive all events in time")
	}

	cancel()
	if err := <-errCh; err != context.Canceled {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(events) {
		t.Fatalf("delivered %d events, want %d", len(got), len(events))
	}
	for i := range events {
		if got[i].ID != events[i].ID {
			t.Fatalf("event %d: delivered id %q, want %q", i, got[i].ID, events[i].ID)
		}
	}
}
