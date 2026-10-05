package nestbox

import (
	"context"
	"log/slog"
	"time"
)

// Relay drains a Store on an interval until its context is cancelled. It is the scheduling half of the
// outbox; the Store owns the atomic claim-deliver-mark step. Running several relays against the same
// Store is safe and is how you scale throughput — the Postgres store's SKIP LOCKED hands each relay a
// disjoint set of rows.
type Relay struct {
	store    Store
	handler  Handler
	batch    int
	interval time.Duration
	logger   *slog.Logger
}

// Option configures a Relay.
type Option func(*Relay)

// WithBatchSize sets how many events a single drain claims (default 100). A value below 1 is treated
// as 1.
func WithBatchSize(n int) Option { return func(r *Relay) { r.batch = n } }

// WithInterval sets how often the relay polls when idle (default 1s). A value of 0 or less keeps the
// default. When a drain returns a full batch the relay keeps going immediately, so a backlog is
// cleared without waiting for ticks.
func WithInterval(d time.Duration) Option { return func(r *Relay) { r.interval = d } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(r *Relay) { r.logger = l } }

// NewRelay builds a relay that drains store, delivering each event to handler.
func NewRelay(store Store, handler Handler, opts ...Option) *Relay {
	r := &Relay{store: store, handler: handler, batch: 100, interval: time.Second, logger: slog.Default()}
	for _, o := range opts {
		o(r)
	}
	// Guard against a misconfigured batch size: a batch of 0 would make every drain return 0 rows, and
	// the catch-up loop below (which stops only when a drain returns fewer than batch) would spin
	// forever delivering nothing.
	if r.batch < 1 {
		r.batch = 1
	}
	if r.interval <= 0 {
		r.interval = time.Second
	}
	return r
}

// Run drains the outbox until ctx is cancelled, then returns ctx.Err() (context.Canceled on a clean
// shutdown). Each wake-up drains repeatedly while batches come back full, so a backlog is caught up
// quickly; a drain error is logged and retried on the next tick rather than stopping the relay.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		for {
			n, err := r.store.Drain(ctx, r.batch, r.handler)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.logger.Error("outbox drain failed", "err", err)
				break // back off until the next tick
			}
			if n < r.batch {
				break // nothing left for now
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
