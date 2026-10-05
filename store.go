package nestbox

import "context"

// Store is the relay's view of the outbox.
type Store interface {
	// Drain, in a single transaction, claims up to batch unpublished events, delivers each to h in
	// order, and marks the delivered ones published. It returns the number delivered.
	//
	// If h returns an error the whole transaction is rolled back — nothing is marked published and the
	// batch is retried on a later drain — so handlers must be idempotent. Implementations claim rows in
	// a way that is safe to run concurrently (the Postgres store uses FOR UPDATE SKIP LOCKED).
	Drain(ctx context.Context, batch int, h Handler) (int, error)
}
