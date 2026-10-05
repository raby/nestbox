// Package nestbox is a tiny, zero-dependency transactional outbox for Postgres.
//
// Append events in the same transaction as your business writes, and a Relay delivers them
// at-least-once. The relay claims work with SELECT ... FOR UPDATE SKIP LOCKED, so any number of
// workers can run concurrently without double-delivering or blocking each other. The library package
// itself imports only the standard library; the example worker uses a Postgres driver.
package nestbox

import (
	"context"
	"database/sql"
	"time"
)

// Event is one row in the outbox: a fact to be delivered, enqueued atomically with the write that
// produced it.
type Event struct {
	ID            string // caller-supplied unique id (a UUID); the primary key
	AggregateType string // e.g. "sighting"
	AggregateID   string // the id of the aggregate the event is about
	Type          string // the event type, e.g. "SightingRecorded"
	Payload       []byte // the event body as JSON
	OccurredAt    time.Time
}

// Handler delivers an Event to its destination — a log, Kafka, SNS, a webhook, anything. It must be
// idempotent: the relay guarantees at-least-once delivery, so the same Event may arrive more than
// once (after a crash between delivery and commit, or when a batch is retried).
type Handler interface {
	Handle(ctx context.Context, e Event) error
}

// HandlerFunc adapts an ordinary function to a Handler.
type HandlerFunc func(ctx context.Context, e Event) error

// Handle calls f(ctx, e).
func (f HandlerFunc) Handle(ctx context.Context, e Event) error { return f(ctx, e) }

// execer is the slice of *sql.Tx (and *sql.DB) that Append needs. Taking an interface lets an event
// be enqueued on whatever transaction the caller is already using for the business write.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

const appendSQL = `INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, occurred_at)
VALUES ($1, $2, $3, $4, $5::jsonb, $6)`

// Append enqueues an event on the caller's transaction, so it commits atomically with the business
// write — the whole point of the transactional-outbox pattern. Pass the *sql.Tx you are already using
// for the write; nestbox adds the event to it and lets you commit everything together.
func Append(ctx context.Context, tx execer, e Event) error {
	_, err := tx.ExecContext(ctx, appendSQL,
		e.ID, e.AggregateType, e.AggregateID, e.Type, string(e.Payload), e.OccurredAt)
	return err
}
