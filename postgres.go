package nestbox

import (
	"context"
	"database/sql"
	"fmt"
)

// Schema is the DDL for the outbox table. Apply it with your migration tool, or run it once at
// startup (the example worker does the latter). published_at IS NULL means "not yet delivered"; the
// partial index keeps the relay's claim query fast as delivered rows pile up.
const Schema = `
CREATE TABLE IF NOT EXISTS outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (occurred_at) WHERE published_at IS NULL;
`

const (
	claimBatchSQL = `SELECT id, aggregate_type, aggregate_id, event_type, payload, occurred_at
FROM outbox
WHERE published_at IS NULL
ORDER BY occurred_at
LIMIT $1
FOR UPDATE SKIP LOCKED`

	markPublishedSQL = `UPDATE outbox SET published_at = now() WHERE id = $1`
)

// PostgresStore is a Store backed by Postgres. Drain claims rows with FOR UPDATE SKIP LOCKED, so
// concurrent relays never block on or double-claim each other's rows.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore wraps an open *sql.DB (any database/sql driver that speaks Postgres, e.g. pgx).
func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }

// Drain implements Store. It opens a transaction, locks up to batch unpublished rows (skipping rows
// locked by other relays), delivers each in order and marks it published, then commits. Any handler
// error rolls the whole transaction back, so the batch is retried later and nothing is lost.
func (s *PostgresStore) Drain(ctx context.Context, batch int, h Handler) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once Commit succeeds

	rows, err := tx.QueryContext(ctx, claimBatchSQL, batch)
	if err != nil {
		return 0, fmt.Errorf("claim batch: %w", err)
	}
	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.Type, &e.Payload, &e.OccurredAt); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows: %w", err)
	}

	for _, e := range events {
		if err := h.Handle(ctx, e); err != nil {
			return 0, fmt.Errorf("handle %s: %w", e.ID, err) // rollback whole batch; retried later
		}
		if _, err := tx.ExecContext(ctx, markPublishedSQL, e.ID); err != nil {
			return 0, fmt.Errorf("mark published %s: %w", e.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(events), nil
}
