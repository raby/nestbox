package nestbox

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
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
)

// markPublishedSQL builds a single UPDATE that marks every id in one statement, e.g.
// "UPDATE outbox SET published_at = now() WHERE id IN ($1, $2, $3)". An IN-list with one placeholder
// per id (rather than a Postgres array) keeps the statement portable across any database/sql driver —
// the store needs only database/sql, not a driver that can encode a Go slice as an array.
func markPublishedSQL(n int) string {
	placeholders := make([]string, n)
	for i := range placeholders {
		placeholders[i] = "$" + strconv.Itoa(i+1)
	}
	return "UPDATE outbox SET published_at = now() WHERE id IN (" + strings.Join(placeholders, ", ") + ")"
}

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

	if len(events) == 0 {
		return 0, nil // nothing claimed; the empty commit/rollback is harmless but skip it
	}

	ids := make([]any, len(events))
	for i, e := range events {
		if err := h.Handle(ctx, e); err != nil {
			return 0, fmt.Errorf("handle %s: %w", e.ID, err) // rollback whole batch; retried later
		}
		ids[i] = e.ID
	}

	// Every event in the batch delivered (any handler error returns above and rolls the whole
	// transaction back), so mark them all published in a single statement rather than one per row.
	if _, err := tx.ExecContext(ctx, markPublishedSQL(len(ids)), ids...); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(events), nil
}
