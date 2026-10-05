package nestbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// openTestDB connects to the Postgres named by DATABASE_URL, applies the schema and truncates the
// outbox. The test skips when DATABASE_URL is unset, so `go test` stays green without a database
// (run it with, e.g., DATABASE_URL=postgres://nestbox:nestbox@localhost:5432/nestbox?sslmode=disable).
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), Schema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.Exec("TRUNCATE outbox"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("TRUNCATE outbox")
		db.Close()
	})
	return db
}

func appendN(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		e := Event{
			ID:            fmt.Sprintf("%08d-0000-4000-8000-000000000000", i),
			AggregateType: "sighting",
			AggregateID:   "agg",
			Type:          "SightingRecorded",
			Payload:       []byte(fmt.Sprintf(`{"n":%d}`, i)),
			OccurredAt:    time.Now().UTC(),
		}
		if err := Append(ctx, tx, e); err != nil {
			_ = tx.Rollback()
			t.Fatalf("append: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func unpublishedCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestPostgresDrainDeliversAndMarksPublished(t *testing.T) {
	db := openTestDB(t)
	appendN(t, db, 3)

	store := NewPostgresStore(db)
	var got []Event
	h := HandlerFunc(func(_ context.Context, e Event) error { got = append(got, e); return nil })

	n, err := store.Drain(context.Background(), 10, h)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 3 || len(got) != 3 {
		t.Fatalf("drained %d, delivered %d; want 3 and 3", n, len(got))
	}

	// Everything is published now, so a second drain finds nothing.
	n, err = store.Drain(context.Background(), 10, h)
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if n != 0 {
		t.Fatalf("second drain returned %d, want 0", n)
	}
	if c := unpublishedCount(t, db); c != 0 {
		t.Fatalf("%d rows still unpublished, want 0", c)
	}
}

func TestPostgresDrainRollsBackWholeBatchOnHandlerError(t *testing.T) {
	db := openTestDB(t)
	appendN(t, db, 2)

	store := NewPostgresStore(db)
	h := HandlerFunc(func(_ context.Context, _ Event) error { return errors.New("sink is down") })

	n, err := store.Drain(context.Background(), 10, h)
	if err == nil {
		t.Fatal("expected a drain error when the handler fails")
	}
	if n != 0 {
		t.Fatalf("drained %d, want 0 on failure", n)
	}
	// At-least-once: a failed delivery leaves everything unpublished to retry later.
	if c := unpublishedCount(t, db); c != 2 {
		t.Fatalf("%d rows unpublished after failure, want 2", c)
	}
}
