// Command producer enqueues a few demo events in a single transaction, to show the write side of the
// pattern: the events commit atomically with (here, standing in for) the business write. Run the
// worker alongside to watch them get delivered.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/raby/nestbox"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	dsn := env("DATABASE_URL", "postgres://nestbox:nestbox@localhost:5432/nestbox?sslmode=disable")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		logger.Error("open database", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, nestbox.Schema); err != nil {
		logger.Error("apply schema", "err", err)
		os.Exit(1)
	}

	count := envInt("COUNT", 5)
	species := []string{"Turdus merula", "Erithacus rubecula", "Apus apus", "Falco peregrinus", "Streptopelia turtur"}

	// One transaction for all of it: in a real service the business insert would be here too, and it
	// commits with the events or not at all.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error("begin", "err", err)
		os.Exit(1)
	}
	for i := 0; i < count; i++ {
		sightingID := newUUID()
		payload := fmt.Sprintf(`{"sightingId":%q,"species":%q,"count":%d}`, sightingID, species[i%len(species)], i+1)
		e := nestbox.Event{
			ID:            newUUID(),
			AggregateType: "sighting",
			AggregateID:   sightingID,
			Type:          "SightingRecorded",
			Payload:       []byte(payload),
			OccurredAt:    time.Now().UTC(),
		}
		if err := nestbox.Append(ctx, tx, e); err != nil {
			_ = tx.Rollback()
			logger.Error("append", "err", err)
			os.Exit(1)
		}
	}
	if err := tx.Commit(); err != nil {
		logger.Error("commit", "err", err)
		os.Exit(1)
	}
	logger.Info("enqueued events in one transaction", "count", count)
}

// newUUID returns a random RFC 4122 version-4 UUID string, without pulling in a UUID dependency.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand should never fail; fall back to a time-seeded value rather than panicking.
		binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
