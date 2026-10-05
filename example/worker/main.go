// Command worker is a small runnable outbox relay: it drains the Postgres outbox and "publishes" each
// event (here, by logging it — swap the sink for Kafka/SNS/a webhook). Run several instances against
// one database to scale; SKIP LOCKED keeps them from stepping on each other. Stops cleanly on
// SIGINT/SIGTERM.
package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/raby/nestbox"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dsn := env("DATABASE_URL", "postgres://nestbox:nestbox@localhost:5432/nestbox?sslmode=disable")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		logger.Error("open database", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	// Demo convenience: ensure the table exists. In a real service this is a migration.
	if _, err := db.ExecContext(context.Background(), nestbox.Schema); err != nil {
		logger.Error("apply schema", "err", err)
		os.Exit(1)
	}

	// The sink. A real one would publish to a broker; this one logs, and returning nil marks the
	// event published. Returning an error would leave it for a later drain (at-least-once).
	sink := nestbox.HandlerFunc(func(_ context.Context, e nestbox.Event) error {
		logger.Info("published",
			"type", e.Type,
			"aggregate", e.AggregateType+"/"+e.AggregateID,
			"payload", string(e.Payload))
		return nil
	})

	relay := nestbox.NewRelay(nestbox.NewPostgresStore(db), sink,
		nestbox.WithBatchSize(envInt("BATCH", 100)),
		nestbox.WithInterval(time.Duration(envInt("POLL_MS", 1000))*time.Millisecond),
		nestbox.WithLogger(logger),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("nestbox worker started")
	if err := relay.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("relay stopped with error", "err", err)
		os.Exit(1)
	}
	logger.Info("nestbox worker stopped cleanly")
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
