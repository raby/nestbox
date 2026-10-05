# nestbox

A tiny, **zero-dependency** [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) for Postgres, in Go.

Write your event in the **same transaction** as the business change that caused it, and a relay delivers
it **at-least-once** — so you never lose an event to a crash between "commit the row" and "publish the
message", and you never publish one for a transaction that rolled back. The relay claims work with
`SELECT … FOR UPDATE SKIP LOCKED`, so you can run **as many workers as you like**: they share the
backlog without double-delivering or blocking each other.

The library package imports only the Go standard library (`database/sql`). A Postgres driver is needed
only to open the connection — the example worker uses [pgx](https://github.com/jackc/pgx).

## The idea

A service that both writes to its database and publishes events has a dual-write problem: commit the
row then crash before publishing, and the event is lost; publish then roll back, and you've announced
something that never happened. The outbox makes the event part of the same transaction:

```go
tx, _ := db.BeginTx(ctx, nil)

// ... your business write(s) on tx ...

_ = nestbox.Append(ctx, tx, nestbox.Event{
    ID:            newUUID(),
    AggregateType: "sighting",
    AggregateID:   sightingID,
    Type:          "SightingRecorded",
    Payload:       []byte(`{"species":"Turdus merula"}`),
    OccurredAt:    time.Now().UTC(),
})

tx.Commit() // the row and the event commit together, or neither does
```

A relay then drains the outbox and delivers each event to your `Handler`:

```go
sink := nestbox.HandlerFunc(func(ctx context.Context, e nestbox.Event) error {
    return publishToKafka(ctx, e) // or SNS, a webhook, anything
})

relay := nestbox.NewRelay(nestbox.NewPostgresStore(db), sink)
relay.Run(ctx) // blocks until ctx is cancelled; run several for throughput
```

## Guarantees

- **At-least-once.** A drain locks a batch, delivers it, and marks it published in one transaction. If
  the handler fails, the transaction rolls back and the batch is retried later — so handlers must be
  **idempotent**.
- **Safe to run concurrently.** `FOR UPDATE SKIP LOCKED` hands each relay a disjoint set of rows, so
  horizontal scaling is just running more workers. No leader election, no partitioning.
- **Ordered within a drain.** Events are claimed and delivered oldest-first (`ORDER BY occurred_at`).

## Run the demo

```sh
docker compose up --build -d        # Postgres + the relay worker
docker compose run --rm producer    # enqueue 5 events in one transaction
docker compose logs worker          # watch them get published
docker compose down -v
```

Scale the relay and watch SKIP LOCKED share the work:

```sh
docker compose up --build -d --scale worker=3
```

## Test

```sh
go test ./...                                   # unit tests (no database needed)
DATABASE_URL=postgres://nestbox:nestbox@localhost:5432/nestbox?sslmode=disable go test ./...
```

The Postgres-backed tests skip unless `DATABASE_URL` is set.

## Schema

`nestbox.Schema` holds the DDL (an `outbox` table plus a partial index on undelivered rows). Apply it
with your migration tool, or run it once at startup as the example worker does.

## License

MIT — see [LICENSE](LICENSE).
