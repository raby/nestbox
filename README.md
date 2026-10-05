# nestbox

A tiny, **zero-dependency** [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) for Postgres, in Go.

Write your event in the **same transaction** as the business change that caused it, and a relay delivers
it **at-least-once** — so you never lose an event to a crash between "commit the row" and "publish the
message", and you never publish one for a transaction that rolled back. The relay claims work with
`SELECT … FOR UPDATE SKIP LOCKED`, so you can run **as many workers as you like**: they share the
backlog without double-delivering or blocking each other.

The library module imports only the Go standard library (`database/sql`), so `go get
github.com/raby/nestbox` pulls in nothing else. A Postgres driver is needed only to open the
connection; the runnable worker and producer that use one live in a separate `example/` module (see
[Project layout](#project-layout)), so the dependency never reaches a consumer of the library.

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
- **Best-effort FIFO, not a total order.** A single drain claims and delivers oldest-first
  (`ORDER BY occurred_at`), but ordering is *not* a guarantee across the whole stream: run several
  relays and each takes a disjoint set with no ordering between them, and even one relay can deliver a
  slow-committing older event after a faster newer one. Don't rely on nestbox for strict ordering; rely
  on it for at-least-once delivery.

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

## Operating

A few things worth knowing before you run this in anger:

- **Handlers run inside the claiming transaction.** A batch holds its rows locked (and a database
  connection, and the VACUUM horizon) for as long as delivery takes, so keep handlers fast and lower
  the batch size if a handler does slow per-event I/O. This is the deliberate trade for the clean
  all-or-nothing rollback.
- **Prune published rows.** nestbox never deletes anything; the partial index keeps the claim query
  fast regardless, but the table grows forever otherwise. Run a periodic sweep, e.g.
  `DELETE FROM outbox WHERE published_at < now() - interval '7 days'`.
- **Drain errors retry on the next tick.** A database outage logs one error per interval and retries;
  there is no exponential backoff, so widen the interval (or wrap the logger) if a sustained outage is
  noisy.

## Payload

`Event.Payload` is stored in a `jsonb` column, so it must be valid, non-empty JSON. jsonb normalizes on
the round-trip (insignificant whitespace dropped, object keys reordered), so the delivered bytes are
JSON-equal to what you appended, not byte-identical. If you need byte-exact payloads, use a `text` or
`bytea` column instead.

## Schema

`nestbox.Schema` holds the DDL (an `outbox` table plus a partial index on undelivered rows). Apply it
with your migration tool, or run it once at startup as the example worker does.

## Project layout

```
.            the nestbox library module — stdlib only, no dependencies
example/     a separate module: the worker, the producer, and the Postgres integration tests
```

The driver (pgx) and the integration tests live in `example/` precisely so the library module stays
dependency-free for anyone who imports it.

## Test

```sh
go test ./...                       # library unit tests, no database needed

cd example                          # the Postgres-backed integration + concurrency tests
DATABASE_URL=postgres://nestbox:nestbox@localhost:5432/nestbox?sslmode=disable go test ./...
```

The integration tests skip unless `DATABASE_URL` is set, so both commands stay green without a database.

## License

MIT — see [LICENSE](LICENSE).
