// The example module: the runnable worker and producer, plus the Postgres-backed
// integration tests. These need a database driver (pgx), so they live in their own
// module — that keeps the root nestbox library module free of any dependency, so
// `go get github.com/raby/nestbox` pulls nothing but the standard library.
module github.com/raby/nestbox/example

go 1.23

require (
	github.com/jackc/pgx/v5 v5.7.2
	github.com/raby/nestbox v0.0.0-00010101000000-000000000000
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)

replace github.com/raby/nestbox => ../
