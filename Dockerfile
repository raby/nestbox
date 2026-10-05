# syntax=docker/dockerfile:1

# Build the worker and producer. They live in the example module (which brings the Postgres driver);
# the root nestbox library module it depends on is dependency-free and resolved by a local replace.
FROM golang:1.23 AS build
WORKDIR /src
COPY . .
WORKDIR /src/example
RUN go mod download \
 && CGO_ENABLED=0 go build -trimpath -o /out/worker ./worker \
 && CGO_ENABLED=0 go build -trimpath -o /out/producer ./producer

# Minimal runtime: a distroless static image, non-root. ENTRYPOINT is the worker; the producer binary
# is also present (docker compose run producer).
FROM gcr.io/distroless/static-debian12 AS runtime
COPY --from=build /out/worker /worker
COPY --from=build /out/producer /producer
USER nonroot:nonroot
ENTRYPOINT ["/worker"]
