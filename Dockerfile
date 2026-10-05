# syntax=docker/dockerfile:1

# Build the worker and producer as static binaries.
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -trimpath -o /out/producer ./cmd/producer

# Minimal runtime: a distroless static image, non-root. ENTRYPOINT is the worker; the producer binary
# is also present (docker compose run producer).
FROM gcr.io/distroless/static-debian12 AS runtime
COPY --from=build /out/worker /worker
COPY --from=build /out/producer /producer
USER nonroot:nonroot
ENTRYPOINT ["/worker"]
