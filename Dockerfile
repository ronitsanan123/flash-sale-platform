# Build and integration images include CGO, GCC and libpq headers so the Go
# race detector and the libpq-backed PostgreSQL adapter are available.
FROM golang:1.23-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev libpq-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY prototype/ ./
ENV CGO_ENABLED=1
RUN go build -o /out/flashsale ./cmd/flashsale \
    && go build -o /out/mockpsp ./cmd/mockpsp \
    && go build -o /out/worker ./cmd/worker \
    && go build -o /out/flashctl ./cmd/flashctl \
    && go build -o /out/reconcile ./cmd/reconcile \
    && go build -o /out/demo ./cmd/demo \
    && go build -o /out/demo-naive ./cmd/demo-naive

FROM golang:1.23-bookworm AS integration
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev libpq-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY prototype/ ./
ENV CGO_ENABLED=1
CMD ["sleep", "infinity"]

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/flashsale"]
