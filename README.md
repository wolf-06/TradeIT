# TradeIT — Microservices Trading Engine

A trading system split into two independent Go services that communicate through Redis
queues, with PostgreSQL for persistence:

- **User-Order Service** — HTTP REST API (port `8000`) for users, orders, cancellation and
  price modification.
- **Engine Service** — queue-driven order matching engine (no HTTP listener); it consumes
  the incoming/cancel/modify queues, matches orders, and persists trades and order status.

```
Client ──HTTP──▶ User-Order Service :8000 ──▶ Redis (DB 0) IncomingOrder
                                                   │
                                                   ▼
                                          Engine Service (queue workers)
                                                   │
                              ┌────────────────────┴────────────────────┐
                              ▼                                          ▼
                     Redis (DB 1) replies                     PostgreSQL (trades, status)
                              │
                              ▼
                    User-Order Service (blocks on reply)
```

## Order matching engine

The matcher is an **integer-priced, array-based (ladder) limit order book**:

- Prices are `int64` in a fixed scaled unit (`models.PriceScale = 100`); the user-order
  service converts once at the boundary, so the matcher only ever compares integers.
- Each side is a **ladder**: an array of price levels indexed by `price - origin` plus an
  occupancy bitmap, so price lookup is an array index and best price is a short bitmap scan
  instead of an O(log n) heap walk.
- Resting orders at a level form a FIFO doubly linked list; `orderTable` maps an order id to
  its node so cancel and modify are O(1).
- Trade registration and order-status writes are handed to **batched background writers**,
  so the match path never blocks on PostgreSQL.

This replaced the original heap + price-level-map implementation.

## Tech stack

| Area | Choice |
|---|---|
| Language | Go `1.23` (`go.mod`: `go 1.23.5`) |
| HTTP | Gin |
| ORM | GORM |
| Database | PostgreSQL 18 |
| Queue / cache | Redis 7 |
| Auth | JWT (`internal/middleware`) |
| Containers | Docker / Docker Compose |

## Repository layout

```
cmd/
  user-order-service/     HTTP API entry point
  engine-service/         matching engine entry point
  engine-benchmark/       matcher benchmark harness (CLI)
services/
  user-order-service/     controller / routes / services / repo
  engine-service/         engine/ (matcher, ledger, trade writers), services/
internal/
  database/               DB connection + init
  models/                 shared models (Order, Trade, User, price helpers)
  middleware/             JWT auth + user details
  benchmark/              workload generator + latency stats
  testing/                engine + cancellation/modify scenario tests
docker-compose.yml        PostgreSQL + Redis (service containers are optional)
```

## Prerequisites

- Go `1.23` or newer
- Docker + Docker Compose (to run PostgreSQL and Redis)

## Configuration

Copy the sample environment file and fill it in:

```bash
cp .env.example .env
```

| Variable | Purpose | Example |
|---|---|---|
| `jwtkey` | JWT signing key (required; the service refuses to start if empty) | `change-me` |
| `redis_addr` | Redis address | `localhost:6379` |
| `redis_password` | Redis password (empty for local) | `""` |
| `db_host` | PostgreSQL host | `localhost` |
| `db_port` | PostgreSQL port | `5432` |
| `db_user` | PostgreSQL user | `postgres` |
| `db_name` | PostgreSQL database | `tradeit` |
| `db_passwd` | PostgreSQL password | `dev123` |

## Quick start

```bash
# 1. Start PostgreSQL and Redis
docker compose up -d db redis

# 2. Run the three services (each in its own terminal)
go run ./cmd/user-order-service     # HTTP API on :8000
go run ./cmd/engine-service         # ring consumer (metrics/health on :8001)
go run ./cmd/projection-service     # event log -> Postgres + Redis cache (metrics/health on :8002)
```

Set `TRADEIT_CONFIG=config/tradeit.toml` (it is in `.env.example`) so all three services
share the symbol table and the shared-memory ingest directory. With `ingest.enabled = true`
in that file, orders flow through the shared-memory ring instead of the legacy Redis queue.

## Demo: shared-memory ring pipeline

The whole stack runs in containers, sharing one ingest volume:

```bash
docker compose up -d --build db redis user-order-service engine-service projection-service

# Place orders and verify they become queryable through the read path.
go run ./cmd/loadgen -n 20000 -c 64 -verify 200

# End-to-end smoke test (brings the stack up, drives load, checks read-back).
scripts/smoke.sh 20000 64

# Recovery demo: kill -9 the engine mid-load and show no order is lost.
scripts/recovery.sh 20000 64
```

Observability (Prometheus + Grafana with a provisioned dashboard):

```bash
docker compose --profile observability up -d
# Grafana:    http://localhost:3000  (anonymous viewer, admin/admin)
# Prometheus: http://localhost:9090
```

Every service exposes `/metrics`, `/healthz` and `/readyz`. The key panel is the pipeline
lag: `tradeit_consumer_watermark - tradeit_projection_seq`.

## API

All routes except `register`/`login` require the JWT in the `Authorization` header
(the raw token, not a `Bearer` prefix).

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/register` | no | Register a user |
| `POST` | `/login` | no | Log in and receive a JWT |
| `PUT` | `/user/update/email` | yes | Update email |
| `PUT` | `/user/update/password` | yes | Update password |
| `GET` | `/user/fund` | yes | Get account funds |
| `GET` | `/order/` | yes | List the user's orders |
| `POST` | `/order/sort` | yes | Query orders with filters |
| `POST` | `/order/create` | yes | Place an order (202 in ring mode) |
| `POST` | `/order/cancel` | yes | Cancel a resting order (202) |
| `POST` | `/order/modify` | yes | Reduce quantity or re-price (202) |
| `GET` | `/order/:id` | yes | One order's status (cache -> Postgres) |
| `GET` | `/healthz`, `/readyz`, `/metrics` | no | Ops endpoints |

## Redis queues

| Queue / key | DB | Producer → Consumer |
|---|---|---|
| `IncomingOrder` | 0 | user-order → engine (new orders) |
| `cancel` | 1 | user-order → engine (cancel requests) |
| `cancel:<orderId>` | 1 | engine → user-order (cancel reply) |
| `PriceModifyQueue` | 1 | user-order → engine (price modifications) |
| `PriceModifyQueue:<orderId>` | 1 | engine → user-order (modify reply) |

The user-order service blocks on the reply key for cancel/modify, so the engine must be
running for those calls to return.

## Testing

```bash
go test ./...
```

Most packages need no external services. The engine and model tests run standalone:

```bash
go test ./services/engine-service/engine/ ./internal/models/ ./internal/testing/oldbench/ -count=1
```

The scenario tests in `internal/testing` and `internal/testing/engine_test` need
PostgreSQL and Redis (`docker compose up -d`). Those two packages share one database and
reuse fixed order ids, so run them **one package at a time** to avoid cross-package
contention:

```bash
go test ./internal/testing/ -count=1
go test ./internal/testing/engine_test/ -count=1
```

## Benchmarking the matching engine

`cmd/engine-benchmark` generates a production-like order flow — the same JSON payloads
`repo.CreateOrder` pushes onto the Redis `IncomingOrder` list — and drives the matcher with
it. It reports throughput and per-order latency (min/mean/p50/p90/p99/p99.9/max), trade
counts, resting depth and price levels.

```bash
# Pure in-memory matcher throughput (no Redis, no database)
go run ./cmd/engine-benchmark -mode match -orders 200000

# Build the production-like queue and leave it for a running engine service
go run ./cmd/engine-benchmark -mode load -orders 100000 -reset

# Consume the queue exactly like engine.OrderEngine.ProcessOrderQueue does
go run ./cmd/engine-benchmark -mode drain

# Load, then drain (end-to-end: Redis round trip + decode + match)
go run ./cmd/engine-benchmark -mode all -orders 100000 -reset

# Machine-readable report
go run ./cmd/engine-benchmark -mode match -orders 200000 -json

# Idiomatic Go benchmark instead of the CLI
go test -run '^$' -bench BenchmarkMatcherProductionLike -benchtime 1x ./internal/benchmark/
```

The workload mimics production: several symbols trading around a slowly drifting mid on
discrete tick levels, a configurable limit/market mix, both sides and varied quantities.
Tune it with `-symbols`, `-tick-bps`, `-spread-ticks`, `-aggressive`, `-market-ratio`,
`-min-qty`/`-max-qty`, `-users`, `-mid-drift` and `-seed` (reproducible runs).

By default the matcher runs without PostgreSQL writes (`engine.PersistenceEnabled = false`)
so the numbers isolate the in-memory matching path; pass `-persist` to keep production
behaviour (trade registration plus order status updates), which requires the database.

An order of magnitude difference between `-mode match` and the queue modes is expected: the
queue modes include one Redis `BLPOP` round trip per order, which is exactly the consumer
path (and bottleneck) the engine service uses in production.

### Benchmark environment

Numbers are only meaningful with a correct host. The set below was collected on:

| Item | Value |
|---|---|
| CPU | AMD Ryzen 7 4700U (4C/8T, 15 W) |
| Clocksource | `tsc` (`/sys/devices/system/clocksource/clocksource0/current_clocksource`) |
| CPU governor | `performance` (boost enabled, ~3.66 GHz observed) |
| Go | `go1.27.0 linux/amd64` |
| Persistence | disabled (`-mode match`, pure in-memory matcher) |
| Seed | fixed, so both books see the identical flow |

A slow clocksource (e.g. `hpet`) makes `time.Now()` ~2 µs, and since the harness reads the
clock per order it inflates **both** throughput and latency for every book. Always check the
clocksource and governor before trusting sub-microsecond figures.

### Results: ladder vs. the previous heap book

Same host, same seed, one process at a time; `ratio` is ladder vs heap. Raw output lives in
the local `benchmarks/` directory (not committed).

**4-symbol production-like flow, 500k orders**

| Metric | Heap | Ladder | Ratio |
|---|---|---|---|
| Throughput | 672,320 /s | **2,573,627 /s** | **3.83x** |
| p50 | 701 ns | **230 ns** | 3.05x |
| p99 | 7.10 µs | **1.08 µs** | 6.56x |
| p99.9 | 13.21 µs | **5.62 µs** | 2.35x |

**1-symbol (AAPL), 1,000,000 orders**

| Metric | Heap | Ladder | Ratio |
|---|---|---|---|
| Throughput | 754,852 /s | **2,848,099 /s** | **3.77x** |
| p50 | 633 ns | **211 ns** | 3.00x |
| p99 | 6.56 µs | **0.94 µs** | 6.96x |
| p99.9 | 11.83 µs | **4.21 µs** | 2.81x |

**Go benchmark `BenchmarkMatcherProductionLike` (100k, 4 symbols)**

| Metric | Heap | Ladder | Ratio |
|---|---|---|---|
| orders/s | 815,868 | **3,983,908** | **4.88x** |
| B/op | 40,297,040 | **10,740,616** | 3.75x less |
| allocs/op | 557,234 | **46,300** | 12.03x fewer |

**Insert-only synthetic (wide sparse range, ~211k levels)**

| Metric | Heap | Ladder | Ratio |
|---|---|---|---|
| Throughput | **1,430,969 /s** | 965,979 /s | 0.68x |
| max | **1.69 ms** | 20.63 ms | 0.08x |

**End-to-end through Redis, 30k orders (`-mode all`)**

| Metric | Heap | Ladder | Ratio |
|---|---|---|---|
| Throughput | 17,350 /s | 19,777 /s | 1.14x |

### Findings

- The ladder is **~3.8x faster on match-heavy flows** (CLI) and **~4.9x** in the Go
  benchmark, with a **6–7x better p99** — the tail win comes from decoupled persistence and
  node pooling.
- **Insert-only into a wide sparse range is the ladder's weak spot:** ~32% slower and a 12x
  worse max, because the window must span the whole price range and `grow` reallocates. A
  segmented/chunked window (or a heap-of-levels fallback for very sparse ranges) is the
  natural next step.
- **End-to-end is ingestion-bound** (~17–21k/s) for both books: one `BLPOP` round trip per
  order dominates. Batched reads (`LMPOP`/`XREADGROUP`) are the next real win.

## Development notes

- Business logic lives in `services/*/`; shared code in `internal/`; entry points in `cmd/`.
- The matcher is covered by `services/engine-service/engine/orderbook_ladder_test.go` and
  the scenario tests under `internal/testing/`.
