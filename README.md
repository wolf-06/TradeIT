# TradeIT

TradeIT is a trading engine built as three Go services. A user-order service accepts orders
over HTTP and publishes them to a shared-memory ring. An engine service consumes the ring
and matches orders on an in-memory order book. A projection service turns the engine's
durable event log into PostgreSQL rows and a Redis cache that brokers read back.

The matching engine sustains 4.4M orders per second, and the full ring pipeline (durable
write, ring handoff, match, and event log) sustains 2.6M orders per second end to end.

## Run it locally

You need Docker and Docker Compose. Everything runs in containers.

```bash
cp .env.example .env            # set your Postgres password and JWT key
docker compose up -d --build    # db, redis, and the three services
docker compose ps               # wait until all five show healthy
```

Place orders and read them back through the full pipeline:

```bash
go run ./cmd/loadgen -n 20000 -c 64 -verify 200
```

The load generator registers a user, sends orders, and verifies that they become queryable
through `GET /order/:id`. To run it inside the compose network instead:

```bash
docker compose --profile bench run --rm loadgen
```

Add dashboards:

```bash
docker compose --profile observability up -d
# Grafana    http://localhost:3000
# Prometheus http://localhost:9090
```

Stop everything:

```bash
docker compose down          # keep data
docker compose down -v       # wipe data
```

## Architecture

Three services, and one shared-memory ring per symbol.

```
Client --HTTP--> user-order-service :8000
                     |
                     | publish (durable in the producer WAL)
                     v
              shared-memory ring (one per symbol)
                     |
                     | consume
                     v
              engine-service :8001   (in-memory order book)
                     |
                     | durable event log (applied records and trades)
                     v
              projection-service :8002
                     |
        +------------+------------+
        v                         v
   PostgreSQL                 Redis cache
   (source of truth)          (order status)
```

How it works:

- The user-order service writes each command to a write-ahead log, publishes it to the
  symbol's ring, and returns `202 Accepted` with the order id. The write-ahead log is the
  durability point, so no database write happens on the request path.
- The engine service drains the ring, applies commands to the order book, and logs every
  applied command and trade to a durable event log. The order book is an integer-priced
  ladder, so price lookup is an array index and the best price is a short bitmap scan.
- The projection service tails the event log and writes orders and trades to PostgreSQL and
  the Redis status cache. The database is the source of truth and the cache is best effort.
- Brokers poll `GET /order/:id` for status. Reads check the cache first and fall back to the
  database.

Why a ring instead of a queue:

The original design pushed every order through Redis, and the engine blocked on a round trip
per order. That capped the engine near 20k orders per second. The shared-memory ring removes
the network hop: the producer and consumer hand off fixed-size records in memory, and
durability comes from the write-ahead logs on either side. The engine is no longer the
bottleneck.

Recovery:

- The producer replays any command the engine has not applied when it restarts.
- The engine restores the last book snapshot and replays the applied log tail. A per-symbol
  watermark makes the replay idempotent.
- The projection service resumes from a checkpoint committed in the same transaction as its
  data, so a restart never double counts.

`scripts/recovery.sh` kills the engine mid load and shows that no order is lost.

## Configuration

Copy the template and set your values:

```bash
cp .env.example .env
```

`.env` holds the database and Redis connection details and the JWT signing key. The services
read it through godotenv, and Docker Compose reads the same file for substitution. Use plain
`KEY=value` with no spaces or quotes.

| Variable | Purpose |
|---|---|
| `db_host`, `db_port`, `db_user`, `db_passwd`, `db_name` | PostgreSQL connection |
| `redis_addr`, `redis_password` | Redis connection |
| `jwtkey` | JWT signing key (required) |
| `TRADEIT_CONFIG` | Path to the shared TOML config |

`config/tradeit.toml` holds the shared settings: the symbol list, the ring size, the group
commit thresholds, and the snapshot and projection knobs. All services load the same file,
so the symbol list is the single source of truth for symbol ids.

## API

All routes except `register` and `login` require the JWT in the `Authorization` header (the
raw token, with no `Bearer` prefix).

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/register` | no | Register a user |
| POST | `/login` | no | Log in and receive a JWT |
| GET | `/order/` | yes | List the user's orders |
| POST | `/order/sort` | yes | Query orders with filters |
| POST | `/order/create` | yes | Place an order (202) |
| POST | `/order/cancel` | yes | Cancel a resting order (202) |
| POST | `/order/modify` | yes | Reduce quantity or re-price (202) |
| GET | `/order/:id` | yes | One order's status (cache, then database) |
| GET | `/healthz`, `/readyz`, `/metrics` | no | Ops endpoints |

## Observability

Every service exposes `/metrics`, `/healthz`, and `/readyz`. The dashboard's key panel is
pipeline lag, the gap between the engine watermark and the projection position. Prometheus
and Grafana run under the `observability` profile with a provisioned dashboard.

## Benchmarking

`scripts/bench.sh` collects the environment, the in-process hot path, the component
micro-benchmarks, and, when the stack is up, the end-to-end load test, pipeline metrics, and
database counts into one markdown report.

```bash
scripts/bench.sh                 # -> benchmarks/bench-<timestamp>.md
RECOVERY=yes scripts/bench.sh    # also run the recovery demo
E2E=no scripts/bench.sh          # in-process only, no stack needed
PROFILE=yes scripts/bench.sh     # also capture CPU and allocation profiles
```

Results and the improvements behind them are in [BENCHMARK.md](BENCHMARK.md).

## Repository layout

```
cmd/
  user-order-service/    HTTP API and ring producer
  engine-service/        ring consumer and matching engine
  projection-service/    event log to PostgreSQL and Redis
  loadgen/               load generator for tests and benchmarks
services/                service code (controllers, engine, producer)
internal/
  shmring/               shared-memory ring
  wal/                   durable append-only log
  ipc/                   command, outcome, and trade records
  ingest/                producer and consumer wiring
  projection/            projector, PostgreSQL store, Redis cache
  snapshot/              compressed book snapshots
  control/               shared watermark region
  config/                TOML config and symbol table
  metrics/               Prometheus metrics
scripts/                 smoke, recovery, and benchmark scripts
deploy/                  Prometheus and Grafana provisioning
```

## Testing

```bash
go test ./...
```

Most packages need no external services. The database backed tests under `internal/testing/`
need PostgreSQL and Redis running.

## Development notes

- Entry points live in `cmd/`, service code in `services/`, and shared code in `internal/`.
- The matching engine is covered by the tests under `services/engine-service/engine/`.
- Run `gofmt` and `go vet ./...` before committing.
