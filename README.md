# TradeIT - Microservices Trading Engine

A microservices-based trading system with separate services for user management/order placement and order matching/execution.

## Architecture

The project is organized into two independent microservices:

- **User-Order Service** (Port 8000): HTTP REST API for user authentication and order management
- **Engine Service**: Queue-based order matching and execution engine

For detailed architecture documentation, see [MICROSERVICES_STRUCTURE.md](MICROSERVICES_STRUCTURE.md).

## Quick Start

### Prerequisites
- Go 1.21+
- Docker & Docker Compose
- PostgreSQL 13+
- Redis 7+

### Build

```bash
# Build user-order-service
go build ./cmd/user-order-service

# Build engine-service
go build ./cmd/engine-service
```

### Run with Docker

```bash
docker-compose -f docker-compose.yml up --build
```

Services will be available at:
- User-Order Service: `http://localhost:8000`
- PostgreSQL: `localhost:5432`
- Redis: `localhost:6379`

## Project Structure

```
├── cmd/                    Service entry points
├── services/              Service implementations
├── internal/              Shared packages
├── docker-compose.yml     Docker orchestration
└── go.mod                Go module file
```

## Documentation

- [MICROSERVICES_STRUCTURE.md](MICROSERVICES_STRUCTURE.md) - Complete architecture and design
- [RESTRUCTURE_CHECKLIST.md](RESTRUCTURE_CHECKLIST.md) - Implementation checklist

## Environment Setup

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Update with your database and Redis credentials.

## API Endpoints

### User Management
- `POST /register` - Register new user
- `POST /login` - Login user

### Order Management  
- `POST /order/create` - Place new order
- `GET /order` - Get all orders
- `POST /order/sort` - Get orders with filters

All endpoints except register/login require JWT authentication.

## Development

The codebase follows the standard Go project layout:
- Business logic in `services/*/` directories
- Shared utilities in `internal/` directory
- Entry points in `cmd/` directory

See [MICROSERVICES_STRUCTURE.md](MICROSERVICES_STRUCTURE.md) for detailed information.

## Benchmarking the Matching Engine

`cmd/engine-benchmark` generates a production-like order flow - the same JSON payloads
`repo.CreateOrder` pushes onto the Redis `IncomingOrder` list - and drives the matcher with
it. It reports throughput and per-order latency (min/mean/p50/p90/p99/max), trade counts,
resting depth and price levels.

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
so the numbers isolate the in-memory matching path; pass `-persist` to keep the production
behaviour (trade registration plus order status updates), which requires the database and
shows how much those writes dominate.

An order of magnitude difference between `-mode match` and the queue modes is expected: the
queue modes include one Redis `BLPOP` round trip per order, which is exactly the consumer
path (and bottleneck) the engine service uses in production.
