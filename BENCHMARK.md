# TradeIT benchmarks

TradeIT is a trading engine built as three Go services. This document records what it does
under load, how the numbers were measured, and the improvements behind them.

## Headline results

| Metric | Result |
|---|---|
| Matching engine | 4.4M orders per second |
| Journaled ingest-to-match pipeline | 2.6M orders per second |
| HTTP API, same host | 19,938 orders per second at 6.4 ms median latency |
| Recovery | No accepted order lost through an engine kill and restart under load |

The ingest-to-match pipeline runs in process with the write-ahead log on tmpfs. It covers
encoding, WAL append, ring handoff, match, and event-log append, and it excludes HTTP and
PostgreSQL. See the measurement notes for why.

## The problem it solves

The first version pushed every order through Redis, and the engine blocked on a round trip
per order. That capped the engine near 20k orders per second, so the queue, not the matcher,
was the ceiling.

The rewrite moves ingestion into shared memory. The producer writes each command to a
write-ahead log and publishes it to a per-symbol ring in memory. The engine drains the ring,
matches, and logs the result to a durable event log. A projection service tails that log and
writes to PostgreSQL and Redis. No network hop sits between the order and the match.

## Ingestion path, before and after

Measured end to end through the engine on the same machine (Ryzen 7 4700U, 8 threads):

| Ingestion path | Throughput |
|---|---|
| Redis queue round trip | ~20k orders per second |
| Shared-memory ring | ~1.4M orders per second |

That is roughly a 70x improvement in the ingestion path, and it removes the queue as the
bottleneck.

## Matcher redesign: heap to ladder

The matcher was rebuilt from a heap and a price-level map into an integer-priced ladder,
where each side is an array indexed by price plus an occupancy bitmap. Price lookup becomes
an array index and the best price becomes a short bitmap scan, so the hot path stops walking
a tree.

Measured locally, same host, same seed, one process at a time (Ryzen 7 4700U):

| Flow | Heap | Ladder | Gain |
|---|---|---|---|
| 4 symbols, 500k orders | 672,320 per second | 2,573,627 per second | 3.8x |
| 1 symbol, 1M orders | 754,852 per second | 2,848,099 per second | 3.8x |

Latency on the 4-symbol flow:

| Percentile | Heap | Ladder | Gain |
|---|---|---|---|
| p50 | 701 ns | 230 ns | 3.0x |
| p99 | 7.10 us | 1.08 us | 6.6x |

Allocation drop in the Go benchmark (100k orders):

| Metric | Heap | Ladder | Gain |
|---|---|---|---|
| allocs per op | 557,234 | 46,300 | 12x fewer |
| bytes per op | 40.3 MB | 10.7 MB | 3.8x less |

The ladder wins hardest on the tail. A 6.6x better p99 comes from decoupled persistence and
node pooling, so the match path never blocks on the database.

## Current capability

Measured on AWS EC2 `c7i.2xlarge` (Intel Xeon Platinum 8488C, 8 threads, Go 1.23.5):

| Stage | Throughput | What it covers |
|---|---|---|
| Matcher alone | 4.4M orders per second | pure in-memory matching |
| Ring producer | 3.5M orders per second | encode, WAL append, ring publish, group commit |
| Ring consumer | 2.9M orders per second | drain, decode, match, event log append |
| Ring end-to-end | 2.6M orders per second | producer and consumer together, tmpfs WAL |

The pipeline retains about 59 percent of the raw matcher throughput after encoding, WAL
append, ring handoff, and event logging. The rest is the work that lets the engine recover
without replaying from the start.

## Scaling from a laptop to cloud hardware

The hot path is single threaded, so it scales with CPU clock and instructions per cycle.
Moving the same code from a developer laptop to an AWS EC2 `c7i.2xlarge`:

| Stage | Laptop (Ryzen 7 4700U) | Cloud (c7i.2xlarge) | Gain |
|---|---|---|---|
| Matcher | ~3.0M per second | 4.4M per second | 1.5x |
| Ring producer | ~2.4M per second | 3.5M per second | 1.5x |
| Ring consumer | ~1.6M per second | 2.9M per second | 1.8x |
| Ring end-to-end | ~1.4M per second | 2.6M per second | 1.9x |
| HTTP API | ~5.8k per second | 19,938 per second | 3.4x |

The engine gains 1.5 to 1.9x with no code change. The HTTP front door gains more, 3.4x,
because it is latency bound: the lower per-request latency on the cloud host lets more
requests run at once, and the median drops from 18.5 ms to 6.4 ms.

## HTTP API

The HTTP front door is the practical limit for a deployed service. Measured on the same EC2
box with the load generator on the host:

| Metric | Result |
|---|---|
| Throughput | 19,938 orders per second |
| p50 latency | 6.4 ms |
| p95 latency | 8.7 ms |
| p99 latency | 9.7 ms |
| Read back | 500 of 500 orders queryable in 97 ms |

Each request is accepted, written to the durable log, published to the ring, and answered
with 202. JSON parsing and HTTP framing set the ceiling here, not the engine. This is why the
API runs at about 20k per second while the engine behind it runs at millions per second.

## Durability without a throughput tax

The write-ahead logs use group commit: the producer fsyncs after a batch of appends or a
short time bound, whichever comes first. Batching the fsync turns the log from a per record
cost into a small fixed cost per batch.

Group-commit overhead on tmpfs:

| Write pattern | Throughput |
|---|---|
| Append with an fsync per record | 137 MB per second |
| Append with group commit (batch of 256) | 2.0 GB per second |

In the end-to-end run, 100,000 orders needed only 784 fsyncs, about 128 orders per sync.

## Recovery

The engine restores the last book snapshot and replays the applied log tail. A per-symbol
watermark makes the replay idempotent, and the producer replays any command the engine has
not applied.

Test: 100,000 orders placed at 20,006 orders per second while the engine is killed and
restarted mid load. Result: 200 of 200 sampled orders visible after the restart. The
projection service also resumes from a checkpoint committed in the same transaction as its
data, so a restart never double counts.

## Hot path allocations

The consumer started at five times the matcher's allocation count, which put avoidable
pressure on the garbage collector. A profile showed the cost was a heap-allocated trade id
per fill and a closure rebuilt on every order. Both are gone.

Allocations per 100,000-order iteration:

| Path | Before | After |
|---|---|---|
| Matcher (baseline) | 46k | 46k |
| Ring consumer | 229k | 47k |

The consumer is now within about 2 percent of the raw matcher.

## Hot path latency

Throughput alone hides the tail, and the tail is what a matching engine lives or dies by.
The in-process benchmarks now report a per-operation latency distribution (mean, p50, p90,
p99, p99.9, max) next to `orders/sec`:

- `p*-ns/order` for the matcher and the ring consumer: the latency of one matching command.
  The consumer times `Handler.Apply`, so it covers decode, match, and trade collection but
  not the applied-WAL append that follows it.
- `p*-ns/batch` for the ring producer: one `SubmitOrders` call, which is 512 orders.

Latency is sampled — one order in 16 by default — because a clock read costs roughly 20-30
ns and timing every order would inflate a ~230 ns/order match and depress the throughput
number it is reported beside. Set `BENCH_LATENCY_STRIDE=1` for a latency-focused run with
full tail resolution, or a larger stride to spend even less on the clock. The figures land
in the raw report's "Latency (sampled)" section, which takes the median across the `-count`
repetitions.

## How the numbers were measured

- The hot path benchmarks run in process on tmpfs, so the write-ahead log fsync is not a
  confound. They isolate the ring, the durable log, and the matcher.
- The hot-path benchmarks also report a sampled per-operation latency distribution; see
  [Hot path latency](#hot-path-latency) for what each unit covers.
- Each benchmark runs three times and the median is reported.
- The HTTP numbers come from the bundled load generator, which registers a user, sends
  orders, and verifies read back through the API.
- The improvement comparison ran on a Ryzen 7 4700U (8 threads). The current capability ran
  on an AWS EC2 `c7i.2xlarge` (8 threads, Go 1.23.5) with `GOMAXPROCS` set to the core count.
- The raw report behind the current capability numbers is committed at
  [docs/benchmarks/2026-10-12-ec2-c7i2xlarge.md](docs/benchmarks/2026-10-12-ec2-c7i2xlarge.md),
  with the commit SHA, host, commands, workload parameters, and the per-run medians.

Reproduce everything with one command:

```bash
scripts/bench.sh
```

It collects the environment, the hot path benchmarks, the component micro-benchmarks, and,
when the stack is running, the end-to-end load test, pipeline metrics, and database counts
into a single report.
