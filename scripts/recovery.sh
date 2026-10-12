#!/usr/bin/env bash
# Recovery demo: drive load while killing and restarting the matching engine, and show
# that no order is lost or duplicated. The engine restores its book from the last snapshot
# and replays the applied WAL tail; the producer keeps publishing (durably) meanwhile, and
# the consumer's dedup makes the replay idempotent.
#
# Usage: scripts/recovery.sh [orders] [concurrency]
set -euo pipefail

cd "$(dirname "$0")/.."

ORDERS="${1:-20000}"
CONCURRENCY="${2:-64}"

echo "==> placing ${ORDERS} orders at concurrency ${CONCURRENCY} while restarting the engine"
go run ./cmd/loadgen -n "${ORDERS}" -c "${CONCURRENCY}" -verify 200 -verify-timeout 60s &
LOADGEN=$!

sleep 2
echo "==> kill -9 the engine"
docker kill tradeit_engine_service >/dev/null
sleep 2
echo "==> restart the engine (restores snapshot + replays WAL tail)"
docker start tradeit_engine_service >/dev/null

if ! wait "$LOADGEN"; then
  echo "!!! recovery demo failed: orders were lost or not projected" >&2
  exit 1
fi

echo "==> recovery demo passed: every sampled order was projected despite the restart"
