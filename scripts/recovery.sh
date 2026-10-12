#!/usr/bin/env bash
# Recovery check: drive load while killing and restarting the matching engine, and show
# that no accepted order is lost. The engine restores its book from the last snapshot and
# replays the applied WAL tail; the producer keeps publishing (durably) meanwhile, and the
# consumer's dedup makes the replay idempotent.
#
# It prebuilds the load generator so the kill timing is not affected by a cold compile,
# waits until the engine has actually started applying this run's load before killing it,
# and fails if any request was rejected or any accepted order is not visible afterwards.
#
# Usage: scripts/recovery.sh [orders] [concurrency]
set -euo pipefail

cd "$(dirname "$0")/.."

ORDERS="${1:-20000}"
CONCURRENCY="${2:-64}"
BASE="${BASE:-http://localhost:8000}"

# Prebuild the load generator so the load does not wait on a compile.
BIN="$(mktemp -d)/loadgen"
go build -o "$BIN" ./cmd/loadgen

# applied reports the engine's applied-command count, or 0 when it is unreachable.
applied() {
  curl -fsS -m 2 http://localhost:8001/metrics 2>/dev/null \
    | awk '/^tradeit_commands_applied_total/{print $2; exit}' || echo 0
}

before="$(applied)"
echo "==> placing ${ORDERS} orders at concurrency ${CONCURRENCY} while restarting the engine"
"$BIN" -base "$BASE" -n "${ORDERS}" -c "${CONCURRENCY}" -verify-all -verify-timeout 120s &
LOADGEN=$!

# Wait until the engine has applied more than it had before, so the kill lands mid load.
for _ in $(seq 1 600); do
  if [ "$(applied)" -gt "${before:-0}" ]; then
    break
  fi
  sleep 0.1
done

echo "==> kill -9 the engine"
docker kill tradeit_engine_service >/dev/null
sleep 2
echo "==> restart the engine (restores snapshot + replays WAL tail)"
docker start tradeit_engine_service >/dev/null

if ! wait "$LOADGEN"; then
  echo "!!! recovery check failed: a request was rejected or an accepted order was not visible after the restart" >&2
  exit 1
fi

echo "==> recovery check passed: every accepted order was visible after the restart"
