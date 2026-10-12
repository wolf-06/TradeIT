#!/usr/bin/env bash
# End-to-end smoke test: bring up the full ring pipeline, drive load, and verify that the
# placed orders become queryable through the read path (cache -> Postgres).
#
# Usage: scripts/smoke.sh [orders] [concurrency]
set -euo pipefail

cd "$(dirname "$0")/.."

ORDERS="${1:-5000}"
CONCURRENCY="${2:-64}"

wait_health() {
  local url="$1" name="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "    $name healthy"
      return 0
    fi
    sleep 2
  done
  echo "!!! $name did not become healthy ($url)" >&2
  docker compose logs --tail 40 "$name" || true
  return 1
}

echo "==> starting the stack"
docker compose up -d --build db redis user-order-service engine-service projection-service

echo "==> waiting for services"
wait_health http://localhost:8000/healthz user-order-service
wait_health http://localhost:8001/healthz engine-service
wait_health http://localhost:8002/healthz projection-service

echo "==> placing ${ORDERS} orders at concurrency ${CONCURRENCY}"
go run ./cmd/loadgen -n "${ORDERS}" -c "${CONCURRENCY}" -verify 200 -verify-timeout 30s

echo "==> smoke test passed"
