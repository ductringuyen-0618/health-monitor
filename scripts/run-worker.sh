#!/usr/bin/env bash
# Run the poller against the local compose Postgres.
set -euo pipefail
cd "$(dirname "$0")/.."
export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5433/monitor?sslmode=disable}"
export MODE=worker
exec go run ./cmd/monitor
