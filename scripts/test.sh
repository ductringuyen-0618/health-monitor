#!/usr/bin/env bash
# Lint and run every test. Store tests need TEST_DATABASE_URL; run scripts/db-up.sh first.
set -euo pipefail
cd "$(dirname "$0")/.."
export TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgres://postgres:postgres@localhost:5433/monitor?sslmode=disable}"
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
  echo "gofmt needed on: $unformatted" >&2
  exit 1
fi
go vet ./...
go test ./... "$@"
