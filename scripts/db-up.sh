#!/usr/bin/env bash
# Start the local Postgres and wait until it accepts connections.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d db
for _ in $(seq 1 30); do
  if docker compose exec -T db pg_isready -U postgres >/dev/null 2>&1; then
    echo "postgres ready on localhost:5433"
    exit 0
  fi
  sleep 1
done
echo "postgres did not become ready" >&2
exit 1
