#!/usr/bin/env bash
# Register one healthy and one failing target, then wait for the failing one
# to go DOWN. Works locally (defaults) and against the live app:
#   BASE=https://<app>.ondigitalocean.app OK_URL=https://httpbin.org/status/200 \
#   BAD_URL=https://httpbin.org/status/500 HOOK_URL=https://webhook.site/<id> ./scripts/demo.sh
set -euo pipefail
BASE="${BASE:-http://localhost:8080}"
OK_URL="${OK_URL:-http://localhost:9090/ok}"
BAD_URL="${BAD_URL:-http://localhost:9090/bad}"
HOOK_URL="${HOOK_URL:-http://localhost:9090/hook}"
SUFFIX="$(date +%s)"

json_field() { python -c "import sys,json; print(json.load(sys.stdin)['$1'])"; }

echo "== healthz"; curl -sf "$BASE/healthz"; echo
echo "== register healthy target"
OK_ID=$(curl -sf -X POST "$BASE/targets" -H 'Content-Type: application/json' \
  -d "{\"url\":\"$OK_URL?d=$SUFFIX\",\"webhook_url\":\"$HOOK_URL\"}" | json_field id)
echo "== register failing target"
BAD_ID=$(curl -sf -X POST "$BASE/targets" -H 'Content-Type: application/json' \
  -d "{\"url\":\"$BAD_URL?d=$SUFFIX\",\"webhook_url\":\"$HOOK_URL\"}" | json_field id)
echo "healthy=$OK_ID failing=$BAD_ID"

for i in $(seq 1 40); do
  STATUS=$(curl -sf "$BASE/targets/$BAD_ID" | json_field status)
  echo "t+$((i*3))s failing target status: $STATUS"
  if [ "$STATUS" = "DOWN" ]; then
    echo "== failing target"; curl -sf "$BASE/targets/$BAD_ID"; echo
    echo "== healthy target"; curl -sf "$BASE/targets/$OK_ID"; echo
    if [[ "$HOOK_URL" == http://localhost* ]]; then
      echo "== webhooks received by sink"; curl -sf "$HOOK_URL"; echo
    else
      echo "check $HOOK_URL for the target.down payload"
    fi
    exit 0
  fi
  sleep 3
done
echo "failing target never went DOWN" >&2
exit 1
