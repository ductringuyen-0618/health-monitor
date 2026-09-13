#!/usr/bin/env bash
# Black-box production verification driver for the live health-monitor app.
#
#   BASE=https://<app>.ondigitalocean.app APP_ID=<doctl app id> ./scripts/verify-prod.sh all
#
# Sub-commands: init s1 s2 s3 s4 s5 s6 logs6 s7 s8 cleanup all
#
# Re-runnable: state (run id, webhook.site tokens, target ids) lives in
# $OUTDIR/state.env, so an interrupted run resumes with the same run id and the
# same tokens. Every target this script registers carries ?v=<run-id>-... in its
# URL and `cleanup` deletes exactly those, never anything else.
#
# No jq on the dev machine: all JSON is parsed with python3.
set -uo pipefail

BASE="${BASE:-https://health-monitor-gyx32.ondigitalocean.app}"
APP_ID="${APP_ID:-d2017e54-d876-42bd-81c8-63c2a29c0c3a}"
OUTDIR="${OUTDIR:-tmp/verify}"
CURL_MAX_TIME="${CURL_MAX_TIME:-30}"

mkdir -p "$OUTDIR"
STATE="$OUTDIR/state.env"
[ -f "$STATE" ] && . "$STATE"
RUN_ID="${RUN_ID:-$(date +%s)}"

save() { # save KEY VALUE
  grep -v "^export $1=" "$STATE" > "$STATE.tmp" 2>/dev/null || : > "$STATE.tmp"
  printf 'export %s=%s\n' "$1" "$2" >> "$STATE.tmp"
  mv "$STATE.tmp" "$STATE"
  export "$1=$2"
}

nowz() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say()  { printf '\n--- %s  (%s)\n' "$*" "$(nowz)"; }
c()    { curl -s -m "$CURL_MAX_TIME" "$@"; }

# pyj EXPR -- stdin is JSON, bound to d
pyj() { python -c "import sys,json
d=json.load(sys.stdin)
print($1)"; }

# one-line summary of a target JSON on stdin
tsum() { python -c "import sys,json
t=json.load(sys.stdin)
print('status=%s failures=%s code=%s err=%s checked=%s' % (
  t['status'], t['consecutive_failures'], t['last_status_code'],
  json.dumps(t['last_error']), t['last_checked_at']))"; }

reg() { # reg URL [WEBHOOK_URL] -> prints the created target JSON
  if [ -n "${2:-}" ]; then
    c -X POST "$BASE/targets" -H 'Content-Type: application/json' \
      -d "{\"url\":\"$1\",\"webhook_url\":\"$2\"}"
  else
    c -X POST "$BASE/targets" -H 'Content-Type: application/json' -d "{\"url\":\"$1\"}"
  fi
}

# ---- webhook.site helpers -------------------------------------------------
ws_token() { c -X POST https://webhook.site/token | pyj "d['uuid']"; }

ws_set() { # ws_set UUID STATUS -- set the token's default response status
  c -X PUT "https://webhook.site/token/$1" -H 'Content-Type: application/json' \
    -d "{\"default_status\":$2,\"default_content\":\"verify-$RUN_ID\",\"default_content_type\":\"text/plain\"}" \
    -o /dev/null -w "PUT token/$1 default_status=$2 -> %{http_code}\n"
  sleep 1   # free tier rate-limits; space PUTs by at least a second
}

# ws_hist -- histogram of target.down payloads at the receiver, by target_id
ws_hist() {
  c "https://webhook.site/token/$WS_RECEIVER/requests?sorting=newest&per_page=50" | python -c "
import sys,json,collections
d=json.load(sys.stdin)
items=d.get('data',d if isinstance(d,list) else [])
n=collections.Counter()
for it in items:
    try: b=json.loads(it.get('content') or '')
    except Exception: continue
    if isinstance(b,dict) and b.get('event')=='target.down':
        n[b['target_id']]+=1
print('receiver requests fetched:', len(items))
for k,v in sorted(n.items()): print('%s  %d' % (k,v))
"
}

ws_payload() { # ws_payload TARGET_ID -- raw target.down bodies for one target
  c "https://webhook.site/token/$WS_RECEIVER/requests?sorting=newest&per_page=50" | python -c "
import sys,json
tid=sys.argv[1]
d=json.load(sys.stdin)
for it in d.get('data',[]):
    try: b=json.loads(it.get('content') or '')
    except Exception: continue
    if isinstance(b,dict) and b.get('event')=='target.down' and b.get('target_id')==tid:
        print(it.get('created_at'), it.get('content'))
" "$1"
}

# poll ID SECONDS [LABEL] -- one line per 3s poll of GET /targets/{id}
poll() {
  local id="$1" secs="$2" label="${3:-}" i n
  n=$(( secs / 3 ))
  for i in $(seq 1 "$n"); do
    printf 't+%03ds %s ' "$(( i * 3 ))" "$label"
    c "$BASE/targets/$id" | tsum
    sleep 3
  done
}

# ---------------------------------------------------------------- scenarios
init() {
  save RUN_ID "$RUN_ID"
  say "init run $RUN_ID against $BASE"
  [ -z "${WS_RECEIVER:-}" ] && save WS_RECEIVER "$(ws_token)"
  sleep 1
  [ -z "${WS_CTRL1:-}" ] && save WS_CTRL1 "$(ws_token)"
  sleep 1
  [ -z "${WS_CTRL2:-}" ] && save WS_CTRL2 "$(ws_token)"
  echo "RUN_ID=$RUN_ID"
  echo "receiver   https://webhook.site/$WS_RECEIVER"
  echo "ctrl1      https://webhook.site/$WS_CTRL1"
  echo "ctrl2      https://webhook.site/$WS_CTRL2"
}

s1() {
  say "S1 baseline"
  echo '$ GET /healthz'
  c -w '\n-> %{http_code}\n' "$BASE/healthz"
  echo '$ GET /targets'
  c -w '\n-> %{http_code}\n' "$BASE/targets" | python -c "
import sys,json
raw=sys.stdin.read()
body,_,code=raw.rpartition('-> ')
d=json.loads(body)
print('json type:', type(d).__name__, 'len:', len(d))
print('-> ' + code.strip())"
  echo '$ POST /targets with url not-a-url'
  c -w '\n-> %{http_code}\n' -X POST "$BASE/targets" -H 'Content-Type: application/json' \
    -d '{"url":"not-a-url"}'
  echo '$ POST /targets with url ftp://example.com/'
  c -w '\n-> %{http_code}\n' -X POST "$BASE/targets" -H 'Content-Type: application/json' \
    -d '{"url":"ftp://example.com/"}'
  echo '$ POST /targets (first registration, then the same url again)'
  local u id
  u="https://example.com/?v=$RUN_ID-s1-1"
  id=$(reg "$u" | pyj "d['id']")
  echo "first -> id=$id"
  c -w '\n-> %{http_code}\n' -X POST "$BASE/targets" -H 'Content-Type: application/json' \
    -d "{\"url\":\"$u\"}"
  echo '$ DELETE /targets/00000000-0000-0000-0000-000000000000 (unknown id)'
  c -w '\n-> %{http_code}\n' -X DELETE "$BASE/targets/00000000-0000-0000-0000-000000000000"
  echo '$ GET /targets/00000000-0000-0000-0000-000000000000'
  c -w '\n-> %{http_code}\n' "$BASE/targets/00000000-0000-0000-0000-000000000000"
}

s2() {
  say "S2 consecutive failures -> DOWN plus exactly one webhook"
  ws_set "$WS_CTRL1" 500
  local id
  id=$(reg "https://webhook.site/$WS_CTRL1?v=$RUN_ID-s2-1" "https://webhook.site/$WS_RECEIVER" | pyj "d['id']")
  save S2_ID "$id"
  echo "target id: $id  url: https://webhook.site/$WS_CTRL1?v=$RUN_ID-s2-1"
  poll "$id" 60 "s2"
  echo '$ receiver histogram after DOWN'
  ws_hist
  echo '$ raw target.down payload'
  ws_payload "$id"
  say "S2 two more intervals: counter rises, status stays DOWN, receiver count stays 1"
  poll "$id" 36 "s2"
  ws_hist
}

s3() {
  say "S3 recovery is silent, re-alert on the next transition"
  ws_set "$WS_CTRL1" 200
  poll "$S2_ID" 36 "s3-up"
  echo '$ receiver histogram after recovery (must still be 1)'
  ws_hist
  say "S3 back to 500"
  ws_set "$WS_CTRL1" 500
  poll "$S2_ID" 54 "s3-down"
  echo '$ receiver histogram after the second transition (must be 2)'
  ws_hist
  ws_payload "$S2_ID"
}

s4() {
  say "S4 a single failure must not flip status"
  ws_set "$WS_CTRL2" 200
  local id i st f
  id=$(reg "https://webhook.site/$WS_CTRL2?v=$RUN_ID-s4-1" "https://webhook.site/$WS_RECEIVER" | pyj "d['id']")
  save S4_ID "$id"
  echo "target id: $id"
  for i in $(seq 1 15); do
    st=$(c "$BASE/targets/$id" | pyj "d['status']")
    printf 't+%02ds status=%s\n' "$(( i * 3 ))" "$st"
    [ "$st" = "UP" ] && break
    sleep 3
  done
  say "S4 switch to 500, poll every 2s for the first failure"
  ws_set "$WS_CTRL2" 500
  : > "$OUTDIR/s4-polls.txt"
  for i in $(seq 1 40); do
    c "$BASE/targets/$id" | tsum | tee -a "$OUTDIR/s4-polls.txt"
    f=$(tail -1 "$OUTDIR/s4-polls.txt" | sed 's/.*failures=\([0-9][0-9]*\).*/\1/')
    if [ "$f" -ge 1 ] 2>/dev/null; then break; fi
    sleep 2
  done
  say "S4 first failure seen, switching straight back to 200"
  ws_set "$WS_CTRL2" 200
  for i in $(seq 1 12); do
    c "$BASE/targets/$id" | tsum | tee -a "$OUTDIR/s4-polls.txt"
    sleep 3
  done
  echo '$ every status value observed for this target'
  sed 's/ failures.*//' "$OUTDIR/s4-polls.txt" | sort | uniq -c
  echo '$ receiver histogram (this target must be absent)'
  ws_hist
}

s5() {
  say "S5 transport failures count as failures"
  local id u i
  : > "$OUTDIR/s5-ids.txt"
  for u in "https://httpbin.org/delay/10?v=$RUN_ID-s5-1" \
           "https://nonexistent-host-$RUN_ID.invalid/?v=$RUN_ID-s5-2" \
           "http://127.0.0.1:1/?v=$RUN_ID-s5-3"; do
    id=$(reg "$u" "https://webhook.site/$WS_RECEIVER" | pyj "d['id']")
    echo "$id $u" | tee -a "$OUTDIR/s5-ids.txt"
  done
  for i in $(seq 1 18); do
    printf 't+%03ds\n' "$(( i * 3 ))"
    while read -r id u; do
      printf '  %-52s ' "${u#*//}"
      c "$BASE/targets/$id" | tsum
    done < "$OUTDIR/s5-ids.txt"
    sleep 3
  done
  echo '$ full JSON of each transport-failure target'
  while read -r id u; do c "$BASE/targets/$id"; echo; done < "$OUTDIR/s5-ids.txt"
  echo '$ receiver histogram (one per transport target)'
  ws_hist
  echo '$ raw payloads'
  while read -r id u; do ws_payload "$id"; done < "$OUTDIR/s5-ids.txt"
}

s6() {
  say "S6 concurrent polling, 40 targets, no duplicate checks"
  local n t0 t1
  # One file per background curl. Concurrent >> appends from separate MSYS
  # processes lose writes, which silently truncates the histogram.
  rm -rf "$OUTDIR/s6-codes"; mkdir -p "$OUTDIR/s6-codes"
  t0=$(date +%s.%N)
  for n in $(seq 1 40); do
    curl -s -m "$CURL_MAX_TIME" -o /dev/null -w '%{http_code}\n' -X POST "$BASE/targets" \
      -H 'Content-Type: application/json' \
      -d "{\"url\":\"https://example.com/?v=$RUN_ID-conc-$n\"}" > "$OUTDIR/s6-codes/$n" &
  done
  wait
  t1=$(date +%s.%N)
  echo "40 parallel POSTs, wall time: $(python -c "print('%.2fs' % ($t1-$t0))")"
  echo 'status code histogram (one line per POST):'
  cat "$OUTDIR"/s6-codes/* | tr -d '\r' | sort | uniq -c
  say "S6 waiting for all 40 to be checked"
  for n in $(seq 1 12); do
    printf 't+%03ds ' "$(( n * 5 ))"
    c "$BASE/targets" | python -c "
import sys,json
run=sys.argv[1]
d=json.load(sys.stdin)
mine=[t for t in d if ('v=%s-conc-' % run) in t['url']]
print('registered=%d checked=%d up=%d' % (len(mine),
  sum(1 for t in mine if t['last_checked_at']),
  sum(1 for t in mine if t['status']=='UP')))" "$RUN_ID"
    sleep 5
  done
  c "$BASE/targets" | python -c "
import sys,json
run=sys.argv[1]
d=json.load(sys.stdin)
mine=[t for t in d if ('v=%s-conc-' % run) in t['url']]
open('$OUTDIR/s6-target-ids.txt','w').write('\n'.join(t['id'] for t in mine))
print('final: %d targets, checked=%d up=%d' % (len(mine),
  sum(1 for t in mine if t['last_checked_at']),
  sum(1 for t in mine if t['status']=='UP')))" "$RUN_ID"
}

logs6() {
  say "S6 log analysis: checks per target per 15s window"
  doctl apps logs "$APP_ID" poller --type run --tail 2000 > "$OUTDIR/s6-logs.txt" 2>&1
  wc -l < "$OUTDIR/s6-logs.txt" | sed 's/^/poller log lines pulled: /'
  echo 'poller instances seen (component prefix on each log line):'
  awk '{print $1}' "$OUTDIR/s6-logs.txt" | sort | uniq -c
  python - "$OUTDIR/s6-logs.txt" "$OUTDIR/s6-target-ids.txt" <<'PY'
import sys, json, collections, datetime
logf, idf = sys.argv[1], sys.argv[2]
ids = set(l.strip() for l in open(idf) if l.strip())
win = collections.Counter()
claims = collections.Counter()
for line in open(logf, encoding='utf-8', errors='replace'):
    i = line.find('{')
    if i < 0:
        continue
    inst = line[:i].split()[0] if line[:i].split() else '?'
    try:
        r = json.loads(line[i:])
    except Exception:
        continue
    if r.get('msg') == 'claimed':
        claims[inst] += 1
    elif r.get('msg') == 'checked' and r.get('target') in ids:
        ts = datetime.datetime.fromisoformat(r['time'].replace('Z', '+00:00'))
        win[(r['target'], int(ts.timestamp()) // 15)] += 1
print('claimed lines by instance:', dict(claims))
print('distinct instances emitting claimed:', len(claims))
print('S6 targets appearing in the log:', len(set(t for t, _ in win)))
print('(target, 15s window) buckets:', len(win))
print('checks per target per 15s window ->', dict(collections.Counter(win.values())))
dupes = [k for k, v in win.items() if v > 1]
print('buckets with more than one check:', len(dupes), dupes[:5])
PY
}

s7() {
  say "S7 spike: 150 more targets"
  local n t0 t1 pids sampler i
  rm -rf "$OUTDIR/s7-codes"; mkdir -p "$OUTDIR/s7-codes"; : > "$OUTDIR/s7-healthz.txt"
  ( for n in $(seq 1 20); do
      curl -s -m 20 -o /dev/null -w '%{http_code} %{time_total}\n' "$BASE/healthz" >> "$OUTDIR/s7-healthz.txt"
      sleep 2
    done ) &
  sampler=$!
  pids=""
  t0=$(date +%s.%N)
  for n in $(seq 1 150); do
    curl -s -m "$CURL_MAX_TIME" -o /dev/null -w '%{http_code}\n' -X POST "$BASE/targets" \
      -H 'Content-Type: application/json' \
      -d "{\"url\":\"https://example.com/?v=$RUN_ID-spike-$n\"}" > "$OUTDIR/s7-codes/$n" &
    pids="$pids $!"
  done
  wait $pids
  t1=$(date +%s.%N)
  echo "150 parallel POSTs, wall time: $(python -c "print('%.2fs' % ($t1-$t0))")"
  echo 'status code histogram (one line per POST):'
  cat "$OUTDIR"/s7-codes/* | tr -d '\r' | sort | uniq -c
  say "S7 time until all 190 example.com targets have last_checked_at"
  for i in $(seq 1 18); do
    printf 't+%06.1fs ' "$(python -c "import time;print(time.time()-$t1)")"
    c "$BASE/targets" | python -c "
import sys,json
run=sys.argv[1]
d=json.load(sys.stdin)
mine=[t for t in d if ('v=%s-conc-' % run) in t['url'] or ('v=%s-spike-' % run) in t['url']]
print('example.com targets=%d unchecked=%d up=%d' % (len(mine),
  sum(1 for t in mine if not t['last_checked_at']),
  sum(1 for t in mine if t['status']=='UP')))" "$RUN_ID"
    sleep 5
  done
  wait $sampler
  echo '$ GET /healthz samples taken during the spike (code time_total)'
  cat "$OUTDIR/s7-healthz.txt"
  python -c "
v=[l.split() for l in open('$OUTDIR/s7-healthz.txt') if l.strip()]
t=[float(x[1]) for x in v]
print('healthz samples=%d codes=%s min=%.3fs max=%.3fs' % (len(v), sorted(set(x[0] for x in v)), min(t), max(t)))"
  say "S7 claimed lines with backlog during the spike"
  doctl apps logs "$APP_ID" poller --type run --tail 2000 > "$OUTDIR/s7-logs.txt" 2>&1
  grep -o '"msg":"claimed","count":[0-9]*,"backlog":[0-9]*,"in_flight":[0-9]*' "$OUTDIR/s7-logs.txt" | tail -30
  echo 'backlog values seen across all claimed lines:'
  grep -o '"backlog":[0-9]*' "$OUTDIR/s7-logs.txt" | sort | uniq -c
  echo 'HTTP status codes returned by the API during the spike (from the code histogram above)'
}

s8() {
  say "S8 target deleted mid-flight"
  # The DELETE must land while a check is actually running, and the dev machine
  # clock is skewed from the container clock, so absolute scheduling off
  # next_check_at is not reliable. Instead watch the row itself: the poller
  # pushes next_check_at forward as a lease at CLAIM time and only writes
  # last_checked_at when the check COMPLETES. next_check_at moving while
  # last_checked_at stands still means the check is in flight right now.
  local url id i snap cur
  url="https://httpbin.org/delay/4?v=$RUN_ID-s8-1"
  id=$(reg "$url" | pyj "d['id']")
  save S8_ID "$id"
  echo "target id: $id  url: $url"
  for i in $(seq 1 20); do
    c "$BASE/targets/$id" | tsum
    if c "$BASE/targets/$id" | pyj "d['last_checked_at'] or ''" | grep -q .; then break; fi
    sleep 3
  done
  echo '$ full JSON before the watch'
  c "$BASE/targets/$id"; echo
  snap=$(c "$BASE/targets/$id" | pyj "'%s|%s' % (d['next_check_at'], d['last_checked_at'])")
  echo "watching every 0.5s for next_check_at to move while last_checked_at stands still"
  echo "  baseline next_check_at|last_checked_at = $snap"
  for i in $(seq 1 60); do
    cur=$(c "$BASE/targets/$id" | pyj "'%s|%s' % (d['next_check_at'], d['last_checked_at'])")
    if [ "$cur" != "$snap" ]; then
      echo "  observed  next_check_at|last_checked_at = $cur"
      case "$cur" in
        *"|${snap#*|}") echo "  claimed: lease moved, result not written yet -> check is IN FLIGHT"; break ;;
        *) echo "  a completed check landed instead, resetting the baseline"; snap="$cur" ;;
      esac
    fi
    sleep 0.5
  done
  echo "$ DELETE /targets/$id at $(nowz)"
  c -w '\n-> %{http_code}\n' -X DELETE "$BASE/targets/$id"
  echo "$ GET /targets/$id"
  c -w '\n-> %{http_code}\n' "$BASE/targets/$id"
  echo 'waiting 25s for the in-flight check to land, then grepping the poller log'
  sleep 25
  doctl apps logs "$APP_ID" poller --type run --tail 1500 > "$OUTDIR/s8-logs.txt" 2>&1
  echo "$ grep -c 'record check failed'"
  grep -c 'record check failed' "$OUTDIR/s8-logs.txt"
  grep 'record check failed' "$OUTDIR/s8-logs.txt" | tail -5
  echo "$ grep '$id'"
  grep "$id" "$OUTDIR/s8-logs.txt" | tail -8
  echo '$ grep -c ERROR level lines'
  grep -c '"level":"ERROR"' "$OUTDIR/s8-logs.txt"
}

cleanup() {
  say "cleanup: delete every target whose url contains v=$RUN_ID"
  : > "$OUTDIR/cleanup-codes.txt"
  c "$BASE/targets" | python -c "
import sys,json
run=sys.argv[1]
d=json.load(sys.stdin)
for t in d:
    if ('v=%s-' % run) in t['url']: print(t['id'])" "$RUN_ID" > "$OUTDIR/cleanup-ids.raw"
  # python on Windows writes CRLF; a stray CR in the path makes curl report 000
  tr -d '\r' < "$OUTDIR/cleanup-ids.raw" > "$OUTDIR/cleanup-ids.txt"
  echo "targets to delete: $(grep -c . "$OUTDIR/cleanup-ids.txt")"
  local id code n=0
  while read -r id; do
    [ -z "$id" ] && continue
    code=$(c -o /dev/null -w '%{http_code}' -X DELETE "$BASE/targets/$id")
    echo "$code" >> "$OUTDIR/cleanup-codes.txt"
    n=$(( n + 1 ))
  done < "$OUTDIR/cleanup-ids.txt"
  echo "delete calls issued: $n"
  echo 'delete status histogram:'
  sort "$OUTDIR/cleanup-codes.txt" | uniq -c
  say "cleanup verification: no v=$RUN_ID target may remain"
  c "$BASE/targets" | python -c "
import sys,json
run=sys.argv[1]
d=json.load(sys.stdin)
left=[t['url'] for t in d if run in t['url']]
print('targets still registered:', len(d))
print('targets matching this run id:', len(left), left)
for t in d: print('  KEPT (not created by this run):', t['id'], t['url'])" "$RUN_ID"
}

all() { init; s1; s2; s3; s4; s5; s6; logs6; s7; s8; cleanup; }

cmd="${1:-all}"; shift || true
"$cmd" "$@"
