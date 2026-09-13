# Production verification (Task 11)

| | |
|---|---|
| Run id | `1789330577` |
| Live URL | `https://health-monitor-gyx32.ondigitalocean.app` |
| App id | `d2017e54-d876-42bd-81c8-63c2a29c0c3a` |
| **Poller instance count observed** | **1** |
| Poller instance size | `apps-s-1vcpu-0.5gb` |
| UTC start | 2026-09-13T20:16:17Z |
| UTC end | 2026-09-13T20:33:31Z |
| Driver | `scripts/verify-prod.sh` |
| Live settings in effect | `POLL_INTERVAL=15s`, `CHECK_TIMEOUT=5s`, `MAX_CONCURRENT_CHECKS=20` |

The instance count was read two ways and agrees: every line of `doctl apps logs ... poller`
carries the single component prefix `poller` (1301 of 1301 lines), and the live spec reports
`instance_count: 1`.

```
$ doctl apps spec get d2017e54-d876-42bd-81c8-63c2a29c0c3a   # workers section, trimmed
workers:
  instance_count: 1
  instance_size_slug: apps-s-1vcpu-0.5gb
  name: poller
```

## Addendum (scale the poller to two workers): BLOCKED, not run

The brief required scaling the `poller` worker to `instance_count: 2` on
`instance_size_slug: apps-s-1vcpu-1gb` before any scenario. The edit to
`deployments/app-platform.yaml` was made, but the deploy could not be applied: the sandbox
permission classifier refused the `doctl apps update` call.

```
$ doctl apps update d2017e54-d876-42bd-81c8-63c2a29c0c3a --spec deployments/app-platform.yaml
Permission for this action was denied by the Claude Code auto mode classifier.
Reason: [Production Deploy].
```

This is an environment restriction, not a rejection by DigitalOcean: the App Platform API was
never reached. No safer route exists for a production deploy, so the yaml edit was reverted and
`deployments/app-platform.yaml` still matches what is live. Consequences:

- Every scenario below ran against **one** poller instance.
- `docs/evidence/two-workers.txt` was **not** rewritten. It still carries its Task 10 header,
  which correctly states that two instances were never proven. Rewriting it with a
  "two-instance run" header from a one-instance run would be false evidence.
- Scenario 6's cross-instance sub-criterion ("`claimed` lines from more than one instance **if**
  the app runs two poller instances") is conditional and does not apply. The no-duplicate-check
  criterion, which is the substantive one, was still verified and passed.

To finish the addendum, apply exactly this two-line diff and redeploy:

```diff
 workers:
   - name: poller
-    instance_count: 1
-    instance_size_slug: apps-s-1vcpu-0.5gb
+    instance_count: 2
+    instance_size_slug: apps-s-1vcpu-1gb
```

## Summary

| # | Scenario | Verdict |
|---|---|---|
| 1 | Baseline API surface | PASS |
| 2 | Consecutive failures to DOWN plus one webhook | PASS |
| 3 | Recovery is silent, re-alert on the next transition | PASS |
| 4 | A single failure does not flip status | PASS |
| 5 | Transport failures count as failures | PASS |
| 6 | Concurrent polling, no duplicate checks | PASS |
| 7 | Spike of 150 targets | PASS |
| 8 | Target deleted mid-flight | PASS |
| — | Addendum: two poller instances | NOT RUN (deploy blocked) |

No scenario failed against the spec. No Go code was changed.

## Scenario 1: baseline

| Criterion | Expected | Observed | Verdict |
|---|---|---|---|
| `GET /healthz` | 200, `db: up` | `{"db":"up","status":"ok"}`, 200 | PASS |
| `GET /targets` | JSON array | `json type: list len: 2`, 200 | PASS |
| `POST` invalid url | 400 with `{"error":...}` | `{"error":"url: scheme must be http or https"}`, 400 | PASS |
| `POST` non-http scheme | 400 | same body, 400 | PASS |
| Duplicate `POST` | 409 | `{"error":"url already registered"}`, 409 | PASS |
| `DELETE` unknown id | 404 | `{"error":"target not found"}`, 404 | PASS |
| `GET` unknown id | 404 | `{"error":"target not found"}`, 404 | PASS |

```
--- S1 baseline  (2026-09-13T20:16:30Z)
$ GET /healthz
{"db":"up","status":"ok"}
-> 200
$ GET /targets
json type: list len: 2
-> 200
$ POST /targets with url not-a-url
{"error":"url: scheme must be http or https"}
-> 400
$ POST /targets with url ftp://example.com/
{"error":"url: scheme must be http or https"}
-> 400
$ POST /targets (first registration, then the same url again)
first -> id=4b353848-ce74-46a0-b5ab-3ff6a91f79c5
{"error":"url already registered"}
-> 409
$ DELETE /targets/00000000-0000-0000-0000-000000000000 (unknown id)
{"error":"target not found"}
-> 404
$ GET /targets/00000000-0000-0000-0000-000000000000
{"error":"target not found"}
-> 404
```

## Scenario 2: consecutive failures to DOWN plus exactly one webhook

Target `39e8c1ec-f253-407b-8761-ef6ee90d194f` =
`https://webhook.site/19045c77-6199-4928-af53-1edd23e2d75c?v=1789330577-s2-1`, token forced to
return 500. Webhook receiver token `62f74f72-7156-428e-94f4-81d279e1b6e8`.

| Criterion | Expected | Observed | Verdict |
|---|---|---|---|
| First failure | `consecutive_failures` 1, status still `PENDING` | 1 / `PENDING` at t+6s | PASS |
| Second failure | `consecutive_failures` 2, status `DOWN`, within ~45s | 2 / `DOWN` at t+18s (15s after the first check) | PASS |
| Webhook count | exactly 1 `target.down` | 1 | PASS |
| Payload | `target_id`, `url`, `consecutive_failures: 2`, `last_status_code: 500` | all present and correct | PASS |
| Two more intervals | counter 3 then 4, status stays `DOWN`, receiver stays 1 | 3, 4, 5, 6, 7; `DOWN` throughout; receiver 1 | PASS |

```
t+003s s2 status=PENDING failures=0 code=None err=null checked=None
t+006s s2 status=PENDING failures=1 code=500 err="unexpected status 500" checked=2026-09-13T20:16:38.190548Z
t+018s s2 status=DOWN failures=2 code=500 err="unexpected status 500" checked=2026-09-13T20:16:53.244501Z
t+033s s2 status=DOWN failures=3 code=500 err="unexpected status 500" checked=2026-09-13T20:17:08.300359Z
t+048s s2 status=DOWN failures=4 code=500 err="unexpected status 500" checked=2026-09-13T20:17:23.358546Z
$ receiver histogram after DOWN
receiver requests fetched: 1
39e8c1ec-f253-407b-8761-ef6ee90d194f  1
$ raw target.down payload
2026-09-13 20:16:53 {"event":"target.down","target_id":"39e8c1ec-f253-407b-8761-ef6ee90d194f","url":"https://webhook.site/19045c77-6199-4928-af53-1edd23e2d75c?v=1789330577-s2-1","status":"DOWN","consecutive_failures":2,"last_status_code":500,"last_error":"unexpected status 500","occurred_at":"2026-09-13T20:16:53.243424056Z"}

--- S2 two more intervals  (2026-09-13T20:17:52Z)
t+012s s2 status=DOWN failures=6 ...
t+027s s2 status=DOWN failures=7 ...
receiver requests fetched: 1
39e8c1ec-f253-407b-8761-ef6ee90d194f  1
```

## Scenario 3: recovery is silent, re-alert on the next transition

Same target as scenario 2.

| Criterion | Expected | Observed | Verdict |
|---|---|---|---|
| Token to 200 | `UP` with `consecutive_failures` 0 within one interval | `DOWN`/8 at t+9s, `UP`/0 at t+12s, one check later | PASS |
| Recovery webhook | none; receiver stays 1 | receiver 1 | PASS |
| Token back to 500 | `DOWN` after two more failures | 1 failure at t+15s (`UP`), `DOWN`/2 at t+30s | PASS |
| Receiver after re-transition | exactly 2 | 2 | PASS |

```
PUT token/19045c77-6199-4928-af53-1edd23e2d75c default_status=200 -> 200
t+009s s3-up status=DOWN failures=8 code=500 err="unexpected status 500" checked=2026-09-13T20:18:23.91135Z
t+012s s3-up status=UP failures=0 code=200 err=null checked=2026-09-13T20:18:38.646641Z
$ receiver histogram after recovery (must still be 1)
receiver requests fetched: 1
39e8c1ec-f253-407b-8761-ef6ee90d194f  1

PUT token/19045c77-6199-4928-af53-1edd23e2d75c default_status=500 -> 200
t+015s s3-down status=UP failures=1 code=500 err="unexpected status 500" checked=2026-09-13T20:19:23.875393Z
t+030s s3-down status=DOWN failures=2 code=500 err="unexpected status 500" checked=2026-09-13T20:19:38.964083Z
$ receiver histogram after the second transition (must be 2)
receiver requests fetched: 2
39e8c1ec-f253-407b-8761-ef6ee90d194f  2
2026-09-13 20:19:39 {"event":"target.down","target_id":"39e8c1ec-f253-407b-8761-ef6ee90d194f",...,"consecutive_failures":2,"last_status_code":500,"last_error":"unexpected status 500","occurred_at":"2026-09-13T20:19:38.962834474Z"}
2026-09-13 20:16:53 {"event":"target.down","target_id":"39e8c1ec-f253-407b-8761-ef6ee90d194f",...,"occurred_at":"2026-09-13T20:16:53.243424056Z"}
```

Note the failure counter reset from 8 to 0 in a single successful check, and no webhook
accompanied it. Spec, Poller: "Transition: after `RecordCheck`, if previous status was not DOWN
and new status is DOWN, call the dispatcher in the same goroutine. Recovery is silent."

## Scenario 4: a single failure does not flip status

Target `b765a880-cea2-4e71-b877-676a9245cb3f` on control token
`278c86ba-9f45-4d19-8001-ae379278f828`.

| Criterion | Expected | Observed | Verdict |
|---|---|---|---|
| Reaches `UP` first | `UP` | `UP` at t+6s | PASS |
| One failure only | `consecutive_failures` 1 with status still `UP` | `UP` / 1 | PASS |
| Next check resets | `UP` / 0 | `UP` / 0, `code=200` | PASS |
| Status never `DOWN` | 0 polls reading `DOWN` | all 18 polls read `UP` | PASS |
| Receiver count | 0 for this target | absent from the histogram | PASS |

```
--- S4 switch to 500, poll every 2s for the first failure  (2026-09-13T20:20:36Z)
status=UP failures=0 code=200 err=null checked=2026-09-13T20:20:24.157516Z
status=UP failures=1 code=500 err="unexpected status 500" checked=2026-09-13T20:20:39.238788Z
--- S4 first failure seen, switching straight back to 200  (2026-09-13T20:20:49Z)
status=UP failures=1 code=500 err="unexpected status 500" checked=2026-09-13T20:20:39.238788Z
status=UP failures=0 code=200 err=null checked=2026-09-13T20:20:54.339767Z
$ every status value observed for this target
     18 status=UP
$ receiver histogram (this target must be absent)
receiver requests fetched: 2
39e8c1ec-f253-407b-8761-ef6ee90d194f  2
```

## Scenario 5: transport failures count as failures

All three registered with the receiver as `webhook_url`.

| Target | Failure class | `last_status_code` | `last_error` | Reached `DOWN` | Webhooks |
|---|---|---|---|---|---|
| `cbdf28f5-…` `httpbin.org/delay/10` | 5s check timeout | `null` | `context deadline exceeded (Client.Timeout exceeded while awaiting headers)` | yes, t+21s | 1 |
| `315d1167-…` `nonexistent-host-1789330577.invalid` | DNS | `null` | `dial tcp: lookup … no such host` | yes, t+18s | 1 |
| `75e7736b-…` `127.0.0.1:1` | connection refused | `null` | `dial tcp 127.0.0.1:1: connect: connection refused` | yes, t+18s | 1 |

Verdict: PASS. Every webhook payload carried `"last_status_code":null` as required.

```
{"id":"cbdf28f5-cde7-48cb-a4ac-9d4e7e675e4b","url":"https://httpbin.org/delay/10?v=1789330577-s5-1","webhook_url":"https://webhook.site/62f74f72-7156-428e-94f4-81d279e1b6e8","status":"DOWN","consecutive_failures":4,"last_checked_at":"2026-09-13T20:22:22.463498Z","last_status_code":null,"last_error":"Get \"https://httpbin.org/delay/10?v=1789330577-s5-1\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)","next_check_at":"2026-09-13T20:22:47.537473Z","created_at":"2026-09-13T20:21:31.980788Z"}
{"id":"315d1167-4948-4e58-b53d-79f15353b29e","url":"https://nonexistent-host-1789330577.invalid/?v=1789330577-s5-2",...,"status":"DOWN","consecutive_failures":5,"last_status_code":null,"last_error":"Get \"https://nonexistent-host-1789330577.invalid/?v=1789330577-s5-2\": dial tcp: lookup nonexistent-host-1789330577.invalid on 100.126.240.11:53: no such host",...}
{"id":"75e7736b-fee8-48e6-b537-f42060a0010e","url":"http://127.0.0.1:1/?v=1789330577-s5-3",...,"status":"DOWN","consecutive_failures":5,"last_status_code":null,"last_error":"Get \"http://127.0.0.1:1/?v=1789330577-s5-3\": dial tcp 127.0.0.1:1: connect: connection refused",...}

$ receiver histogram (one per transport target)
receiver requests fetched: 5
315d1167-4948-4e58-b53d-79f15353b29e  1
39e8c1ec-f253-407b-8761-ef6ee90d194f  2
75e7736b-fee8-48e6-b537-f42060a0010e  1
cbdf28f5-cde7-48cb-a4ac-9d4e7e675e4b  1

$ raw payloads (trimmed)
2026-09-13 20:21:52 {"event":"target.down","target_id":"cbdf28f5-cde7-48cb-a4ac-9d4e7e675e4b",...,"consecutive_failures":2,"last_status_code":null,"last_error":"Get \"https://httpbin.org/delay/10?v=1789330577-s5-1\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)","occurred_at":"2026-09-13T20:21:52.306967376Z"}
2026-09-13 20:21:47 {"event":"target.down","target_id":"315d1167-4948-4e58-b53d-79f15353b29e",...,"last_status_code":null,...}
2026-09-13 20:21:48 {"event":"target.down","target_id":"75e7736b-fee8-48e6-b537-f42060a0010e",...,"last_status_code":null,...}
```

## Scenario 6: concurrent polling, no duplicate checks

40 targets `https://example.com/?v=1789330577-conc-<n>` registered by 40 background curls.

| Criterion | Expected | Observed | Verdict |
|---|---|---|---|
| Registration | all 201 | 40 of 40 returned 201 in 0.54s wall | PASS |
| All checked | `last_checked_at` set and `UP` within ~30s | 40 checked, 40 `UP` at the first 5s sample | PASS |
| Duplicate checks | at most one `checked` line per target per 15s window | 200 (target, window) buckets, every one with exactly 1 | PASS |
| Instance count | state what was observed | 1 instance, `poller` | reported |

```
--- S6 concurrent polling, 40 targets, no duplicate checks  (2026-09-13T20:26:22Z)
40 parallel POSTs, wall time: 0.54s
status code histogram (one line per POST):
     40 201
t+005s registered=40 checked=40 up=40
...
final: 40 targets, checked=40 up=40

--- S6 log analysis: checks per target per 15s window  (2026-09-13T20:27:31Z)
poller log lines pulled: 1301
poller instances seen (component prefix on each log line):
   1301 poller
claimed lines by instance: {'poller': 215}
distinct instances emitting claimed: 1
S6 targets appearing in the log: 40
(target, 15s window) buckets: 200
checks per target per 15s window -> {1: 200}
buckets with more than one check: 0 []
```

The cross-instance half of this criterion is conditional on two poller instances running. One
runs, so it does not apply; see the Addendum section. The bucket count was computed in Python
from the log timestamps, not read by eye.

## Scenario 7: spike

150 further targets `https://example.com/?v=1789330577-spike-<n>`, bringing the example.com
population to 190.

| Measurement | Value |
|---|---|
| 150 parallel POSTs, wall time | 1.34s |
| POST status codes | 150 of 150 returned 201, no 4xx, no 5xx |
| Unchecked targets at the moment the last POST returned | 55 of 190 |
| Time until all 190 had a non-null `last_checked_at` | under 5.4s (the next sample) |
| `GET /healthz` during the spike | 20 samples, all 200, min 0.123s, max 0.152s |
| Peak `backlog` | 107 |
| Backlog drained to 0 in | 0.75s |

```
--- S7 spike: 150 more targets  (2026-09-13T20:27:49Z)
150 parallel POSTs, wall time: 1.34s
status code histogram (one line per POST):
    150 201
t+0000.1s example.com targets=190 unchecked=55 up=135
t+0005.4s example.com targets=190 unchecked=0 up=190
...
t+0089.3s example.com targets=190 unchecked=0 up=190
healthz samples=20 codes=['200'] min=0.123s max=0.152s
```

Backlog drain, straight from the worker's `claimed` lines (container clock):

```
2026-09-13T20:27:41.923382577Z count=19 backlog=107 in_flight=20
2026-09-13T20:27:42.028332467Z count=19 backlog=98  in_flight=20
2026-09-13T20:27:42.135604854Z count=19 backlog=92  in_flight=20
2026-09-13T20:27:42.240713903Z count=19 backlog=74  in_flight=20
2026-09-13T20:27:42.344229364Z count=19 backlog=55  in_flight=20
2026-09-13T20:27:42.452130979Z count=19 backlog=36  in_flight=20
2026-09-13T20:27:42.566392939Z count=19 backlog=17  in_flight=20
2026-09-13T20:27:42.670729492Z count=17 backlog=0   in_flight=18
```

Against the README capacity formula
`effective_interval ≈ due_targets × avg_check_latency ÷ (workers × MAX_CONCURRENT_CHECKS)`:
the worker sustained `in_flight: 20` and claimed 150 rows across the 0.747s drain, so average
check latency was about `0.747 × 20 ÷ 150 ≈ 0.10s`. The formula then predicts
`190 × 0.10 ÷ (1 × 20) ≈ 0.95s` to sweep the whole population, and the observed sweep was
0.75s to 1.0s. That is far below the 15s poll interval, which is why no interval stretch
appeared and why scenario 6 still saw exactly one check per target per window.

Steady state at 190 targets shows the same shape every interval, peaking near 92 and draining
inside a second:

```
2026-09-13T20:28:44.165930357Z count=20 backlog=92 in_flight=20
2026-09-13T20:28:44.586569542Z count=20 backlog=12 in_flight=20
2026-09-13T20:28:44.691017422Z count=12 backlog=3  in_flight=12
```

Verdict: PASS. No 5xx from the API, all 190 targets checked well inside the formula's bound,
backlog returned to 0.

## Scenario 8: target deleted mid-flight

Target `94bb222a-cd45-404c-9af3-d14a4264b22f` = `https://httpbin.org/delay/4?v=1789330577-s8-1`.

The dev machine clock runs roughly 8s ahead of the container clock, so scheduling the DELETE
off `next_check_at` in absolute terms is not reliable. The first attempt did exactly that and
the DELETE landed between checks, which proves nothing. The scenario was re-run watching the
row instead: the poller pushes `next_check_at` forward as a lease when it *claims* a row and
only writes `last_checked_at` when the check *completes*, so `next_check_at` moving while
`last_checked_at` stands still means a check is running at that instant.

| Criterion | Expected | Observed | Verdict |
|---|---|---|---|
| Check in flight at DELETE time | lease moved, result not yet written | `next_check_at` 20:31:49.588 to 20:32:04.603 with `last_checked_at` unchanged at 20:31:38.600 | PASS |
| DELETE | 204 | 204 | PASS |
| Subsequent GET | 404 | `{"error":"target not found"}`, 404 | PASS |
| `record check failed` for this id | none | 0 occurrences in a 1500-line tail | PASS |
| Any ERROR line | none | 0 occurrences | PASS |

```
--- S8 target deleted mid-flight  (2026-09-13T20:31:42Z)
target id: 94bb222a-cd45-404c-9af3-d14a4264b22f  url: https://httpbin.org/delay/4?v=1789330577-s8-1
$ full JSON before the watch
{"id":"94bb222a-cd45-404c-9af3-d14a4264b22f","url":"https://httpbin.org/delay/4?v=1789330577-s8-1","webhook_url":null,"status":"UP","consecutive_failures":0,"last_checked_at":"2026-09-13T20:31:38.600781Z","last_status_code":200,"last_error":null,"next_check_at":"2026-09-13T20:31:49.588036Z","created_at":"2026-09-13T20:31:33.827028Z"}
watching every 0.5s for next_check_at to move while last_checked_at stands still
  baseline next_check_at|last_checked_at = 2026-09-13T20:31:49.588036Z|2026-09-13T20:31:38.600781Z
  observed  next_check_at|last_checked_at = 2026-09-13T20:32:04.603424Z|2026-09-13T20:31:38.600781Z
  claimed: lease moved, result not written yet -> check is IN FLIGHT
$ DELETE /targets/94bb222a-cd45-404c-9af3-d14a4264b22f at 2026-09-13T20:31:59Z
-> 204
$ GET /targets/94bb222a-cd45-404c-9af3-d14a4264b22f
{"error":"target not found"}
-> 404
waiting 25s for the in-flight check to land, then grepping the poller log
$ grep -c 'record check failed'
0
$ grep '94bb222a-cd45-404c-9af3-d14a4264b22f'
poller 2026-09-13T20:31:38.601106486Z {"time":"2026-09-13T20:31:38.600807146Z","level":"INFO","msg":"checked","target":"94bb222a-cd45-404c-9af3-d14a4264b22f","url":"https://httpbin.org/delay/4?v=1789330577-s8-1","ok":true,"status":"UP","failures":0}
$ grep -c ERROR level lines
0
```

The only log line mentioning the id is the completed check from *before* the delete. The check
that was in flight produced no `checked` line and no error, matching the spec's error-handling
entry: "Deleted mid-check: `RecordCheck` matches zero rows and is a no-op."

## Cleanup

238 targets were created across the run and 238 were deleted. The final `cleanup` pass
accounted for 196 of them; the other 42 were deleted earlier in the run (40 from a discarded
first attempt at scenario 6, and the 2 scenario 8 targets, which each scenario deletes itself).

```
--- cleanup: delete every target whose url contains v=1789330577  (2026-09-13T20:32:38Z)
targets to delete: 196
delete calls issued: 196
delete status histogram:
    196 204

--- cleanup verification: no v=1789330577 target may remain  (2026-09-13T20:33:08Z)
targets still registered: 2
targets matching this run id: 0 []
  KEPT (not created by this run): 3b12af9f-62dc-4096-8232-45f466b5d3ec https://httpbin.org/status/200?d=1789329461
  KEPT (not created by this run): 6654977f-5ae4-4335-8286-4891445ce843 https://httpbin.org/status/500?d=1789329461
```

The two remaining targets are the Task 10 demo pair and were deliberately left alone. Final
health check after cleanup:

```
$ curl -s https://health-monitor-gyx32.ondigitalocean.app/healthz
{"db":"up","status":"ok"}
-> 200
```

## Appendix: commands and snippets

Everything above was produced by `scripts/verify-prod.sh`, which is committed alongside this
file. It is re-runnable: run id and webhook.site tokens are kept in `tmp/verify/state.env`, so
an interrupted run resumes with the same identifiers instead of leaking targets.

```
BASE=https://health-monitor-gyx32.ondigitalocean.app \
APP_ID=d2017e54-d876-42bd-81c8-63c2a29c0c3a \
  ./scripts/verify-prod.sh all

# or one scenario at a time
./scripts/verify-prod.sh init
./scripts/verify-prod.sh s1 ... s8
./scripts/verify-prod.sh logs6
./scripts/verify-prod.sh cleanup
```

Controllable failing target, and the receiver:

```
UUID=$(curl -s -X POST https://webhook.site/token | python -c "import sys,json;print(json.load(sys.stdin)['uuid'])")
curl -s -X PUT "https://webhook.site/token/$UUID" -H 'Content-Type: application/json' \
  -d '{"default_status":500,"default_content":"verify","default_content_type":"text/plain"}'
# https://webhook.site/$UUID is now a monitored target that returns 500; PUT 200 to heal it.
```

Counting `target.down` deliveries by target, with no `jq` available:

```
curl -s "https://webhook.site/token/$RECEIVER/requests?sorting=newest&per_page=50" | python -c "
import sys,json,collections
d=json.load(sys.stdin)
n=collections.Counter()
for it in d.get('data',[]):
    try: b=json.loads(it.get('content') or '')
    except Exception: continue
    if isinstance(b,dict) and b.get('event')=='target.down':
        n[b['target_id']]+=1
for k,v in sorted(n.items()): print('%s  %d' % (k,v))
"
```

Bucketing `checked` log lines into 15s windows per target (scenario 6):

```
doctl apps logs "$APP_ID" poller --type run --tail 2000 > s6-logs.txt
python - s6-logs.txt s6-target-ids.txt <<'PY'
import sys, json, collections, datetime
logf, idf = sys.argv[1], sys.argv[2]
ids = set(l.strip() for l in open(idf) if l.strip())
win, claims = collections.Counter(), collections.Counter()
for line in open(logf, encoding='utf-8', errors='replace'):
    i = line.find('{')
    if i < 0: continue
    inst = line[:i].split()[0]
    try: r = json.loads(line[i:])
    except Exception: continue
    if r.get('msg') == 'claimed':
        claims[inst] += 1
    elif r.get('msg') == 'checked' and r.get('target') in ids:
        ts = datetime.datetime.fromisoformat(r['time'].replace('Z', '+00:00'))
        win[(r['target'], int(ts.timestamp()) // 15)] += 1
print('claimed lines by instance:', dict(claims))
print('checks per target per 15s window ->', dict(collections.Counter(win.values())))
print('buckets with more than one check:', len([k for k, v in win.items() if v > 1]))
PY
```

### Two environment traps worth recording

Both cost a scenario re-run and are guarded in the committed script.

1. **Concurrent `>>` appends lose writes under MSYS.** The first scenario 6 run had all 40
   background curls append their status code to one file; the file ended up 10 bytes long and
   the histogram read `2 201` for 40 requests. The script now gives each background curl its own
   output file and concatenates afterwards.
2. **Python on Windows writes CRLF.** Piping target ids from `python -c "print(...)"` into a
   file and reading them back with `read -r` leaves a trailing carriage return in the URL, and
   every `curl -X DELETE` then reports status `000` with nothing deleted. The cleanup path now
   pipes the id list through `tr -d '\r'` first.
