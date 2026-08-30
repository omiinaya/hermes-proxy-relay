# Changelog

All notable changes to Hermes Proxy Relay.

## [Unreleased]

### Stream-path correctness fixes (from the 2026-08-30 audit)

- **C-1 (CRITICAL) — successful streams were retried.** A 2xx stream response
  fell through the stream retry loop's `400 <= code` guard and was re-POSTed up
  to `MAX_REQUEST_RETRIES` extra times (discarding each good stream, returning
  the last). Now returns immediately on `<500`/429, mirroring the non-stream
  path. Regression test asserts exactly one upstream call for a 200 stream.
- **C-2 — stream-SETUP client-borrow leak on cancellation.** A client disconnect
  during `client.send()` raised `asyncio.CancelledError` (a `BaseException`),
  bypassing the `except Exception` handler that releases the pooled-client
  borrow → the client was stuck in-use forever (neither evicted nor reaped).
  Added a `CancelledError` handler that releases the borrow, then re-raises.
- **H-1 — weakref.finalize thread race.** Finalizer releases now route through
  `call_soon_threadsafe` (with a synchronous fallback if the loop is closed) so
  the semaphore wake + borrow decrement happen on the event-loop thread.
- **H-2 — reload drift.** `STREAM_IDLE_TIMEOUT` is now hot-reloadable and
  `MODEL_EXHAUST_CAP` is pushed into the live pool (`set_exhaust_cap`) on reload
  instead of diverging from the initial snapshot.
- **H-3 — stream model breaker.** A 400 "Model is unavailable" on the stream path
  now trips the global model breaker (was non-stream-only); the per-sweep
  breaker short-circuit already existed.
- **H-4 — fallback bridges gated.** Every fallback-model bridge probe now runs
  under a dedicated concurrency slot (`_fallback_call`) so an exhausted-primary
  cascade cannot exceed `MAX_CONCURRENT_UPSTREAM`.
- **M-1 — bounded stream error-body reads.** Error bodies on the stream 429/`>=400`
  paths read through `_read_bounded_body` (capped at `MAX_RESPONSE_SIZE`) instead
  of an unbounded `aread()`.
- **M-2 — SSE-framed stream fallbacks.** A `stream:true` client served by the
  fallback bridge now gets its buffered response re-framed as SSE `data:` events
  + `[DONE]` instead of a raw JSON body under `text/event-stream`.
- **M-4 — stricter health revival.** A permanently-dead proxy is revived only by
  a genuine `<400` health response, not a 401/redirect that merely "answered".
- **M-5 — pooled health probes.** Health probes reuse a warm pooled client when
  one is cached (no fresh SOCKS5+TLS handshake per probe) with fresh-client fallback.
- **L-2 — accurate exhaust message.** The global-model-breaker short-circuit now
  reports "circuit breaker open" instead of claiming every proxy was exhausted.
- **L-4 — documented 429 asymmetry.** Added explicit rationale for the differing
  caps/floors (proxy cooldown vs model-exhaust).
- **L-5 — loud GO_UPSTREAM key fallback.** Import logs a warning when
  `GO_UPSTREAM_API_KEY` is unset and falls back to the primary key.
- Docs: test counts corrected to 687 across 15 files; 3.10 added to the CI matrix.

## [1.11.0] — 2026-08-30

### Zen-style anonymous free-tier fixes (upstream /v1)

- **Client UA + identity headers** — `_build_headers` now injects the expected
  client User-Agent (`opencode/1.18.25`) plus identity headers
  (`HTTP-Referer: https://opencode.ai/`, `X-Title: opencode`) by default.
  The zen-style anonymous free tier hard-gates on these: requests with a
  browser/generic UA (or missing identity headers) get a
  `FreeUsageLimitError` 429 even with a valid key. Verified 2026-08-30 from the
  box AND through the SOCKS5 pool. The models-list fetch gets the same
  treatment for Cloudflare-parity.
- **Global model circuit breaker** — upstream 400 `Model is unavailable` is a
  global capacity gate (e.g. deepseek-v4-flash-free during peak), not a per-IP
  budget. Every proxy fails identically, so the relay now trips a model-level
  breaker (default 300s) instead of sweeping the pool. While open, requests for
  that model short-circuit to the `FreeUsageLimitError` shape the Hermes
  fallback bridge listens for — zero wasted upstream round-trips, straight to
  the next model in the chain.
- **`big-pickle` stays on the free model list** — the `-free` models filter
  also keeps `big-pickle` (already-pinned active model) so it isn't hidden from
  `/models` when `MODELS_FREE_ONLY` is on.
- **Breakers on `/health`** — `model_breakers` field reports
  `{model: seconds_remaining}` for observability.
- Tests updated to assert the client UA + identity headers (the old tests
  asserted the previous browser-UA behavior).
- 644 tests, 100% coverage.

## [1.10.0] — 2026-08-05

### Bottleneck audit pass (findings → fixes)

- **Client pool cap auto-scales to proxy count (`CLIENT_POOL_MAX`, default 100
  = floor)** — the pooled-httpx-client cap is now `max(CLIENT_POOL_MAX,
  #proxies)`. With 250 proxies in rotation and the old fixed 100-cap, ~60% of
  round-robin requests landed on a proxy with NO warm client and paid a fresh
  TCP+SOCKS5+TLS handshake on the single event loop — a handshake storm under
  burst (the exact "tanking" pattern). One warm client per proxy means rotation
  never pays a cold handshake.
- **Stream idle timeout (`STREAM_IDLE_TIMEOUT`, default 60s)** — a proxy that
  goes silent mid-SSE now releases its concurrency permit + pooled client after
  the inter-chunk idle bound instead of holding them for the full
  `UPSTREAM_READ_TIMEOUT` (120s). `0` falls back to the read timeout. Slow-but-
  alive streams that deliver within the window are never killed (per-chunk).
- **Bytearray buffering (requests AND responses)** — `_proxy_single` and
  `_read_body_capped` accumulate into a single `bytearray` instead of a chunk
  list + `b"".join`. Peak memory ≈ 1× payload instead of 2× (200MB response cap
  → 400MB transient, 100MB body cap → 200MB spike, both halved).
- **EWMA latency tracking (α=0.2)** — `record_latency` replaces the never-
  decaying arithmetic mean. A proxy that degrades hours after its fast samples
  now tracks CURRENT performance, so `LATENCY_SKIP_THRESHOLD_MS` stays
  responsive. (Latency-aware selection remains opt-in; round-robin is still the
  default because it keeps per-IP quota spread even across the pool.)
- **Single-pass request body parse** — `_parse_request_body` does model-alias
  translation + model extraction + stream detection in ONE `json.loads`
  (previously up to three serial parses on the event loop per request). Large
  bodies (>256KB) fall back to byte-scan stream detection; the extracted model
  comes from the TRANSLATED body so budget-parking keys stay consistent.
- 644 tests, 100% coverage, ruff clean.

## [1.9.0] — 2026-08-05

### Production parity port (deployed to the relay :4002)

- **Proxy-group loader** — `DECODO_HOST/USER/PASS/START_PORT/END_PORT`
  env groups (DECODO..DECODO9) build the proxy pool, matching the old 776-line
  production relay's env contract (250 proxies in prod).
- **Model alias translation** — `oc-deepseek-v4-flash` → `deepseek-v4-flash-free`
  etc. at the choke point (fixes the 2026-08-02 fleet-wide 404/burned-proxy
  outage root cause).
- **Per-model budget exhaustion** — `FreeUsageLimitError` 429 parks a proxy for
  THAT MODEL only (per-IP quota); the sweep continues through the rest of the
  pool; a clean 429 is returned only when EVERY proxy is parked. Proxies stay
  active for other models — the pool-burn pattern is impossible.
- **Truncation validation** — a 200 chat-completion without a structurally valid
  `choices[0].message` is treated as SOCKS5 truncation and retried on the next
  proxy.
- **Browser UA spoofing** — Cloudflare 403-proof UA always sent upstream.
- **`/go/v1/*` routes** — second upstream (`GO_UPSTREAM_BASE` + dedicated key),
  parity routes for models/chat/responses.
- **Free-models filter** — `MODELS_FREE_ONLY=true` returns only `-free` models.
- Deployed live: 250 proxies, dynamic cap auto-tuning (24→500 under idle
  CPU/disk), alias requests 200. Old relay backed up (`relay.py.bak-pre1.9-*`).

## [1.8.1] — 2026-08-05

### Disk-I/O awareness in the dynamic cap

- **`DYNAMIC_CAP_DISK_TARGET_PCT` (70) / `DYNAMIC_CAP_DISK_MAX_PCT` (85)** — the
  adjuster also samples the busiest real block device's utilization
  (`/proc/diskstats` field 12 "ms spent doing I/O", stdlib; skips
  loop/ram/zram/dm-/zd virtual devices). Disk targets sit LOWER than CPU because
  I/O latency collapses near saturation. The cap grows only when BOTH cpu and
  disk have headroom; either exceeding its max triggers a hard 2× backoff.
  Non-Linux → CPU-only tuning. `/health` reports `disk_pct`.

## [1.8.0] — 2026-08-05

### Dynamic concurrency cap (auto-tuned)

- **`DYNAMIC_CAP_ENABLED=true` + `HOLD_PERMIT_FOR_STREAM=true`** — replaces the
  fixed `MAX_CONCURRENT_UPSTREAM` with a background adjuster that samples
  process CPU (`getrusage`, stdlib — no psutil) every `DYNAMIC_CAP_INTERVAL_S`
  (5s) and tunes the effective cap: grow +10%/tick below 75%, hold in a
  hysteresis band (75–90%), ease down 90–96%, hard 2× backoff above 96%. EWMA
  smoothing (0.3) + >5% change gate prevent churn; range `[DYNAMIC_CAP_MIN,
  DYNAMIC_CAP_MAX]` = [10, 500]. In idle conditions the cap grows toward 500 —
  effectively NO hard cap; the relay self-limits only when streams genuinely
  consume CPU. Requires hold=true (a held permit is what lets the cap govern
  stream lifetime). `/health` exposes `dynamic_cap.effective_max`, `cpu_pct`,
  `adjustments`.

## [1.7.2] — 2026-08-04

- **Default `MAX_CONCURRENT_UPSTREAM` raised 10 → 24** — at 10 the relay
  self-throttled below what pool+upstream can take (cap == max concurrent
  streams == max concurrent conversations when the permit is held per-stream).
  The upstream has run 24 concurrent since 2026-08-02, well under the free-tier
  burst limit.
- **Throughput profile** — deployment config uses `MAX_CONCURRENT_UPSTREAM=24`
  + `HOLD_PERMIT_FOR_STREAM=false` for max concurrency; upstream 503s trigger
  retries, never proxy-cool (503 is NOT in the cool list).
- **Hermetic tests** — conftest sets `RELAY_CONFIG=""` so the module imports
  with pure defaults and never an operator's live config.json.

## [1.7.1] — 2026-08-04

- **Auth-switch retry re-borrows the pooled client (single-shot path)** — the
  retry after a successful auth switch reused a client whose borrow had already
  exited (`_client_in_use == 0`), so under load an LRU eviction or pool prune
  could `aclose()` it mid-flight. Now wrapped in its own `async with
  _borrow_client(...)`. Regression test proves `[1,0]` pre-fix vs `[1,1]`
  post-fix.

### Stability / disconnects (full audit pass)

- **Stale-keep-alive prevention (`CLIENT_IDLE_TTL`, default 120s)** — pooled
  connections the proxy/upstream silently closed while idle are now reaped
  BEFORE reuse (LRU-order scan on borrow), instead of failing on a dead
  socket and mis-attributing a healthy proxy as down. `0` disables.
- **Decoupled upstream timeouts (`UPSTREAM_CONNECT_TIMEOUT` 15s /
  `UPSTREAM_READ_TIMEOUT` 120s)** — replaces the fixed 60s on every pooled
  client. A slow first-token upstream or a stream with a long inter-token
  gap is no longer killed by a single shared timeout. Applies as connect vs
  per-chunk-read respectively.
- **`MAX_RESPONSE_SIZE` (default 200MB, 0=unlimited)** — single-shot
  responses are now read as a stream with a cap; a runaway upstream can no
  longer make the relay buffer an unbounded response (request bodies were
  already capped; responses were not). Oversized → 502 `response_too_large`
  + transient cooldown.

### Overload / slowdowns

- **Short retry semaphore wait (`RETRY_SEMAPHORE_WAIT_SECONDS`, default 2s)**
  — the FIRST attempt may queue up to `SEMAPHORE_WAIT_SECONDS` for capacity,
  but retries after a failure fail fast instead of stacking another 30s wait
  on an already-failing request (was ~90s worst-case to a 503 under load).
- **Exponential retry backoff (`RETRY_BACKOFF_BASE` 0.1s / `RETRY_BACKOFF_MAX`
  1s, `0` disables)** — kinder to the upstream during a failure cascade.
- **Latency-aware proxy selection (`LATENCY_SKIP_THRESHOLD_MS`, default 0 =
  round-robin)** — when enabled, a measured-slow proxy is skipped in favor of
  a faster available one (falling back to it only when nothing faster exists).
  Opt-in; preserves round-robin behavior by default.
- **Pool-lock contention** — `aclose()` calls are hoisted OUT of
  `_client_pool_lock` (at eviction, prune, deferred-close, and shutdown), so
  a draining close can no longer serialize all other client acquisitions.

### Connections / upstream load

- **Health sweep probes only proxies that need attention** — permanently-dead
  (revival), cooling (recovery), or never-used (new). A fully-healthy pool now
  triggers ZERO upstream probes (was ~N requests/min of load for nothing).
- **Models refresh retries across proxies on connect failure** — one dead
  proxy no longer stalls a cold-cache `/v1/models`; non-200 statuses still
  serve the cache immediately (no pointless retry).
- **AuthSwitcher probes now honor the concurrency gate** — `_probe_auth`
  acquires the semaphore (short wait); at capacity the probe is deferred as
  `inconclusive` (never an auth signal, so no false switch).
- **Inbound connection caps (`RELAY_MAX_CONNECTIONS` / `RELAY_BACKLOG`)** —
  passed to uvicorn; guard against FD exhaustion / slow-loris BEFORE the
  semaphore backlog logic runs. `0` = uvicorn defaults.
- **`RELAY_LOG_REQUESTS` (default true)** — toggle per-request INFO logging
  for minimum overhead at very high rates.
- **`socks5h://` recommendation** — `--check` warns when `socks5://` URLs are
  used (local DNS resolves the upstream hostname at the relay; `socks5h://`
  resolves at the proxy for privacy + CDN-correct IPs).

## [1.6.0] — 2026-08-04

### Performance / Scaling (bottleneck pass)

- **Streams now reuse the shared per-proxy httpx client** (`_make_streaming_client`
  borrows from the LRU pool instead of building a fresh client + transport per
  stream). Previously every streaming request paid a brand-new TCP → SOCKS5
  handshake → TLS handshake on the single event loop — a thundering herd under
  burst load. Warm connections are now reused across streams; eviction still
  skips in-use clients so a live stream is never aborted. The stream generator
  releases the borrow (exactly-once, guarded like the semaphore) instead of
  closing the client.
- **Bounded semaphore backlog (`MAX_QUEUED_REQUESTS`, default 100)** — when that
  many requests are already queued for a concurrency permit, new requests fail
  fast with 503 instead of piling up behind long-held permits. Bursts drain up
  to the cap, then excess load is shed immediately. `0` restores unlimited
  queueing. `/health` now reports `semaphore.queued`.
- **`HOLD_PERMIT_FOR_STREAM` (default `true`)** — the concurrency permit is held
  for the whole stream lifetime (upstream-queue-safe; keeps the observed
  anonymous free tier "queue is full" 503 failure mode from happening). Set `false` to
  release the permit after connection setup for unbounded stream throughput —
  opt-in, documented trade-off.
- **Parallel health-check sweeps (`HEALTH_CHECK_CONCURRENCY`, default 20)** —
  the per-proxy probes now run concurrently with a bounded semaphore instead of
  strictly serial (`~N × probe-time` per sweep on a 250-proxy pool became
  `~N/20 × probe-time`). Per-proxy failure semantics, revival, and the
  all-failed guard are unchanged.
- **`RELAY_WORKERS` (default 1)** — opt-in uvicorn multi-process scaling. Each
  worker carries its OWN pool/cooldowns/health state (not shared); the startup
  log warns about this. Custom SIGTERM/SIGINT handlers are skipped in
  multi-process mode so uvicorn's master manages worker lifecycle.
- **Request counters no longer serialize on a module-global `asyncio.Lock`** —
  plain increments behind a cheap `threading.Lock` (the old lock wasn't
  thread-safe and could bind to a stale loop).
- **Stream detection for large bodies** — the byte scan locates the `"stream"`
  key with a fast C `find` and regexes only a 256-byte window after it instead
  of a full-body regex; IGNORECASE semantics preserved via a fallback scan when
  no lowercase key exists.

### Fixed

- The relay now ships at 100% test coverage again (v1.5.0 had drifted to
  99.34%): new tests cover the AuthSwitcher disabled-probe, probe read-timeout,
  state-persistence failure, stream auth-switch retry, and bearer admin-health
  branches.

## [1.5.0] — 2026-08-01

### Added
- **Smart auth switching (`AuthSwitcher`)** — the relay now detects upstream
  auth-method changes (e.g. OpenCode Zen flipping `x-api-key` → Bearer) and
  self-heals WITHOUT manual intervention. Only a **401** counts as an auth
  signal — 5xx, 429, and connection errors never trigger a switch. On N
  consecutive 401s (default 3), alternate auth types are probed with the SAME
  API key against `GET /models`; a candidate returning 200 twice is adopted,
  the current request is retried once with the verified type, and the change
  is persisted to `AUTH_STATE_PATH` so restarts keep the fix. Anti-flap:
  probe cooldown (default 300s) and max switches per window (default 3/h);
  a dead key (all candidates 401) sets a `key_revoked` alert instead of
  flapping; sustained switching sets `flapping` and stops auto-switching.
  Status is surfaced in `/health` (`auth_switch`), and config reloads
  propagate the new knobs live.
- Config keys: `AUTH_SWITCH_ENABLED`, `AUTH_SWITCH_CANDIDATES`,
  `AUTH_SWITCH_TRIGGER_THRESHOLD`, `AUTH_SWITCH_PROBE_SUCCESSES`,
  `AUTH_SWITCH_COOLDOWN_S`, `AUTH_SWITCH_MAX_PER_WINDOW`,
  `AUTH_SWITCH_WINDOW_S`, `AUTH_STATE_PATH`.

## [1.4.2] — 2026-08-01

### Fixed
- **Upstream auth default flipped `x-api-key` → `bearer`** — the zen-style
  upstream switched authentication to `Authorization: Bearer`; the old
  name/key heuristics in `_infer_auth_type()` (plugin) and setup.sh mapped
  legacy alias names and `api_key: "public"` to `x-api-key`,
  which produced 401s against the new upstream. Both now default to `bearer`
  — force `x-api-key` explicitly with `/relay setup clone <N> x-api-key`.

## [1.4.1] — 2026-08-01

### Fixed
- **Client-pool use-after-close TOCTOU** — `_close_client_when_idle` checked
  the in-use counter outside the pool lock; a concurrent borrower could get
  its client force-closed mid-flight. Re-checked under the lock, and the
  deferred-close cap raised 30s → 65s (was below the 60s client timeout).
- **Stream error-path semaphore double-release** — a `client.aclose()` that
  raised on the 429/4xx paths propagated before the caller marked
  `semaphore_handed_off`, so the retry loop's finally released the permit
  AGAIN (over-credit → concurrency limit exceeded). aclose is now
  best-effort on both error paths.
- **3xx responses are now neutral** — they were classified as success,
  which revived permanently-dead proxies and cleared error counters.
- **Stream detection precision** — a nested `"stream": true` inside
  free-form fields (metadata/tool-schema) routed non-stream requests
  through the chunked path. Small bodies now parse JSON and check the
  TOP-LEVEL `stream` key; large bodies keep the cheap byte scan.
- **Scheme-less URL credential leak** — `_mask_proxy_url` returned raw
  `user:pass@host` URLs unchanged (reachable via unauthenticated `/health`);
  now masked via rpartition. `_redact_query` also normalizes param names
  (percent-encoded/dashed/x-api-key variants no longer leak values).
- **CORS expose_headers** — browser JS can now read relayed upstream
  headers (x-request-id, openai-*, x-ratelimit-*).
- **setup.sh install robustness** — re-runs no longer abort while the proxy
  list is still the placeholder (grep `set -euo pipefail` trip), malformed
  `config.yaml` no longer aborts the install with a raw traceback (falls
  back to manual config), all interactive `read` calls tolerate EOF/CI,
  post-write verification honors custom `HERMES_HOME`, systemd unit paths
  are quoted (space-safe), stale plugin symlinks are refreshed, and the
  `/model` instruction preserves unicode provider names.
- **Plugin config safety** — `config.yaml` writes are now atomic
  (temp-file + `os.replace` + fsync: a crash mid-write can no longer
  destroy the whole Hermes config); cloning a second provider no longer
  regenerates `CLIENT_API_KEY` (which silently broke the first clone's
  auth); malformed/non-dict/null `custom_providers` config is tolerated;
  the `switch` unknown-subcommand message no longer prints literal `{sub}`.
- **MCP `tool_upstream_health`** now honors env-first `ADMIN_API_KEY`
  precedence (was 403ing when the relay ran with the key in the env).
- **upstream-health endpoint hardened** — raw exception text no longer leaks
  to clients; connect failures now return 502 (matching the request path) and
  cool the dead proxy; `UPSTREAM_BASE` with embedded `user:pass@` is masked in
  all responses; only a 200 reports `"status": "ok"` (401/404/5xx → degraded).
  Probes run with `probe=True` — a 429 from the upstream no longer cools the
  pool or mutates request counters.
- **Config reload gaps closed** — `PROXY_HEALTH_CHECK_INTERVAL` now hot-reloads;
  malformed `config.json` values return a 400 JSON error instead of a raw 500;
  `main()` `--config` recompiles the model filter regex (it previously updated
  `MODEL_FILTER_PATTERN` but kept filtering with the import-time pattern).
- **Empty env vars treated as unset** — `ADMIN_API_KEY=`, `CLIENT_API_KEY=`, or
  empty numeric settings silently disabled file-configured auth or crashed
  startup with `int('')` ValueError. All config reads now use `or` fallback.
- **Stream errors sanitized** — mid-stream exceptions no longer embed raw
  socket/upstream text in the client-visible error payload; details are
  logged server-side only.
- **Smoke test expanded 15 → 20 checks** — now covers OPTIONS CORS preflight,
  bare-OPTIONS routing (regression guard for the former 405), HEAD routing via
  `-I`, `/admin/upstream-health` (dead proxy → 503, proves pool routing), and
  `/admin/reload-config`. Also fixes `plugin.yaml` version drift (1.4.0 → 1.4.1).
- **Hot-reload ignored cooldown constants** — `CONSECUTIVE_ERROR_THRESHOLD`,
  `PERMANENT_COOLDOWN_SECONDS`, and `MAX_RETRY_AFTER_SECONDS` were read once at
  startup; `/admin/reload-config` and `--config` silently ignored them from the
  config file. All three now hot-reload (env still wins).
- **Streaming requests now respect the concurrency semaphore** — previously the
  semaphore was released before the stream generator ran, so 100+ parallel SSE
  requests could hold unbounded sockets/connections. The slot is now held for
  the stream's entire lifetime and released when the generator finishes.
- **`_acquire_semaphore` TOCTOU + permit leak** — a concurrent reload could swap
  the module-global semaphore mid-acquire (releasing the wrong object → capacity
  limit exceeded), and `wait_for` could cancel an acquire that completed in the
  same tick (permit leaked forever → capacity drifted to 0). Both fixed.
- **`_prune_client_pool` closed in-use clients mid-request** — a reload while a
  request was borrowing a client aborted the live request and misattributed the
  failure to the proxy. In-use clients are now deferred to a bounded
  `_close_client_when_idle` task; `_get_client` marks in-use under the lock
  (TOCTOU closed).
- **ReadTimeout/RemoteProtocolError blamed on the proxy** — a slow/flaky upstream
  through a healthy proxy incremented `consecutive_errors` and could permanently
  kill good proxies. These now use `record_transient` (30s cooldown, NOT counted
  toward permanent death); only connect-level failures count.
- **502/504 never cooled the proxy** — dead proxies stayed in rotation forever.
  Both single-shot and streaming paths now cool on 502/504 (proxy's upstream
  connection failed).
- **Hostile `Retry-After` values** — `Retry-After: 999…9` overflowed the cooldown
  arithmetic and returned 502 instead of the upstream's 429; year-long cooldowns
  removed proxies from rotation forever. Values are now clamped to
  `MAX_RETRY_AFTER_SECONDS` (default 3600); malformed headers degrade to 60s.
- **Proxy credentials leaked into logs** — invalid proxy lines (e.g. a typo'd
  `user:pass@` URL with `@` in the password) were logged verbatim. Now masked.
- **`admin_reset_by_errors` unvalidated input → 500** — a string/bool/None
  `min_consecutive` in the body raised TypeError. Coerced defensively.
- **Health-checker busy loop after hot-reload** — reloading with
  `PROXY_HEALTH_CHECK_INTERVAL=0` spun on `asyncio.sleep(0)` hammering the target.
  The `<= 0` guard now lives inside the loop (60s backoff).
- **`stream: true` beyond the first 8KB missed** — a legal request with the flag
  deep in the JSON took the non-streaming path and buffered an unbounded SSE
  response. Full body is now scanned (byte-level regex, no copy).
- **Non-constant-time API-key comparisons** — both client and admin keys compared
  with `==`/`!=` (timing side channel). Now `secrets.compare_digest`.
- **`Bearer` scheme case-sensitivity** — `bearer <key>` (RFC 7235 case-insensitive)
  was 401'd. Now accepted.
- **Re-clone rotated the client key but left `-proxied` stale** — Hermes kept
  sending the old key → every request 401'd. `_write_proxied_provider` and
  setup.sh's PYHERMES block now update the existing entry's `api_key`.
- **Re-clone silently wiped config.json keys** — `ADMIN_API_KEY`, `RELAY_PORT`,
  etc. were clobbered on re-run. Plugin and setup.sh now preserve existing keys.
- **setup.sh Python code injection** — a provider name containing `'` was
  interpolated into `python3 -c "..."` source. Now passed via environment.
- **setup.sh loop-detection hardcoded `:4002`** — a relay on a custom port wasn't
  excluded from cloning (proxy-loop risk). Now uses `$RELAY_PORT`.
- **MCP tools ignored env-var keys** — if the relay ran with env-var
  `CLIENT_API_KEY`/`ADMIN_API_KEY`, MCP admin tools silently failed auth. Now
  checks env first (matching relay.py precedence), then config.json.
- **MCP `tool_status` dropped /health fields** — `shared_clients`,
  `max_body_size`, and `security` (auth on/off) weren't relayed. Now mirrored.
- **Plugin `_admin_headers` ignored env-var ADMIN_API_KEY** — same fix as MCP.
- **Re-clone with a changed `RELAY_PORT` left `base_url` stale** — the
  `-proxied` entry kept pointing at the old port. Plugin and setup.sh now
  refresh `base_url` on re-clone alongside the client key.
- **Upstream URL with embedded credentials leaked** — `UPSTREAM_BASE` was
  logged verbatim (startup, config-reload) and returned unmasked by the
  unauthenticated `/health` endpoint and `/admin/reload-config`. Now masked
  (`user:pass@` stripped) everywhere it's displayed; internal URL
  construction still uses the real value.
- **OPTIONS/HEAD returned 405** — the catch-all `/v1/{path}` route only
  declared GET/POST/PUT/DELETE/PATCH, so OPTIONS/HEAD were never forwarded
  upstream. Both now route through the proxy path (CORS preflights still
  intercepted locally by CORSMiddleware).
- **`/relay switch upstream|auth` with no value** fell through to "Unknown
  subcommand" — now shows usage.
- **setup.sh claimed success without verification** — the banner printed
  "✅ Setup complete!" even when the relay never started (port in use, crash
  on boot). Health is now checked; failure shows journalctl output and a
  "relay NOT verified running" banner instead of a false success.
- **Proxy URL validation accepted invalid ports** — `:0` and `:99999` entered the
  pool and wasted slots. Ports now validated `1..65535`.
- **Admin reset-proxy leaked the proxy URL** in logs/responses. Now masked.

### Tests
- 443 → 476 tests, **100% line coverage** across relay, plugin, and MCP.

## [1.4.0] — 2026-07-31

### Added
- **`MAX_BODY_SIZE`** (default 100MB) — request bodies over the cap get 413
  before being buffered. The relay reads bodies fully into memory (needed for
  cross-proxy retries), so an unbounded body was a memory-exhaustion risk on
  open relays. Content-Length is pre-checked (cheap reject); body streaming
  reads at most cap+1 bytes so oversized uploads never fully buffer.
  Configurable via env or config.json; reported in `/health` as `max_body_size`.
- **Streaming retry across proxies** — a connect failure on one proxy no
  longer kills a streaming request when another proxy is healthy. Streaming
  now uses the same retry loop as single-shot requests (body is in memory, so
  retrying before any bytes reach the client is safe). Also retries on
  pre-stream upstream 5xx. Rotation-stall guard prevents infinite loops.
- **Stream time-to-first-byte latency** — `_proxy_stream` now records latency
  so pool latency stats reflect streaming traffic, not just single-shot.
- **CI coverage gate 85% → 100%** — the suite achieves full line coverage;
  the gate now enforces it. Ruff lint is a hard failure (was `|| true`).

### Fixed
- **`/admin/reload-config` stale clients** — proxied requests through
  `/admin/reload-config` left pooled httpx clients alive for proxies removed
  from the list. Now prunes them (matches `/admin/reload-proxies`).
- **Concurrency-limit bypass** — `/v1/models` refresh and
  `/admin/upstream-health` called upstream without the semaphore, so they
  could exceed `MAX_CONCURRENT_UPSTREAM` under a cold-cache flood. Both now
  acquire it; models serve cache and health returns 503 when at capacity.
- **Stream latency skew** — `_proxy_stream` recorded latency for every status;
  a proxy that 429s instantly looked artificially fast. Now only success-ish
  (`< 400`) responses count, matching `_proxy_single`.
- **Health checker over-kill** — a single partial sweep (some proxies reach the
  target, some don't) permanently killed the failing proxies for 24h, even when
  their network merely blocks the generic health target. Now requires
  `HEALTH_FAIL_THRESHOLD` (default 3) consecutive failures, resets on success,
  and prefers checking `UPSTREAM_BASE` when configured.
- **Semaphore resize race** — in-flight requests released the module-global
  semaphore, which a concurrent reload may have swapped; releases could
  over-credit the new semaphore and exceed the concurrency limit. `_acquire_semaphore`
  now returns the acquired object and every caller releases that same object.
- **Stream detection memory copy** — the full body was `.lower()`-ed (a 2nd
  copy of multi-MB vision payloads) just to check `"stream": true`. Now a
  case-insensitive scan of the first 8KB (top-level key always appears early).
- **Client-pool eviction race** — LRU eviction closed the least-recently-used
  client even while a request was using it, aborting the in-flight call and
  misattributing the failure to the proxy. Eviction now skips in-use clients
  (usage tracked via `_borrow_client`); if every client is busy the pool
  temporarily exceeds its cap rather than kill a request.
- **Admin key not hot-reloadable** — `/admin/reload-config` updated upstream
  and client keys but left `ADMIN_API_KEY` stuck on the old value. Now reloads.
- **CHANGELOG section order** — 1.3.0 was listed below 1.2.0. Reordered.

### Tests
- 412 → 443 tests, **100% line coverage** across relay, plugin, and MCP.

## [1.3.0] — 2026-07-31

### Fixed (privacy & correctness)
- **IP leak:** `/v1/models` and `/admin/upstream-health` fetched upstream with a
  direct `httpx.AsyncClient`, leaking the relay's real IP — defeating the proxy
  pool's purpose. Both now route through `_proxy_single` via a pool proxy.
- **Stale models after hot-reload:** switching `UPSTREAM_BASE` via
  `/admin/reload-config` served models from the OLD upstream for up to 5
  minutes. The models cache is now invalidated on upstream change.
- **Retry-loop stall:** an untried-but-cooling proxy made `pool.next()` return
  already-tried proxies forever (attempt never incremented). A rotation-stall
  guard breaks after a full rotation of duplicates.
- **Transfer-Encoding forwarded upstream:** chunked client requests passed a
  stale framing header that conflicted with httpx's own body framing. Now
  stripped in `_build_headers`.
- **Retry-After parsing:** negative/zero cooldowns (past HTTP-date, `Retry-After:
  0`) are clamped to a 10s minimum; naive HTTP-dates (no timezone suffix) are
  parsed as UTC instead of erroring to the 60s default.
- **Credential leak in request logs:** query params like `?api_key=...` /
  `?token=...` were logged verbatim. Now redacted (`api_key=***`) in the
  logging middleware.
- **`/v1/models` open on authed relays:** with `CLIENT_API_KEY` set, the models
  endpoint now returns 401 without a valid key (metadata no longer leaks).

### Added
- **`SEMAPHORE_WAIT_SECONDS`** (default 30): requests waiting for a concurrency
  slot beyond this return `503 relay_at_capacity` instead of hanging forever.
- **Live concurrency limit:** hot-reloading `MAX_CONCURRENT_UPSTREAM` now
  recreates the semaphore — the new limit takes effect immediately.
- **IPv6 proxy support:** `socks5://user:pass@[::1]:1080`, `[2001:db8::1]`,
  and zone-id forms (`[fe80::1%eth0]`) accepted by proxy URL validation.
- **Quieter request logs:** `/health` polls log at DEBUG (was INFO noise);
  query strings now included so stream vs non-stream is visible at a glance.
- **`CLIENT_API_KEY`** — optional auth for `/v1/*` proxied requests
  (`Authorization: Bearer <key>` or `X-API-Key: <key>`). Stops the relay
  being used as an open proxy that burns upstream credits. Auto-generated by
  `/relay setup clone` and embedded in the `-proxied` provider entry.
- **`/relay switch clientkey`** — rotate the client API key at runtime
  (updates config.json + proxied provider entries + hot-reloads).
- **Configurable health target** — `PROXY_HEALTH_CHECK_URL` replaces the
  hardcoded httpbin.org check (default unchanged).
- **Health security block** — `/health` reports `security.client_auth_enabled`
  and `security.admin_auth_enabled`; plugin status + MCP health surface them.
- **MCP `proxy_relay_reload_config`** — hot-reload upstream config from MCP
  (11 tools total).
- **100% line coverage** across relay, plugin, and MCP.

## [1.2.0] — 2026-07-31

### Fixed (security)
- **Auth hole:** `admin_reset_by_errors` was missing the `ADMIN_API_KEY` auth
  check — anyone could call it. Now gated by the admin middleware.
- **Admin auth unified:** Removed the dead dual auth mechanism
  (`_check_admin_auth` checking Bearer/X-API-Key). The admin middleware
  (`X-Admin-Key` header) is now the single gate for all `/admin/*` endpoints.
  Previously the middleware's approval was overridden by endpoints checking
  different headers — clients using `X-Admin-Key` got 401 despite correct auth.
- **API key masking:** Plugin `setup list`/`clone` and `setup.sh` displayed
  short API keys in full. New `_mask_key()` helper never reveals keys ≤ 8
  chars (fully hidden or 2+2).
- **Header leak:** `X-Admin-Key` (relay admin credential) was forwarded to the
  upstream API on `/v1/*` requests. Now stripped in `_build_headers`.

### Fixed (correctness)
- **Infinite retry loop:** When `MAX_REQUEST_RETRIES` exceeded the pool size
  and every proxy returned a retryable 5xx, the retry loop spun forever
  (`continue` on tried URLs without advancing). Now breaks once all proxies
  have been tried.
- **Streaming shutdown bug:** `_stream_shutdown_event` was set on shutdown but
  never cleared on startup. After any process restart, every streaming
  response returned "Server shutting down" instead of relaying the upstream
  stream. The event is now cleared at lifespan startup.
- **Plugin crash:** `/relay setup` would crash with `NameError` because
  `_cmd_setup` was referenced in `_handle_slash` but never imported (only
  mentioned in the docstring). Import now happens after helper definitions
  to avoid a circular import.
- **Plugin crash:** `plugin/_cmd_setup.py` referenced `_read_custom_providers`,
  `_infer_auth_type`, `_write_relay_config`, etc. — all defined in the parent
  package but never imported into the module. `/relay setup clone <N>` would
  crash with `NameError`. All names now explicitly imported.
- **Error counting:** Proxy connect failures in the non-streaming retry loop
  never incremented `request_stats.errors` — `/health` showed `errors: 0`
  even when every request failed with 502. Now counted.
- **Health checker pool nuke:** If the health target (httpbin.org) was down,
  ALL proxies were marked permanently dead in one sweep — destroying the pool
  for a transient external issue. Now proxies are only marked dead if at least
  one OTHER proxy succeeded in the same sweep.
- **4xx pool hygiene:** Client errors (400/401/404/422) cooled the proxy for
  30s, so a single bad client request could degrade the whole pool. Now only
  proxy-related 4xx (407/408/425) trigger cooldown.
- **Version drift:** Health endpoint and FastAPI app reported `1.0.0` while
  `--version` printed `1.1.0`. Single `VERSION` constant now used everywhere.
- **Stale pooled clients:** Proxies removed via `/admin/reload-proxies` kept
  their httpx clients alive until LRU eviction. Now pruned on reload.
- **Confusing 502:** Empty `UPSTREAM_BASE` produced a confusing httpx error.
  Now returns a clear 503 with `upstream_not_configured`.
- **`python -m relay.relay` warning:** `relay/__init__.py` eagerly imported
  `relay.relay`, triggering runpy's sys.modules warning. Now uses lazy
  `__getattr__` (PEP 562).

### Performance
- **True LRU client pool:** Eviction was FIFO (heavily-reused clients could be
  evicted first). Now `OrderedDict.move_to_end` gives real least-recently-used
  eviction.
- **Bounded admin rate limiter:** `_admin_rate_hits` grew unboundedly under a
  spoofed/fan-out IP flood. Stale IP entries are pruned above 1000 distinct IPs.
- **Deduplicated proxy list:** Duplicate URLs in the list/env created duplicate
  pool entries (wasted slots, double-tried in retry). Collapsed on init.
- **11x faster test suite:** Lifespan hardcoded `await asyncio.sleep(5)` on
  every shutdown. Now `RELAY_SHUTDOWN_DRAIN_SECONDS` (default 5, tests use 0).
  Suite dropped from ~92s to ~6s.

### Added
- **`--check` flag** — validate config (upstream, auth type, proxies) and exit
  without starting the server. Non-zero exit on errors. Useful for systemd
  health checks and CI.
- **`/admin/reload-config`** — hot-reload upstream, auth, and proxy list from
  config.json/env without a process restart.
- **`/relay switch upstream`/`auth`** — now hot-reloads when the relay is
  running (no restart needed).
- **`/relay status`** — now shows relay version and uptime.
- **Whitespace-tolerant stream detection:** `"stream": true` now detected with
  any JSON whitespace via regex; `"stream": "true"` (string) never
  false-positives; `"streaming": true` never matches.
- **Smoke test** (`scripts/smoke_test.sh`) — end-to-end checks against a
  live relay (health, models, chat, streaming, admin auth, version). Wired
  into Makefile and CI.
- **Benchmark** (`scripts/benchmark.sh`) — measures relay request-processing
  ceiling.
- **Pre-commit config** — ruff + basic hooks.
- **`__version__`** on the `relay` package (lazy).
- **README env var table** — all config options documented.
- **Test suite expanded** across 10 files: proxy validation, admin auth/rate
  limiting, config loading, retry, streaming errors, latency, models cache,
  bootstrap, health checker, main() CLI, mock-transport relay paths, E2E,
  edge paths, plugin helpers, MCP tools.

## [1.1.0] — earlier

- Request retry across up to 3 different proxies on transient failure
- Background proxy health checker (marks dead proxies permanently failed)
- Shared httpx client pool with LRU cap (100 clients)
- Admin rate limiting (20 req/min/IP)
- `ADMIN_API_KEY` auth on admin endpoints
- Dockerfile, pyproject.toml, Makefile, CI workflow
- `--version` flag

## [1.0.0] — initial

- FastAPI relay with SOCKS5 rotation and dynamic 429 cooldown
- Hermes plugin with `/relay` slash commands
- MCP server with pool management tools
- Setup script (venv, plugin, systemd)
