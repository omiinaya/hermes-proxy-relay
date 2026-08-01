# Changelog

All notable changes to Hermes Proxy Relay.

## [1.4.1] — 2026-08-01

### Fixed
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
- **Proxy URL validation accepted invalid ports** — `:0` and `:99999` entered the
  pool and wasted slots. Ports now validated `1..65535`.
- **Admin reset-proxy leaked the proxy URL** in logs/responses. Now masked.

### Tests
- 443 → 472 tests, **100% line coverage** across relay, plugin, and MCP.

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
  auto-star, health checker, main() CLI, mock-transport relay paths, E2E,
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
