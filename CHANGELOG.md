# Changelog

All notable changes to Hermes Proxy Relay.

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
- **Smoke test** (`scripts/smoke_test.sh`) — 11 end-to-end checks against a
  live relay (health, models, chat, streaming, admin auth, version). Wired
  into Makefile and CI.
- **Benchmark** (`scripts/benchmark.sh`) — measures relay request-processing
  ceiling (~200-500 req/s locally).
- **CI coverage gate** — workflow fails below 85% (`--cov-fail-under=85`).
- **Pre-commit config** — ruff + basic hooks.
- **`__version__`** on the `relay` package (lazy).
- **README env var table** — all 16 config options documented.
- **Test suite expanded 71 → 239 tests** across 9 files, 94% line coverage:
  proxy validation, admin auth/rate limiting, config loading, retry, streaming
  errors, latency, models cache, auto-star, health checker, main() CLI,
  mock-transport relay paths, E2E, edge paths, plugin helpers, MCP tools.

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
