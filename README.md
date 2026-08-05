# Hermes Proxy Relay

[![CI](https://github.com/omiinaya/hermes-proxy-relay/actions/workflows/test.yml/badge.svg)](https://github.com/omiinaya/hermes-proxy-relay/actions/workflows/test.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-644%20passing-green.svg)](#test-status)

A lightweight SOCKS5 proxy rotation relay for [Hermes Agent](https://hermes-agent.nousresearch.com).
Routes LLM API calls through a pool of user-provided SOCKS5 proxies with automatic
rate-limit cooldown, concurrency safety, and zero amplification bombs.

```text
Hermes Agent  ──►  Relay (localhost:4002)  ──►  SOCKS5 pool  ──►  Upstream API
```

## Quick Start

### One-Command Install

```bash
git clone https://github.com/omiinaya/hermes-proxy-relay.git
cd hermes-proxy-relay
./scripts/setup.sh
```

The script will ask for upstream details and proxy list path, then offer to
install a systemd service so the relay survives logout. Zero manual steps beyond
pasting your proxy credentials.

### Clone a Provider (Plugin)

```bash
# In Hermes:
/relay setup list      # see existing providers
/relay setup clone 1   # clone one with proxy routing
```

Or skip the plugin entirely — the relay is self-contained:

```bash
pip install -r requirements.txt
PROXY_LIST=~/proxies.txt \
UPSTREAM_BASE=https://api.openai.com/v1 \
UPSTREAM_API_KEY=sk-... \
  python relay/relay.py
```

## Features

- **Clone any provider** — `/relay setup clone <N>` duplicates a `custom_providers`
  entry with relay routing. Never touches the original.
- **Proxy rotation** — Round-robin through N SOCKS5 proxies from a file or env var
- **Shared connection pool** — httpx clients are reused across requests instead of one-per-request (~40x fewer connections under load). Pool capped with LRU eviction; the cap **auto-scales to the proxy count** (`max(CLIENT_POOL_MAX, #proxies)`) so round-robin rotation never pays a fresh SOCKS5+TLS handshake for a non-pooled proxy.
- **Automatic retry** — Non-streaming requests retry across up to 3 different proxies on transient failure (5xx upstream, connection timeout). Avoids retrying the same failed proxy. Exponential backoff (100ms → 1s) between attempts; retries fail fast on a busy semaphore instead of stacking 30s waits.
- **Background health checker** — Periodically tests each proxy's connectivity via httpbin.org. Dead proxies are automatically marked as permanently failed.
- **Admin API key** — Optional `ADMIN_API_KEY` auth on all admin endpoints via `X-Admin-Key` header. Admin rate limiter (20 req/min/IP) prevents abuse.
- **Startup validation** — Warns on missing upstream base, empty API key, and no configured proxies.
- **Smart proxy deactivation** — Proxies are only marked permanently dead when at least one OTHER proxy succeeds in the same health sweep (a downed health target never nukes the whole pool).
- **4xx-aware pool hygiene** — Client errors (400/401/404/422) relay without cooling the proxy; only proxy-related 4xx (407/408/425) trigger cooldown.
- **Whitespace-tolerant stream detection** — `"stream": true` detected with any JSON whitespace; `"stream": "true"` (string) never false-positives.
- **Duplicate proxy dedup** — Duplicate URLs in the list/env are collapsed on init.
- **Dynamic 429 cooldown** — Proxy cooled for the exact `Retry-After` duration.
  Skipped during cooldown. Zero upstream calls when all cooling.
- **Dynamic concurrency cap** — `DYNAMIC_CAP_ENABLED` replaces the fixed
  `MAX_CONCURRENT_UPSTREAM` (base 24) with an **auto-tuned** limit: a background
  task samples process CPU (`getrusage`) and the busiest real block device's I/O
  (`/proc/diskstats`) every 5s and grows/shrinks the cap to use up to ~90% CPU /
  ~70% disk without ever pegging either (hard backoff above 96%/85%). In idle
  conditions the cap grows toward `DYNAMIC_CAP_MAX` (500) — **effectively no hard
  cap**; throughput is bounded by the upstream's own rate limits + retries, not
  by the relay. `HOLD_PERMIT_FOR_STREAM=true` (default) lets the cap govern
  stream lifetime; set `false` to release the permit after connection setup for
  unbounded stream concurrency (opt-in, can saturate upstream queues).
- **Streaming** — SSE streaming through the relay (client lifecycle outside generator).
  Per-chunk `STREAM_IDLE_TIMEOUT` (default 60s) releases a silent mid-stream proxy's
  concurrency slot + pooled client instead of holding them for the full read timeout.
- **Auth translation** — Strips Hermes auth headers, rewrites with upstream key.
  Supports `bearer` and `x-api-key` modes. Auto-inferred from provider name hints and API key value.
  Relay-managed headers (`X-Admin-Key`, `Accept-Encoding`, etc.) never reach upstream.
- **Model filtering** — Regex pattern to filter which upstream models are exposed
- **Model cache with TTL** — Model list auto-refreshes every 5 minutes
- **CORS support** — All origins/methods/headers allowed for browser-based clients
- **Request logging middleware** — Per-request timing and status code logging
- **Config file or env vars** — Relay auto-loads `~/.hermes/proxy-relay/config.json`
  (written by the plugin). Env vars take precedence.
- **Uptime tracking** — Health endpoint reports uptime_seconds and version
- **Config check** — `python relay/relay.py --check` validates upstream,
  auth type, and proxy list without starting the server (exit 0/1)
- **Hot config reload** — `POST /admin/reload-config` re-reads config.json
  and updates upstream/auth/proxies in place (no restart needed)
- **Client API key** — Optional `CLIENT_API_KEY` auth on `/v1/*` requests
  (`Authorization: Bearer` or `X-API-Key`). Auto-generated by
  `/relay setup clone` and embedded in the `-proxied` entry — stops the
  relay being used as an open proxy that burns upstream credits.
  Rotate anytime with `/relay switch clientkey`.
- **Smart auth switching** — Detects upstream auth-method changes (e.g.
  OpenCode Zen flipping `x-api-key` → Bearer) and self-heals. Only a 401
  counts as an auth signal; on repeated 401s the relay probes alternate
  auth types with the same key against `/models`, adopts the first that
  verifies, retries the current request, and persists the fix. See
  `AUTH_SWITCH_*` env vars below.
- **Overload protection** — Requests waiting > `SEMAPHORE_WAIT_SECONDS`
  (default 30s) for a concurrency slot get `503 relay_at_capacity` instead
  of hanging; the limit is auto-tuned live by the dynamic cap (or hot-reload
  `MAX_CONCURRENT_UPSTREAM` to resize a fixed cap in place).
  `MAX_QUEUED_REQUESTS` (default 100) bounds how many requests may queue for
  a permit — beyond that, new requests fail fast instead of piling up behind
  long-held stream permits. `/health` reports `semaphore.queued` and the
  `dynamic_cap` block (effective_max, cpu_pct, disk_pct, adjustments).
- **Pooled streaming clients** — streams reuse the shared per-proxy httpx
  client (warm TCP/TLS/SOCKS5 connection) instead of paying a fresh
  handshake per stream. Eviction skips in-use clients, so a live stream is
  never aborted. `HOLD_PERMIT_FOR_STREAM` (default `true`) holds the
  concurrency permit for the whole stream; set `false` to release it after
  connection setup for unbounded stream throughput.
- **IPv6 proxies** — `[::1]`, `[2001:db8::1]`, and zone-id forms accepted
  in proxy URLs.
- **Configurable health target** — `PROXY_HEALTH_CHECK_URL` replaces the
  hardcoded httpbin.org check (default unchanged).
- **Streaming resilience** — streaming requests retry across proxies on
  connection failure, matching the single-shot path.
- **Request body cap** — `MAX_BODY_SIZE` (default 100MB) returns 413 for
  oversized bodies before buffering, preventing memory exhaustion.
- **100% line coverage** — relay, plugin, and MCP fully tested (644 tests).

## Architecture

```text
┌──────────────────────┐
│   Hermes Agent       │
│   provider: <X>-proxied  │
└──────────┬───────────┘
           │ GET/POST /v1/...
           ▼
┌──────────────────────────┐
│   FastAPI Relay          │  port 4002
│   relay/relay.py          │
├──────────────────────────┤
│  ┌────────────────────┐  │
│  │   CooldownPool     │  │  429 → cool for Retry-After
│  │   • N proxies      │  │  All cooling → immediate 429
│  │   • Round-robin    │  │
│  │   • Dynamic cap    │  │  auto-tuned by CPU+disk headroom
│  └────────┬───────────┘  │
└───────────┬──────────────┘
            │ httpx[socks]
            ▼
┌──────────────────────┐
│   SOCKS5 Proxy Pool  │  User-provided
│   (decodo, IPVanish, │
│    residential, etc) │
└──────────────────────┘
```

## Environment Variables

All configuration is via environment variables. A JSON config file
(`~/.hermes/proxy-relay/config.json` by default) can also be used — env vars
always take precedence.

| Variable | Default | Description |
|----------|---------|-------------|
| `UPSTREAM_BASE` | `""` | Upstream API base URL (e.g. `https://api.openai.com/v1`). **Required.** |
| `UPSTREAM_API_KEY` | `""` | Upstream API key. **Required.** |
| `UPSTREAM_AUTH_TYPE` | `bearer` | Auth header type: `bearer` (Authorization) or `x-api-key` |
| `RELAY_PORT` | `4002` | Port the relay listens on |
| `PROXY_LIST` | `""` | Path to a text file with one proxy URL per line |
| `PROXY_LIST_ENV` | `""` | Comma-separated proxy URLs inline (alternative to file) |
| `ADMIN_API_KEY` | `""` | If set, requires `X-Admin-Key` header on all `/admin/*` endpoints |
| `CLIENT_API_KEY` | `""` | If set, requires `Authorization: Bearer <key>` or `X-API-Key: <key>` on `/v1/*` proxied requests. **Prevents open-proxy abuse** when the relay is reachable beyond localhost. Auto-generated by `/relay setup clone`. |
| `MAX_CONCURRENT_UPSTREAM` | `24` | **Base** concurrency limit (fixed only when `DYNAMIC_CAP_ENABLED=false`). With the dynamic cap on, this is the starting value the adjuster tunes. When held per-stream (`HOLD_PERMIT_FOR_STREAM=true`), the effective cap == max concurrent streams == max concurrent conversations |
| `MAX_QUEUED_REQUESTS` | `100` | Bounded semaphore backlog — when this many requests are already waiting for a permit, new ones fail fast with 503 (`0` = unlimited) |
| `HOLD_PERMIT_FOR_STREAM` | `true` | Hold the concurrency permit for the whole stream lifetime (upstream-queue-safe). Set `false` to release it after connection setup for unbounded stream throughput (opt-in; can saturate upstream queues) |
| `DYNAMIC_CAP_ENABLED` | `false` | Auto-tune the concurrency cap to CPU + disk-I/O headroom instead of a fixed limit. Requires `HOLD_PERMIT_FOR_STREAM=true` to actually bound stream lifetime |
| `DYNAMIC_CAP_CPU_TARGET_PCT` | `90` | Grow toward up to this % of one core, ease down above it |
| `DYNAMIC_CAP_CPU_MAX_PCT` | `96` | Hard backoff (2× step) above this — the core is never pegged |
| `DYNAMIC_CAP_DISK_TARGET_PCT` | `70` | Busiest real block device utilization target (lower than CPU — I/O latency collapses near saturation) |
| `DYNAMIC_CAP_DISK_MAX_PCT` | `85` | Hard backoff when the busiest disk exceeds this |
| `DYNAMIC_CAP_MIN` | `10` | Floor the auto-tuned cap never goes below |
| `DYNAMIC_CAP_MAX` | `500` | Ceiling — in idle conditions the cap grows toward this (effectively no hard cap) |
| `DYNAMIC_CAP_INTERVAL_S` | `5` | Sample interval for the CPU/disk adjuster |
| `DYNAMIC_CAP_STEP` | `0.10` | Fraction to grow/shrink the cap per adjustment |
| `DYNAMIC_CAP_SMOOTHING` | `0.3` | EWMA smoothing (0–1) — damps single-interval spikes so the cap doesn't thrash |
| `HEALTH_CHECK_CONCURRENCY` | `20` | Max simultaneous probes per health-check sweep (a 250-proxy pool is swept in ~N/20 × probe-time instead of N × probe-time serially) |
| `RELAY_WORKERS` | `1` | uvicorn worker processes. `>1` = each worker has its OWN pool/cooldown/health state (NOT shared) — opt-in raw-throughput scaling |
| `RELAY_MAX_CONNECTIONS` | `0` | Inbound connection cap passed to uvicorn (`0` = uvicorn default/unlimited). Guards against FD exhaustion / slow-loris |
| `RELAY_BACKLOG` | `0` | TCP listen backlog passed to uvicorn (`0` = uvicorn default 2048) |
| `UPSTREAM_CONNECT_TIMEOUT` | `15` | Upstream connection timeout (seconds) |
| `UPSTREAM_READ_TIMEOUT` | `120` | Upstream read timeout (seconds) — per-chunk between bytes on streams; slow-but-alive upstreams/streams survive longer |
| `STREAM_IDLE_TIMEOUT` | `60` | Per-chunk idle bound on SSE streams — a silent mid-stream proxy releases its concurrency slot + pooled client after this instead of holding them for `UPSTREAM_READ_TIMEOUT`. `0` = use the read timeout (no extra bound) |
| `CLIENT_POOL_MAX` | `100` | **Floor** for the pooled httpx client count. The effective cap auto-scales to `max(CLIENT_POOL_MAX, #proxies)` — one warm client per proxy so round-robin rotation never pays a fresh handshake |
| `CLIENT_IDLE_TTL` | `120` | Reap pooled clients idle longer than this (seconds) — stale-keep-alive prevention. `0` disables |
| `MAX_RESPONSE_SIZE` | `209715200` | Max upstream RESPONSE bytes for single-shot requests (0 disables). Oversized → 502 `response_too_large` |
| `RETRY_SEMAPHORE_WAIT_SECONDS` | `2.0` | How long a RETRY attempt waits for a concurrency slot (first attempt waits `SEMAPHORE_WAIT_SECONDS`) |
| `RETRY_BACKOFF_BASE` | `0.1` | Exponential backoff base between retry attempts (seconds). `0` = no backoff |
| `RETRY_BACKOFF_MAX` | `1.0` | Backoff cap (seconds) |
| `LATENCY_SKIP_THRESHOLD_MS` | `0` | When > 0, skip proxies measurably slower than this (ms) in favor of faster ones. `0` = pure round-robin |
| `RELAY_LOG_REQUESTS` | `true` | Log every non-/health request at INFO. Set `false` for minimum overhead at high rates |
| `MAX_REQUEST_RETRIES` | `3` | Number of retry attempts on transient proxy failure |
| `SEMAPHORE_WAIT_SECONDS` | `30.0` | Max seconds a request waits for a concurrency slot before returning 503 (overload protection) |
| `MODEL_FILTER_PATTERN` | `.*` | Regex to filter visible models (e.g., `-free$` to show only free models) |
| `CONSECUTIVE_ERROR_THRESHOLD` | `3` | Consecutive failures before a proxy is permanently marked |
| `PERMANENT_COOLDOWN_SECONDS` | `86400` | Cooldown duration (seconds) for permanently failed proxies |
| `MAX_RETRY_AFTER_SECONDS` | `3600` | Upper clamp for `Retry-After` cooldowns — hostile/absurd values can't remove a proxy from rotation for years |
| `PROXY_HEALTH_CHECK_INTERVAL` | `60` | Seconds between background proxy health checks (0 to disable) |
| `PROXY_HEALTH_CHECK_URL` | `http://httpbin.org/ip` | Target URL for proxy health checks (any fast endpoint returning <500) |
| `HEALTH_FAIL_THRESHOLD` | `3` | Consecutive health-check failures before a proxy is permanently marked dead |
| `MAX_BODY_SIZE` | `104857600` | Max request body bytes — larger bodies get 413 (0 disables) |
| `RELAY_CONFIG` | `~/.hermes/proxy-relay/config.json` | Path to JSON config file |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `RELAY_AUTO_STAR` | `""` | Set to `1` + GITHUB_TOKEN to auto-star the repo at startup (explicit opt-in) |
| `AUTH_SWITCH_ENABLED` | `true` | Enable smart auth-type fallback (detect upstream method flips and self-heal) |
| `AUTH_SWITCH_CANDIDATES` | `bearer,x-api-key` | Ordered auth types to try when the active one starts 401ing |
| `AUTH_SWITCH_TRIGGER_THRESHOLD` | `3` | Consecutive upstream 401s before probing alternates |
| `AUTH_SWITCH_PROBE_SUCCESSES` | `2` | Consecutive probe 200s required before adopting a candidate |
| `AUTH_SWITCH_COOLDOWN_S` | `300` | Minimum seconds between probes (anti-flap) |
| `AUTH_SWITCH_MAX_PER_WINDOW` | `3` | Max auto-switches per `AUTH_SWITCH_WINDOW_S`; exceeding latches a `flapping` alert |
| `AUTH_SWITCH_WINDOW_S` | `3600` | Sliding window for the max-per-window switch cap |
| `AUTH_STATE_PATH` | `~/.hermes/proxy-relay/auth_state.json` | Where the verified auth type is persisted (restart-safe) |

## The Clone Workflow (Plugin)

The Hermes plugin at `plugin/` automates the entire setup:

```bash
# List existing custom_providers
/relay setup list
# → 📋 Existing Custom Providers
#     1. spacetimellm       → http://localhost:4000/v1
#     2. ds-v4-flash        → http://localhost:4000/v1
#     3. mimo-v2.5          → http://localhost:4000/v1

# Clone one with proxy routing
/relay setup clone 1
# → ✅ Cloned: spacetimellm → spacetimellm-proxied
#
#   Original (untouched):
#     URL: http://localhost:4000/v1
#
#   Proxied entry created:
#     Name: spacetimellm-proxied
#     Routes through: localhost:4002/v1
#
#   Relay config saved to: ~/.hermes/proxy-relay/config.json
#   Auth type: bearer
```

The plugin **never replaces** the original provider. A new entry with a `-proxied`
suffix is added to `custom_providers`. The original is untouched.

### What the plugin writes

1. **Relay config** (`~/.hermes/proxy-relay/config.json`) — upstream URL, API key,
   auth type. The relay auto-reads this file on startup.
2. **Hermes provider entry** — new `custom_providers` entry like:
   ```yaml
   - name: spacetimellm-proxied
     base_url: http://localhost:4002/v1
     api_key: relay-key
     model: auto
   ```

### Smart filtering

`/relay setup list` automatically excludes:

- Entries named `proxy-relay` (the relay's own entry)
- Entries ending in `-proxied` (already cloned)
- Entries pointing at the relay port (`:4002`) — would create a proxy loop

### Auth auto-inference

| Condition | Auth type |
|---|---|
| Provider name matches known `x-api-key` patterns | `x-api-key` |
| API key = `public` | `x-api-key` |
| Everything else | `bearer` |

Override with: `/relay setup clone 2 x-api-key`

### Slash Commands

| Command | Description |
|---|---|
| `/relay setup` | Overview with cloneable provider list |
| `/relay setup list` | List existing providers with details |
| `/relay setup clone <N>` | Clone provider N with proxy routing |
| `/relay setup clone <N> x-api-key` | Clone with auth type override |
| `/relay status` | Pool health, proxy counts, cooling details |
| `/relay switch upstream <url>` | Change upstream API URL |
| `/relay switch auth <bearer\|x-api-key>` | Change upstream auth header type |
| `/relay switch clientkey` | Rotate the relay client API key |
| `/relay switch proxies` | Reload proxy list from file |
| `/relay reset <url>` | Reset a proxy's cooldown |
| `/relay reset all` | Clear all cooldowns |
| `/relay reset errors [N]` | Reset permanently-failed proxies |
| `/relay reset proxies` | Reload proxy list |
| `/relay logs` | Show recent relay log entries |
| `/relay restart` | Restart the relay service |
| `/relay help` | Full command reference |

## Configuration

The relay reads configuration from three sources (env vars win):

1. **Config file** — `~/.hermes/proxy-relay/config.json` (auto-loaded)
2. **Environment variables** — take precedence over the file
3. **`--config` flag** — override the config file path

### Config File

Written by the plugin during `/relay setup clone`:

```json
{
  "UPSTREAM_BASE": "http://localhost:4000/v1",
  "UPSTREAM_API_KEY": "sk-...",
  "UPSTREAM_AUTH_TYPE": "bearer",
  "RELAY_PORT": 4002,
  "MAX_CONCURRENT_UPSTREAM": 24,
  "HOLD_PERMIT_FOR_STREAM": true,
  "DYNAMIC_CAP_ENABLED": true,
  "DYNAMIC_CAP_CPU_TARGET_PCT": 90,
  "DYNAMIC_CAP_CPU_MAX_PCT": 96,
  "DYNAMIC_CAP_DISK_TARGET_PCT": 70,
  "DYNAMIC_CAP_DISK_MAX_PCT": 85,
  "DYNAMIC_CAP_MIN": 10,
  "DYNAMIC_CAP_MAX": 500,
  "MODEL_FILTER_PATTERN": ".*",
  "LOG_LEVEL": "INFO"
}
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `UPSTREAM_BASE` | — | Upstream API base URL (e.g. `https://api.openai.com/v1`) |
| `UPSTREAM_API_KEY` | — | API key for upstream |
| `UPSTREAM_AUTH_TYPE` | `bearer` | `bearer` or `x-api-key` |
| `PROXY_LIST` | — | Path to proxy list file (one per line) |
| `PROXY_LIST_ENV` | — | Comma-separated proxy URLs inline |
| `RELAY_PORT` | `4002` | Listen port |
| `MAX_CONCURRENT_UPSTREAM` | `24` | Base concurrency limit (fixed only when `DYNAMIC_CAP_ENABLED=false`; otherwise the dynamic cap tunes it) |
| `MAX_REQUEST_RETRIES` | `3` | Retry attempts on transient proxy failure |
| `SEMAPHORE_WAIT_SECONDS` | `30.0` | Seconds to wait for a concurrency slot before 503 |
| `MODEL_FILTER_PATTERN` | `.*` | Regex for allowed model names (e.g. `-free$`) |
| `LOG_LEVEL` | `INFO` | Logging level |
| `ADMIN_API_KEY` | `""` | If set, required as `X-Admin-Key` header on `/admin/*` routes. Protects clear-cooldowns, reset-proxy, reload-proxies, reset-by-errors. **Leave empty for no auth** (safe when relay is bound to localhost). |
| `CLIENT_API_KEY` | `""` | If set, required as `Authorization: Bearer <key>` or `X-API-Key: <key>` on `/v1/*` requests. Stops the relay being used as an open proxy. Generated automatically by `/relay setup clone <N>` and embedded in the `-proxied` provider entry. |

### Proxy List Format

One per line, `#` for comments:

```
socks5://user:pass@192.168.1.100:1080
socks5://user:pass@proxy2.example.com:1080
http://user:pass@residential-proxy:3128
```

## Project Structure

```
hermes-proxy-relay/
├── relay/
│   └── relay.py              # Self-contained FastAPI relay (CooldownPool inlined)
├── plugin/
│   ├── __init__.py            # Hermes plugin: /relay setup list|clone, status
│   └── plugin.yaml            # Plugin metadata
├── mcp/
│   ├── __init__.py
│   └── mcp_server.py          # MCP tools: status, health, clear-cooldowns
├── scripts/
│   └── setup.sh               # One-command setup (install, plugin enable)
├── examples/
│   └── config.yaml            # Example Hermes config.yaml
├── requirements.txt           # Python dependencies
├── LICENSE                    # MIT
└── README.md
```

## Development

```bash
# Clone
git clone https://github.com/omiinaya/hermes-proxy-relay.git
cd hermes-proxy-relay

# Install deps
pip install -r requirements.txt

# Run relay (reads ~/.hermes/proxy-relay/config.json automatically)
PROXY_LIST=~/proxies.txt python relay/relay.py

# Or use a custom config file
python relay/relay.py --config ./my-config.json

# Or override everything with env vars
UPSTREAM_BASE=https://api.openai.com/v1 \
UPSTREAM_API_KEY=sk-... \
PROXY_LIST_ENV=socks5://u:p@proxy:1080 \
  python relay/relay.py

# Verify
curl -s http://localhost:4002/health
curl -s -X POST http://localhost:4002/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":false}'
```

### Health endpoint

`GET /health` returns operational state for monitoring / orchestrators:

```json
{
  "status": "ok | degraded",
  "pool_stats": {
    "total": 4, "available": 3, "cooling": 1, "permanently_failed": 0,
    "cooling_details": [{"proxy": "socks5://...", "remaining_s": 12, "avg_latency_ms": 340.5}],
    "permanently_failed_details": [],
    "all_time_ok": 42, "all_time_429": 2, "avg_latency_ms": 320.1
  },
  "upstream_base": "https://api.example.com/v1",
  "models_available": 18,
  "request_stats": {"total": 50, "ok": 48, "errors": 2, "auth_failed": 1},
  "semaphore": {"max": 10, "used": 2},
  "uptime_seconds": 3600,
  "version": "1.4.1",
  "shared_clients": 3,
  "max_body_size": 104857600,
  "security": {"client_auth_enabled": true, "admin_auth_enabled": true}
}
```

- `status` is `ok` when ≥1 proxy is available, `degraded` otherwise.
- `request_stats.auth_failed` counts rejected client-auth attempts
  (credential stuffing / misconfigured clients show up here).
- `security.*` reports whether client/admin auth is enforced.

## License

MIT

---

## Docker

```bash
# Build
docker build -t hermes-proxy-relay .

# Run (all config via env vars)
docker run -d --name hermes-relay --restart unless-stopped \
  -p 4002:4002 \
  -e UPSTREAM_BASE=https://api.openai.com/v1 \
  -e UPSTREAM_API_KEY=sk-... \
  -e PROXY_LIST_ENV=socks5://user:pass@proxy:1080 \
  -e ADMIN_API_KEY=my-secret-key \
  hermes-proxy-relay

# With a proxy list file mounted
docker run -d --name hermes-relay --restart unless-stopped \
  -p 4002:4002 \
  -v /path/to/proxies.txt:/app/proxies.txt:ro \
  -e PROXY_LIST=/app/proxies.txt \
  -e UPSTREAM_BASE=https://api.openai.com/v1 \
  -e UPSTREAM_API_KEY=sk-... \
  hermes-proxy-relay

# Verify
curl -s http://localhost:4002/health
```

---

## Documentation for AI Agents

| Doc | Purpose |
|-----|---------|
| [AGENTS.md](./AGENTS.md) | Full AI agent onboarding — architecture, file mapping, conventions, pitfalls |
| [CLAUDE.md](./CLAUDE.md) | Claude Code quickstart (signpost to AGENTS.md) |
| [.cursorrules](./.cursorrules) | Cursor IDE rules and conventions |
| [CONTRIBUTING.md](./CONTRIBUTING.md) | Development setup, test suite layout, contribution guidelines |
| [CHANGELOG.md](./CHANGELOG.md) | Version history and notable changes |

## Test Status

```bash
# Run full test suite
python3 -m pytest tests/ -v

# 644 tests pass (100% coverage): resilience (incl. prod-parity ports + dynamic cap), mock-upstream, cooldown-pool, advanced, remaining, edges, e2e, utils, plugin, package
```
