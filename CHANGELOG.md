# Changelog

All notable changes to Hermes Proxy Relay.

## [1.2.0] — 2026-07-30

### Fixed
- **Security:** `admin_reset_by_errors` was missing the `ADMIN_API_KEY` auth
  check — anyone could call it. Now gated by the admin middleware.
- **Admin auth unified:** Removed the dead dual auth mechanism
  (`_check_admin_auth` checking Bearer/X-API-Key). The admin middleware
  (`X-Admin-Key` header) is now the single gate for all `/admin/*` endpoints.
  Previously the middleware's approval was overridden by endpoints checking
  different headers — clients using `X-Admin-Key` got 401 despite correct auth.
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
- **Version drift:** Health endpoint and FastAPI app reported `1.0.0` while
  `--version` printed `1.1.0`. Single `VERSION` constant now used everywhere.

### Added
- **Test suite expanded 71 → 212 tests** across 9 files, 93% line coverage:
  - Proxy URL validation (12), admin rate limiting (5), config loading (5),
    proxy loading (7), shared client pool (5), admin middleware auth (4),
    admin rate-limit endpoint (1), retry logic (2), streaming errors (2)
  - Latency tracking (5), models cache (2), auto-star (5), health checker (2),
    main() CLI entry (4)
  - `_proxy_single`/`_proxy_stream` via `httpx.MockTransport` (16)
  - End-to-end TestClient tests (8)
  - Edge paths — init pool, health checker branches, signal handlers (13)
  - Plugin helpers, slash commands, MCP tools (39)
- **Smoke test script** (`scripts/smoke_test.sh`) — starts the relay, verifies
  11 checks across health, models, chat, streaming, admin auth, and version.
  Wired into Makefile (`make smoke`) and CI.
- **CI coverage enforcement** — workflow fails below 85% coverage
  (`--cov-fail-under=85`).
- **Pre-commit config** (`.pre-commit-config.yaml`) — ruff + basic hooks.
- **`__version__`** on the `relay` package for programmatic version discovery.
- **README env var reference table** — all 16 config options documented.

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
