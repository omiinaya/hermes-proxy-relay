# Contributing

Thank you for considering contributing to Hermes Proxy Relay!

## Development Setup

```bash
git clone https://github.com/omiinaya/hermes-proxy-relay.git
cd hermes-proxy-relay
pip install -r requirements.txt
pip install pytest
```

## Running Tests

```bash
pytest tests/ -v
```

697 tests across 16 test files (100% line coverage):
- `tests/test_cooldown_pool.py` — Thread-safe proxy pool with 429 cooldown
- `tests/test_relay_endpoints.py` — FastAPI endpoint integration tests
- `tests/test_relay_utils.py` — Utility functions (headers, model filtering, retry-after)
- `tests/test_relay_advanced.py` — Proxy validation, admin auth, rate limiting, retry, streaming errors
- `tests/test_relay_remaining.py` — Latency (EWMA), models cache, bootstrap, health checker, main() entry, config check
- `tests/test_relay_mock_upstream.py` — _proxy_single/_proxy_stream via httpx.MockTransport
- `tests/test_relay_e2e.py` — End-to-end TestClient tests with mocked upstream
- `tests/test_relay_edges.py` — Edge paths (init pool, health checker branches, signal handlers, semaphore, client auth)
- `tests/test_relay_package.py` — Package exports (lazy VERSION)
- `tests/test_plugin_mcp.py` — Plugin slash commands and MCP tools
- `tests/test_relay_resilience.py` — Resilience + production-parity ports (model exhaust sweeps, alias translation, truncation, auth-switch reborrow, single-pass body parse, stream idle timeout, client-pool auto-scale)
- `tests/test_auth_switcher.py` — AuthSwitcher state machine, probes, anti-flap, persistence
- `tests/test_relay_scaling.py` — Dynamic cap (CPU+disk), concurrency scaling
- `tests/test_config_drift.py` — Config subsystem drift guards (import parity, env precedence, reload parity, binding set)

Coverage enforcement: CI fails below 100% (`--cov-fail-under=100`).

```bash
# Run with coverage locally
pytest tests/ -v --cov=relay --cov-report=term-missing
```

To run a single test file:

```bash
pytest tests/test_cooldown_pool.py -v
```

## Code Style

- **Python 3.10+** — use type hints on all public functions
- **Line length**: 100 characters max
- **Naming**: `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_CASE` for globals
- **Error handling**: prefer specific exception types over bare `except:`
- **Async**: use `asyncio` throughout (the relay is fully async)

## How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Make your changes — keep them focused on one concern
4. Add or update tests as needed
5. Run the full test suite before committing
6. Commit with a clear message describing the change
7. Open a pull request against `main`

## Pull Request Guidelines

- Keep PRs small and focused — one feature or fix per PR
- Update documentation (README.md or AGENTS.md) if behaviour changes
- Do not bump the version — maintainers handle releases
- Reference related issues in the PR description

## Adding Configuration

If you add a new config option:

1. Add a default in `_DEFAULT_CONFIG` (relay.py)
2. Add env var loading after the `PERMANENT_COOLDOWN_SECONDS` block
3. Add it to the `main()` function's global re-assignment
4. Document it in README.md's environment variables table
5. Add a test for the new behaviour

## Adding MCP Tools

MCP tools go in `mcp/mcp_server.py`. Each tool is an `@mcp.tool()` decorated async function
that delegates to a synchronous `tool_*()` implementation. Keep the sync function focused
on data fetching/transformation; the MCP decorator handles the tool registration.

## Adding Slash Commands

Plugin slash commands are in `plugin/__init__.py` and `plugin/_cmd_setup.py`:

- Add the handler function (`_cmd_*`)
- Wire it in `_handle_slash()` dispatch table
- Add help text in the help command
