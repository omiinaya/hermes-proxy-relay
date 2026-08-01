# Hermes Proxy Relay — Claude Code Quickstart

**First, read [AGENTS.md](./AGENTS.md)** — the full agent onboarding. This file is a signpost.

## One-Line Install
```bash
cd ~/hermes-proxy-relay && ./scripts/setup.sh
```

After install, the user provides proxy URLs and the relay is ready.

## Critical Rules

- **Never replace the original** `custom_providers` entry — always create a `-proxied` variant
- **Never hardcode secrets** — API keys go in `~/.hermes/proxy-relay/config.json` (chmod 600)
- **Never remove the proxy loop filter** in `_read_custom_providers()`
- **Env vars win over config.json** — check both when debugging

## Structure

```
relay/
└── relay.py        # Self-contained FastAPI relay (~1310 lines)
plugin/
├── __init__.py     # Plugin: /relay commands (~460 lines)
├── _cmd_setup.py   # /relay setup list|clone logic
└── plugin.yaml
mcp/
└── mcp_server.py   # MCP tools (11 tools for relay management)
scripts/
└── setup.sh        # One-command install (venv, plugin, systemd)
tests/
├── conftest.py
├── test_cooldown_pool.py      # 38 tests
├── test_relay_endpoints.py    # 26 tests
├── test_relay_utils.py        # 19 tests
├── test_relay_advanced.py     # 52 tests
├── test_relay_remaining.py    # 38 tests
├── test_relay_mock_upstream.py # 80 tests
├── test_relay_e2e.py          # 16 tests
├── test_relay_edges.py        # 28 tests
└── __init__.py
AGENTS.md           # ← Full agent onboarding
```

## Common Tasks

| Task | Command |
|------|---------|
| Install everything | `./scripts/setup.sh` |
| Start relay | `PROXY_LIST=~/.hermes/proxy-relay/proxies.txt python relay/relay.py` |
| Check health | `curl -s http://localhost:4002/health` |
| Run tests (425 total, 100% coverage) | `python3 -m pytest tests/ -v` |
| Relay logs | `journalctl --user -u hermes-proxy-relay -n 50 --no-pager` |
| Plugin commands | `/relay setup list`, `/relay setup clone <N>`, `/relay status`, `/relay logs` |

See [AGENTS.md#task--file-mapping](./AGENTS.md#task--file-mapping) for full file mapping.
