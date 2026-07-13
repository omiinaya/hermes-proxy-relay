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
└── relay.py        # Self-contained FastAPI relay (540 lines)
plugin/
├── __init__.py     # Plugin: /relay setup list|clone|status (420 lines)
└── plugin.yaml
scripts/
└── setup.sh        # One-command install (venv, plugin, systemd)
AGENTS.md           # ← Full agent onboarding
```

## Common Tasks

| Task | Command |
|------|---------|
| Install everything | `./scripts/setup.sh` |
| Start relay | `PROXY_LIST=~/.hermes/proxy-relay/proxies.txt python relay/relay.py` |
| Check health | `curl -s http://localhost:4002/health` |
| Run tests | `python3 -c "..."` (inline tests in relay.cooldown_pool) |
| Relay logs | `journalctl --user -u hermes-proxy-relay -n 50 --no-pager` |
| Plugin commands | `/relay setup list`, `/relay setup clone <N>`, `/relay status` |

See [AGENTS.md#task--file-mapping](./AGENTS.md#task--file-mapping) for full file mapping.
