---
name: Hermes Proxy Relay
description: "Lightweight SOCKS5 proxy rotation relay for Hermes Agent — clone any custom_providers entry with proxy routing, dynamic 429 cooldown, zero amplification bomb"
stack: [python, fastapi]
ports:
  relay: 4002
deps: [python3, pip, hermes]
---

# Hermes Proxy Relay

A self-contained FastAPI relay that routes LLM API calls through a pool of user-provided
SOCKS5 proxies with dynamic rate-limit cooldown. Designed to be cloned and configured
by any Hermes custom_providers entry — never replaces originals.

**Size:** ~540 lines of Python (single file), 1,729 total repo
**Tests:** 8/8 CooldownPool unit tests
**Deps:** fastapi, uvicorn, httpx[socks], pydantic

## Quick Reference

```bash
# Install
cd ~/hermes-proxy-relay && ./scripts/setup.sh

# Run (reads config.json automatically)
PROXY_LIST=~/.hermes/proxy-relay/proxies.txt python relay/relay.py

# Or via systemd (if installed)
systemctl --user start hermes-proxy-relay

# Verify
curl -s http://localhost:4002/health
```

## Workspace Layout

```
hermes-proxy-relay/
├── relay/
│   └── relay.py              # Self-contained FastAPI relay (CooldownPool inlined)
├── plugin/
│   ├── __init__.py            # Hermes plugin: /relay setup list|clone|status
│   └── plugin.yaml            # Plugin metadata
├── mcp/
│   ├── __init__.py
│   └── mcp_server.py          # MCP tools: status, health
├── scripts/
│   └── setup.sh               # Robust install script (venv, plugin, systemd)
├── examples/
│   └── config.yaml            # Example Hermes config
├── AGENTS.md                  # ← This file — AI agent onboarding
├── CLAUDE.md                  # Claude Code quickstart (points here)
├── .cursorrules               # Cursor IDE quickstart
├── requirements.txt           # Python dependencies
├── LICENSE                    # MIT
└── README.md                  # Full architecture and examples
```

## Setup for a New User

The `scripts/setup.sh` script is the single entry point. It handles everything,
**including scanning your Hermes config and writing the relay configuration:**

```bash
# Clone the repo
git clone https://github.com/omiinaya/hermes-proxy-relay.git
cd hermes-proxy-relay

# Full install (venv + plugin + config + systemd)
./scripts/setup.sh
```

The script will:
1. Scan `~/.hermes/config.yaml` for eligible `custom_providers`
2. Present them as a numbered list (filters out already-proxied entries)
3. Ask which one to proxy through
4. Write relay config.json + add a `-proxied` Hermes provider entry
5. **Never touches the original provider**

If no eligible providers are found, it falls back to manual prompts for upstream
URL, API key, and auth type (use the plugin later for `/relay setup clone`).

### What setup.sh does

| Step | Action | Details |
|------|--------|---------|
| 1 | Check prerequisites | python3, pip, git, hermes CLI |
| 2 | Create virtualenv | `~/.hermes-proxy-relay/venv/` + pip install |
| 3 | Install Hermes plugin | Symlinks plugin/ to `~/.hermes/plugins/proxy-relay` + enables it |
| 4 | Create config directory | `~/.hermes/proxy-relay/` + proxy list placeholder |
| 5 | **Scan + clone provider** | Reads config.yaml → picks a provider → writes relay config.json + Hermes `-proxied` entry |
| 6 | Install systemd service | Optional — keeps relay alive after logout |
| 7 | Verify everything | Reports success/failure for each component |

### What the user provides

The user only needs to paste their proxy credentials — everything else is automated:

1. **SOCKS5 proxy URLs** → edit `~/.hermes/proxy-relay/proxies.txt`
2. **Upstream API URL + key** → entered during `setup.sh` (stored in `config.json`)
3. Optionally answer Y/N for systemd + linger

### What they get

```bash
# Relay running as a systemd service (survives logout)
systemctl --user status hermes-proxy-relay

# Hermes plugin for /relay commands
# In Hermes: /relay setup list → /relay setup clone <N> → /model <name>-proxied
```

## Task → File Mapping

| Task | Files to open |
|------|---------------|
| **Change relay config** (upstream, auth) | `~/.hermes/proxy-relay/config.json` or env vars |
| **Update proxy list** | `~/.hermes/proxy-relay/proxies.txt` |
| **Modify relay behaviour** (cooldown, concurrency) | `relay/relay.py` — CooldownPool class at top |
| **Change plugin slash commands** | `plugin/__init__.py` — `_handle_slash()` and `_cmd_*()` |
| **Add MCP tools** | `mcp/mcp_server.py` — add `@mcp.tool()` decorated functions |
| **Update setup script** | `scripts/setup.sh` |
| **Change Hermes provider entry format** | `plugin/__init__.py` — `_write_proxied_provider()` |
| **Modify auth inference** (x-api-key vs bearer) | `plugin/__init__.py` — `_infer_auth_type()` |
| **Run tests** | `python3 -c "import relay.cooldown_pool; ..."` (inline) |

## Architecture

```text
Hermes Agent  ──►  Relay (:4002)  ──►  SOCKS5 pool  ──►  Upstream API
  provider:                              (round-robin)
  <name>-proxied

Layers:
- Plugin (Python) — slash commands, config management, provider cloning.
  Reads/writes ~/.hermes/config.yaml + ~/.hermes/proxy-relay/config.json.
- Relay (FastAPI) — HTTP proxy with CooldownPool, semaphore, auth translation.
  Reads ~/.hermes/proxy-relay/config.json (or env vars).
- CooldownPool (inlined in relay.py) — thread-safe proxy pool with dynamic
  Retry-After cooling, fail-fast when all cooling, round-robin selection.

Clone flow (plugin never replaces originals):
1. User runs "/relay setup list" → plugin scans config.yaml for custom_providers
2. User runs "/relay setup clone 1" → plugin:
   a. Reads provider #1 (e.g., spacetimellm)
   b. Writes relay config to ~/.hermes/proxy-relay/config.json
   c. Writes NEW Hermes provider entry "spacetimellm-proxied" at :4002
   d. Original "spacetimellm" entry is untouched
3. User starts relay + switches Hermes to the -proxied provider
```

## Critical Conventions

1. **Never modify the original custom_providers entry.** Always create a new one with `-proxied` suffix.
2. **Never hardcode secrets.** API keys go in `~/.hermes/proxy-relay/config.json` (chmod 600).
3. **Never remove the proxy list filter.** The `_read_custom_providers()` function filters out entries at `:4002`, `proxy-relay`, and `*-proxied` to prevent proxy loops.
4. **Keep the CooldownPool thread-safe.** It's accessed from multiple asyncio workers. All state mutations go through `self._lock`.
5. **Systemd hardening is intentional.** `ProtectSystem=strict` and `ProtectHome=read-only` prevent the relay from writing outside its allowed paths. If adding new write paths, update `ReadWritePaths` in the systemd unit.
6. **Env vars always win over config.json.** When debugging config issues, check if the user has env vars set that override the file.

## The Clone Workflow (Plugin)

This is the primary way users configure the relay:

### /relay setup list

Scans `~/.hermes/config.yaml` for `custom_providers` entries. **Always filters out:**
- Entries named `proxy-relay` (the relay's own entry)
- Entries ending in `-proxied` (already cloned)
- Entries whose `base_url` contains `:4002` (already routing through the relay)

### /relay setup clone <N>

For the Nth eligible provider:

1. **Reads** the provider's `base_url`, `api_key`, and name from config.yaml
2. **Infers auth type** — `x-api-key` for OpenCode Zen providers, `bearer` for everything else
3. **Writes relay config** to `~/.hermes/proxy-relay/config.json` (chmod 600)
4. **Writes new provider entry** to config.yaml — name is `{original}-proxied`, routes through `localhost:4002`
5. **Returns** a summary of what was created. Original provider is **never touched**.

### Auth auto-inference

| Provider hint | Auth type |
|---|---|
| `opencode-zen`, `oc-zen`, `zen` in name | `x-api-key` |
| API key value is `public` | `x-api-key` |
| Everything else | `bearer` |

Override: `/relay setup clone 2 x-api-key`

## Plugin Commands

| Command | Description | Implementation |
|---------|-------------|----------------|
| `/relay setup` | Overview with cloneable provider list | `_cmd_setup()` in `plugin/__init__.py` |
| `/relay setup list` | List existing providers with details | `_cmd_setup('list')` |
| `/relay setup clone <N>` | Clone provider N with proxy routing | `_cmd_setup('clone <N>')` |
| `/relay setup clone <N> x-api-key` | Clone with auth type override | `_cmd_setup('clone <N> x-api-key')` |
| `/relay status` | Pool health, proxy counts, cooling | `_cmd_status()` |
| `/relay switch` | Change upstream or reload proxies | `_cmd_switch()` |
| `/relay help` | Full command reference | Default handler |

## Relay Config Sources (Precedence)

Config is loaded from three sources. Higher number wins:

1. **Defaults** — hardcoded in relay.py
2. **Config file** — `~/.hermes/proxy-relay/config.json` (auto-loaded, written by plugin)
3. **Environment variables** — always take precedence over the file
4. **`--config` CLI flag** — override config file path

Key env vars:

| Variable | Effect |
|----------|--------|
| `UPSTREAM_BASE` | Override upstream URL from config.json |
| `UPSTREAM_API_KEY` | Override API key from config.json |
| `UPSTREAM_AUTH_TYPE` | `bearer` or `x-api-key` |
| `PROXY_LIST` | Path to SOCKS5 proxy list (one per line) |
| `PROXY_LIST_ENV` | Comma-separated proxy URLs inline |
| `RELAY_PORT` | Listen port |
| `MAX_CONCURRENT_UPSTREAM` | Max parallel upstream connections |
| `MODEL_FILTER_PATTERN` | Regex for model allowlist (e.g., `-free$`) |
| `RELAY_CONFIG` | Path to custom config file |

## Common Pitfalls

1. **Proxy loop.** Setting `provider: oc-zen-socks` as the upstream for a provider that already routes through the relay creates a loop. The plugin filters these, but check manually if modifying configs outside the plugin.

2. **Relay dies on SSH logout without systemd.** If the user didn't install the systemd service or enable linger, the relay process terminates when their SSH session ends. Always offer the systemd unit during setup.

3. **All proxies cooling → 429 to user.** This is by design — the fail-fast pattern prevents wasted upstream calls. The user sees a 429 from the relay with a `Retry-After` header. They wait for cooldown.

4. **Config.json with wrong auth type.** If the upstream returns 401, the most likely cause is auth type mismatch. The relay uses `UPSTREAM_AUTH_TYPE` to choose between `Authorization: Bearer` and `x-api-key`. The plugin auto-infers this, but the override exists for a reason.

5. **YAML parse error in config.yaml.** If the plugin can't write to config.yaml (Hermes process has the file open, or YAML is malformed), the clone fails with an opaque error. Check `python3 -c "import yaml; yaml.safe_load(open('config.yaml'))"` first.

6. **`register()` writes config but changes don't take effect until restart.** Plugin `register()` runs after Hermes has already read config.yaml. The entry is written to disk but won't be active until the next gateway restart. The `/relay setup clone` output tells the user to restart.

## MCP Tools

The MCP server at `mcp/mcp_server.py` provides agent-callable tools.
**Must be configured in config.yaml's `mcp_servers` section to be active.**

Available tools:

| Tool | Description |
|------|-------------|
| `proxy_relay_status` | Full pool state: available/cooling proxies, request stats, model count |
| `proxy_relay_health` | Quick health check with latency |
| `proxy_relay_clear_cooldowns` | Reset all proxy cooldowns (not yet implemented — stub returns instructions) |

## Ports and Services

| Service | Port | Config source | Purpose |
|---------|------|---------------|---------|
| FastAPI relay | 4002 | `~/.hermes/proxy-relay/config.json` or `RELAY_PORT` env var | SOCKS5 proxy rotation relay |
| Hermes gateway | (varies) | `~/.hermes/config.yaml` | Hermes Agent gateway |
| Hermes SPDB memory | 3001 | (external) | SpacetimeDB for memory |

## Documentation Index

| Doc | Audience | Purpose |
|-----|----------|---------|
| `AGENTS.md` | AI agents | Full onboarding (this file) |
| `CLAUDE.md` | Claude Code | Quickstart signpost |
| `.cursorrules` | Cursor IDE | Quickstart rules |
| `README.md` | Humans + agents | Architecture, features, examples |
| `scripts/setup.sh` | Any user | One-command install |
| `examples/config.yaml` | Users | Example Hermes config with proxied entry |
| `plugin/__init__.py` | Developers | Plugin source code (well-commented) |
| `relay/relay.py` | Developers | Relay source code (self-contained, commented) |
