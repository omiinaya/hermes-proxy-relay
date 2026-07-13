# Hermes Proxy Relay

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
- **Dynamic 429 cooldown** — Proxy cooled for the exact `Retry-After` duration.
  Skipped during cooldown. Zero upstream calls when all cooling.
- **Concurrency semaphore** — Caps parallel upstream connections (default 10)
- **Streaming** — SSE streaming through the relay (client lifecycle outside generator)
- **Auth translation** — Strips Hermes auth headers, rewrites with upstream key.
  Supports `bearer` and `x-api-key` modes. Auto-inferred for OpenCode Zen providers.
- **Config file or env vars** — Relay auto-loads `~/.hermes/proxy-relay/config.json`
  (written by the plugin). Env vars take precedence.
- **Model filtering** — Regex pattern to filter which upstream models are exposed

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
│  │   • Semaphore (10) │  │
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

| Provider name hint | Auth type |
|---|---|
| `opencode-zen`, `oc-zen`, `zen` | `x-api-key` |
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
| `/relay switch` | Change upstream or reload proxies |
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
  "MAX_CONCURRENT_UPSTREAM": 10,
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
| `MAX_CONCURRENT_UPSTREAM` | `10` | Max simultaneous upstream connections |
| `MODEL_FILTER_PATTERN` | `.*` | Regex for allowed model names (e.g. `-free$`) |
| `LOG_LEVEL` | `INFO` | Logging level |

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

## License

MIT

---

## Documentation for AI Agents

| Doc | Purpose |
|-----|---------|
| [AGENTS.md](./AGENTS.md) | Full AI agent onboarding — architecture, file mapping, conventions, pitfalls |
| [CLAUDE.md](./CLAUDE.md) | Claude Code quickstart (signpost to AGENTS.md) |
| [.cursorrules](./.cursorrules) | Cursor IDE rules and conventions |
