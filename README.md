# Hermes Proxy Relay

A lightweight SOCKS5 proxy rotation relay for [Hermes Agent](https://hermes-agent.nousresearch.com). 
Routes LLM API calls through a pool of user-provided SOCKS5 proxies with automatic 
rate-limit cooldown, concurrency safety, and zero amplification bombs.

```
Hermes Agent  ──►  Relay (localhost:4002)  ──►  SOCKS5 pool  ──►  Upstream API
```

## Features

- **Proxy rotation** — Round-robin through N SOCKS5 proxies
- **Dynamic 429 cooldown** — Cooled for the exact `Retry-After` duration (not a fixed timeout)
- **Fail-fast on exhausted pool** — All proxies cooling? Return 429 immediately, zero upstream calls
- **Concurrency semaphore** — Caps parallel upstream connections
- **Streaming** — SSE streaming through the relay (client lifecycle outside generator)
- **Auth translation** — Strips or rewrites auth headers as needed
- **Model filtering** — Configurable allowlist/blocklist of model patterns
- **Hermes plugin** — Auto-configures `custom_providers`, slash commands for setup/status
- **MCP server** — Tool-level model listing, health checks, pool inspection

## Quick Start

```bash
# 1. Install the plugin
hermes plugins install hermes-proxy-relay

# 2. Setup
hermes chat -q "/relay setup"

# 3. Done — provider configured, relay running
hermes chat -q "hello world"
```

Or manually:

```bash
# 1. Create your proxy list file
echo 'socks5://user:pass@proxy1:1080' > ~/.hermes/proxy-relay/proxies.txt
echo 'socks5://user:pass@proxy2:1080' >> ~/.hermes/proxy-relay/proxies.txt

# 2. Start the relay
cd relay && pip install -r ../requirements.txt
UPSTREAM_BASE=https://api.openai.com/v1 \
UPSTREAM_API_KEY=sk-... \
PROXY_LIST=~/.hermes/proxy-relay/proxies.txt \
  python relay.py

# 3. Configure Hermes
```

## Architecture

```
┌─────────────────────┐
│   Hermes Agent      │
│  model.default: auto│
│  provider: proxy-relay│
└─────────┬───────────┘
          │ GET/POST /v1/...
          ▼
┌─────────────────────┐
│   FastAPI Relay     │  port 4002
│   relay/relay.py    │
├─────────────────────┤
│  ┌───────────────┐  │
│  │ CooldownPool  │  │  Proxy lifecycle:
│  │ • N proxies   │  │  • 429 → cooldown for Retry-After
│  │ • Round-robin│  │  • All cooling → immediate 429
│  │ • Health mgmt │  │  • Cooling → periodic recovery check
│  └───────────────┘  │
│  ┌───────────────┐  │
│  │ Semaphore (10) │  │  Cap concurrent upstream hits
│  └───────────────┘  │
└─────────┬───────────┘
          │ SOCKS5 via httpx[socks]
          ▼
┌─────────────────────┐
│   SOCKS5 Pool       │
│   (user-provided)   │
│   decodo / IPVanish │
│   / residential /...│
└─────────────────────┘
```

## Hermes Plugin

The companion plugin at `plugin/` provides:

| Slash Command | Description |
|---------------|-------------|
| `/relay setup` | Guided configuration walkthrough |
| `/relay status` | Pool health, proxy counts, cooling details |
| `/relay switch` | Change upstream or proxy list |
| `/relay help` | Usage reference |

MCP tools:

| Tool | Description |
|------|-------------|
| `proxy_relay_status` | Pool state, model list, cooling counts |
| `proxy_relay_pause` | Pause relay (stop processing) |
| `proxy_relay_resume` | Resume relay |
| `proxy_relay_reload_proxies` | Reload proxy list without restart |

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UPSTREAM_BASE` | Yes | — | Upstream API base URL (e.g. `https://api.opencode-zen.com/v1`) |
| `UPSTREAM_API_KEY` | Yes | — | API key for upstream |
| `UPSTREAM_AUTH_TYPE` | No | `bearer` | Auth header type: `bearer` or `x-api-key` |
| `PROXY_LIST` | See below | — | Path to proxy list file (one per line, format: `protocol://user:pass@host:port`) |
| `PROXY_LIST_ENV` | See below | — | Comma-separated proxy URLs inline (alternative to file) |
| `RELAY_PORT` | No | `4002` | Port to listen on |
| `MAX_CONCURRENT_UPSTREAM` | No | `10` | Max simultaneous upstream connections |
| `MODEL_FILTER_PATTERN` | No | `.*` | Regex pattern for allowed model names (`-free$` to filter free models only) |
| `LOG_LEVEL` | No | `INFO` | Logging level |

### Proxy List Format

One proxy per line:
```
socks5://user:pass@192.168.1.100:1080
socks5://user:pass@192.168.1.101:1080
http://user:pass@proxy.example.com:3128
```

## Project Structure

```
hermes-proxy-relay/
├── relay/               # The FastAPI relay application
│   ├── __init__.py
│   ├── relay.py         # Main relay server
│   └── cooldown_pool.py # Proxy pool with dynamic cooldown
├── plugin/              # Hermes plugin
│   ├── __init__.py      # register() — slash commands, auto-config
│   └── plugin.yaml      # Plugin metadata
├── mcp/                 # MCP server (optional companion)
│   ├── __init__.py
│   └── mcp_server.py    # MCP tools for relay management
├── scripts/
│   └── setup.sh         # One-command setup
├── examples/
│   └── config.yaml      # Example Hermes config.yaml
├── requirements.txt     # Python dependencies
├── LICENSE
└── README.md
```

## Development

```bash
# Clone
git clone https://github.com/omiinaya/hermes-proxy-relay.git
cd hermes-proxy-relay

# Install deps
pip install -r requirements.txt

# Run relay directly
cd relay
PROXY_LIST=../examples/proxies.txt \
UPSTREAM_BASE=https://api.openai.com/v1 \
UPSTREAM_API_KEY=sk-test \
  python relay.py

# Test health
curl -s http://localhost:4002/health

# Test chat
curl -s -X POST http://localhost:4002/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":false}'
```

## License

MIT
