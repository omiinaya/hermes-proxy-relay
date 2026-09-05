# Hermes Agent Setup Guide — Proxy Relay

How to install and use the Hermes Proxy Relay **from another Hermes agent** —
the fastest path from "new machine" to "my Hermes agent tunnels through proxies".

This guide is written for an agent (or person) with zero prior knowledge of
this repo. It assumes you are already running Hermes with a `config.yaml`.

---

## 1. What this is

A small HTTP relay (`FastAPI`) that sits **between** your Hermes agent and an
upstream LLM provider:

```
Hermes agent ──:4002──▶ relay ──SOCKS5 pool──▶ upstream API
                          │
                     rotation, retry,
                     health-check, cooldown
```

You keep using normal `custom_providers`, but point Hermes at the relay
(`localhost:4002/v1`) instead of the upstream directly. The relay picks a
healthy proxy per request, rotates on failure, and keeps your real upstream
key out of the wire path you control.

**Use it when:** you need to route LLM traffic through proxies (datacenter,
tor, residential), your upstream rate-limits per-IP, or you want a stable
`/v1/models` + `/v1/chat/completions` surface with retries built in.

---

## 2. Install (one command)

```bash
git clone https://github.com/omiinaya/hermes-proxy-relay.git
cd hermes-proxy-relay
./scripts/setup.sh
```

`setup.sh` will:

1. Detect `python3`, `pip`, `git`, `hermes` CLI
2. Create a venv (`~/.hermes-proxy-relay/venv/`) and install dependencies
3. Symlink the Hermes plugin into `~/.hermes/plugins/` and enable it
4. Create `~/.hermes/proxy-relay/` + a `proxies.txt` placeholder
5. Scan `~/.hermes/config.yaml` for `custom_providers` and offer to clone one
   as a `-proxied` entry (routing through the relay)
6. Optionally install a systemd user service so the relay survives logout

You only need to provide: **the proxy list** and **upstream URL + API key**.

---

## 3. What the plugin gives you (inside Hermes)

Once enabled, use the `/relay` slash commands directly in a Hermes chat:

| Command | What it does |
|---------|--------------|
| `/relay setup list` | List existing `custom_providers` |
| `/relay setup clone <N>` | Clone provider N as `<name>-proxied` (routes via relay) |
| `/relay switch clientkey` | Rotate the relay's client API key |

Then switch your active model to the cloned provider:

```
/model <name>-proxied
```

The original provider entry is never touched.

---

## 4. Config — the two files that matter

| File | Purpose |
|------|---------|
| `~/.hermes/proxy-relay/config.json` | Relay config (upstream, auth, proxy file, profiles) |
| `~/.hermes/proxy-relay/proxies.txt` | One SOCKS5 proxy URL per line |

Minimal `config.json`:

```json
{
  "UPSTREAM_BASE": "http://localhost:4000/v1",
  "UPSTREAM_API_KEY": "sk-your-key",
  "UPSTREAM_AUTH_TYPE": "bearer",
  "RELAY_PORT": 4002,
  "PROXY_LIST": "~/.hermes/proxy-relay/proxies.txt",
  "CLIENT_API_KEY": "relay-key"       // required if you want auth on /v1/*
}
```

Env vars **override** file values at runtime (handy for secrets in a vault —
the relay is commonly launched via `bws run`/vault-exec so keys never sit in
the file).

---

## 5. Running it

```bash
# Foreground (config auto-loaded)
python relay/relay.py

# Boot into a named proxy profile
python relay/relay.py --profile tor

# Validate config without starting (exit 0/1)
python relay/relay.py --check
```

Verify:

```bash
curl http://localhost:4002/health
# → {"status":"ok","uptime_seconds":...,"version":...,"proxies":N}
```

---

## 6. Proxy profiles (optional but recommended)

Profiles let one relay switch between **independent proxy pools** at runtime —
`datacenter`, `tor`, `residential` — without a restart. Each profile has an
isolated cooldown/breaker pool.

```json
{
  "DEFAULT_PROFILE": "datacenter",
  "PROFILES_DIR": "~/.hermes/proxy-relay/profiles",
  "PROFILE_DEFS": [
    { "name": "datacenter", "proxy_file": "dc.txt" },
    { "name": "tor",        "proxy_file": "tor.txt" }
  ]
}
```

Hot-swap:

```bash
curl -X POST http://localhost:4002/admin/profile \
  -H "X-Admin-Key: $ADMIN_KEY" -d '{"profile": "tor"}'
```

If `PROFILE_DEFS` is absent, the relay runs a single legacy pool — nothing
else changes.

---

## 7. Admin endpoints (useful for automation)

With `ADMIN_API_KEY` set, all admin routes require `X-Admin-Key: <key>`:

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Status, uptime, version, proxy count (no auth) |
| `GET /admin/profile` | List profiles + active one (no URL/cred leakage) |
| `POST /admin/profile` | Hot-swap `{"profile": "tor"}` |
| `POST /admin/reload-config` | Re-read config.json live (no restart) |
| `POST /admin/reload-proxies` | Re-read the active profile's proxy sources |

Admin key is rate-limited (20 req/min/IP).

---

## 8. Common operations for an agent

- **"Is the relay up?"** → `curl -s localhost:4002/health`
- **"Which models can I use?"** → `curl -s localhost:4002/v1/models` (filtered
  by `MODEL_FILTER_PATTERN`, e.g. `-free|big-pickle`)
- **"A model 503'd — what now?"** → the relay auto-falls back to
  `FALLBACK_MODEL` and retries across other proxies; a 503 usually means the
  model's free quota is exhausted on all proxies, not a relay fault.
- **"Switch to tor for this task"** → `POST /admin/profile {"profile":"tor"}`
- **"Rotate the client key"** → `/relay switch clientkey` inside Hermes

---

## 9. Troubleshooting quick hits

| Symptom | Likely cause → fix |
|---------|-------------------|
| `HTTP 503 No proxy available` | All proxies cooling/dead for that model's quota → check `FALLBACK_MODEL`, add proxies |
| `401 unauthorized` from relay | Missing/wrong `CLIENT_API_KEY` on `/v1/*` or `X-Admin-Key` on admin |
| Relay starts but proxies never used | `PROXY_LIST` path wrong, or proxies marked dead → check `health_fail_count` / logs |
| Proxy pool "all dead" but network fine | Health target blocked by proxy network → set `PROXY_HEALTH_CHECK_URL` to a domain the proxies allow |
| Config changes ignored | Relay caches config → `POST /admin/reload-config` or restart |

---

## 10. Uninstall

```bash
# Stop + disable service
systemctl --user stop hermes-proxy-relay && systemctl --user disable hermes-proxy-relay

# Remove plugin symlink + config + venv
rm -rf ~/.hermes/plugins/proxy-relay ~/.hermes/proxy-relay ~/.hermes-proxy-relay

# Remove any -proxied entries from ~/.hermes/config.yaml
```

---

**Full details:** see `README.md` (features, env table, architecture) and
`AGENTS.md` (workspace layout, task → file mapping).