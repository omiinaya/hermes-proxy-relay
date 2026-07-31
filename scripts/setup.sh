#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────
# Hermes Proxy Relay — Setup Script
#
# One-command install. Reads your existing Hermes config, asks which
# provider to proxy through, and writes everything needed.
#
# Usage:
#   ./setup.sh                          Full install (recommended)
#   ./setup.sh --relay-only             Install relay only (no plugin)
#   ./setup.sh --help                   This message
#
# What it does:
#   1. Checks prerequisites (python3, pip, git, hermes)
#   2. Creates Python venv + installs deps
#   3. Symlinks the Hermes plugin + enables it
#   4. Creates ~/.hermes/proxy-relay/ directory + proxy list placeholder
#   5. SCANS your Hermes config for existing providers → asks which to clone
#      → writes relay config.json + adds "-proxied" Hermes entry
#      (never touches the original provider entry)
#   6. Optionally installs systemd --user service (survives logout)
#   7. Verifies everything
#   8. Prints next steps
#
# Idempotent — safe to re-run.
# ────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
RELAY_DIR="${HOME}/.hermes-proxy-relay"
RELAY_CONFIG_DIR="${HERMES_HOME}/proxy-relay"
VENV_DIR="${RELAY_DIR}/venv"
PLUGIN_DIR="${HERMES_HOME}/plugins/proxy-relay"
SERVICE_DIR="${HOME}/.config/systemd/user"
SYSTEMD_UNIT="${SERVICE_DIR}/hermes-proxy-relay.service"
RELAY_PORT="${RELAY_PORT:-4002}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'
ok()  { echo -e " ${GREEN}✓${NC} $1"; }
info(){ echo -e " ${YELLOW}ℹ${NC} $1"; }
err() { echo -e " ${RED}✗${NC} $1"; }
head(){ echo -e "\n${BOLD}── $1 ──${NC}\n"; }

RELAY_ONLY=false
for arg in "$@"; do
  case "$arg" in --relay-only) RELAY_ONLY=true ;; --help|-h) sed -n '3,23p' "$0" | sed 's/^# //'; exit 0 ;; esac
done

echo ""
echo "  ╭──────────────────────────────────────────────╮"
echo "  │         Hermes Proxy Relay — Setup           │"
echo "  ╰──────────────────────────────────────────────╯"

# ══════════════════════════════════════════════════════════════════
#  1. Prerequisites
# ══════════════════════════════════════════════════════════════════
head "1/7 — Checking prerequisites"

MISSING=false
if command -v python3 &>/dev/null; then
  PYTHON=$(command -v python3); ok "Python 3: $($PYTHON --version 2>&1)"
else
  err "python3 not found"; MISSING=true
fi

if python3 -m pip --version &>/dev/null 2>&1; then
  ok "pip: $(python3 -m pip --version 2>&1 | head -1)"
else
  err "pip not found"; MISSING=true
fi

if command -v git &>/dev/null; then
  ok "git: $(git --version 2>&1 | head -1)"
else
  err "git not found"; MISSING=true
fi

if command -v hermes &>/dev/null; then
  HERMES=$(command -v hermes); ok "Hermes: $($HERMES --version 2>/dev/null || echo 'found')"
elif $RELAY_ONLY; then
  info "Hermes CLI not found (--relay-only)"
else
  err "Hermes CLI not found. Install: curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash"
  info "Or re-run with --relay-only"; MISSING=true
fi

if $MISSING; then echo ""; err "Fix missing prerequisites and re-run."; exit 1; fi

# ══════════════════════════════════════════════════════════════════
#  2. Python venv + install deps
# ══════════════════════════════════════════════════════════════════
head "2/7 — Creating Python virtual environment"

mkdir -p "$RELAY_DIR"
if [ -d "$VENV_DIR" ] && [ -f "$VENV_DIR/bin/python3" ]; then
  ok "Virtual environment exists at $VENV_DIR"
else
  info "Creating virtual environment..."
  python3 -m venv "$VENV_DIR"
  ok "Virtual environment created"
fi

if [ -f "$REPO_ROOT/requirements.txt" ]; then
  info "Installing dependencies..."
  "$VENV_DIR/bin/pip" install -q -r "$REPO_ROOT/requirements.txt"
  ok "Dependencies installed"
fi

chmod +x "$REPO_ROOT/relay/relay.py" 2>/dev/null || true

# ══════════════════════════════════════════════════════════════════
#  3. Hermes Plugin
# ══════════════════════════════════════════════════════════════════
head "3/7 — Installing Hermes plugin"

if $RELAY_ONLY; then
  info "Skipping Hermes plugin (--relay-only)"
else
  if [ -L "$PLUGIN_DIR" ] || [ -d "$PLUGIN_DIR" ]; then
    ok "Plugin symlink exists at $PLUGIN_DIR"
  else
    mkdir -p "$(dirname "$PLUGIN_DIR")"
    ln -sf "$REPO_ROOT/plugin" "$PLUGIN_DIR"
    ok "Plugin symlinked"
  fi

  if command -v hermes &>/dev/null; then
    if hermes plugins list 2>/dev/null | grep -q 'proxy-relay.*enabled'; then
      ok "Plugin already enabled"
    else
      info "Enabling plugin..."
      hermes plugins enable proxy-relay 2>/dev/null && ok "Plugin enabled" || \
        info "Run manually: hermes plugins enable proxy-relay"
    fi
  fi
fi

# ══════════════════════════════════════════════════════════════════
#  4. Config directory + proxy list
# ══════════════════════════════════════════════════════════════════
head "4/7 — Creating config directory"

mkdir -p "$RELAY_CONFIG_DIR"
chmod 700 "$RELAY_CONFIG_DIR"
ok "Config directory: $RELAY_CONFIG_DIR"

PROXY_LIST_PATH="${RELAY_CONFIG_DIR}/proxies.txt"
if [ -f "$PROXY_LIST_PATH" ]; then
  COUNT=$(grep -v '^#' "$PROXY_LIST_PATH" 2>/dev/null | grep -v '^$' | wc -l)
  ok "Proxy list: $PROXY_LIST_PATH ($COUNT proxies)"
else
  cat > "$PROXY_LIST_PATH" << PROXYEOF
# Hermes Proxy Relay — Proxy List
# One SOCKS5 proxy per line. Lines starting with # are ignored.
# Format: protocol://username:password@host:port
#
# Examples:
# socks5://user:pass@192.168.1.100:1080
# socks5://user:pass@proxy.example.com:1080
# http://user:pass@residential-proxy:3128
#
# Get SOCKS5 proxies from: IPVanish, Decodo, Oxylabs, BrightData, etc.
PROXYEOF
  chmod 600 "$PROXY_LIST_PATH"
  ok "Placeholder proxy list: $PROXY_LIST_PATH"
  info "✎ Edit this file with your real SOCKS5 proxy URLs"
fi

# ══════════════════════════════════════════════════════════════════
#  5. Scan config.yaml → pick provider → write relay + Hermes configs
# ══════════════════════════════════════════════════════════════════
head "5/7 — Configuring relay (scanning Hermes providers)"

# Check if config.json already exists (from a prior run)
if [ -f "${RELAY_CONFIG_DIR}/config.json" ] && [ -z "${REWRITE_CONFIG:-}" ]; then
  CURRENT_UPSTREAM=$("$VENV_DIR/bin/python3" -c "
import json
d = json.load(open('${RELAY_CONFIG_DIR}/config.json'))
print(d.get('UPSTREAM_BASE', '(unknown)'))
" 2>/dev/null || echo "(unknown)")
  echo "  Existing config: upstream = ${CURRENT_UPSTREAM}"
  echo -n "  Reconfigure? [y/N]: "
  read -r RECONFIGURE
  if [[ ! "$RECONFIGURE" =~ ^[Yy]$ ]]; then
    CONFIG_DONE=true
  fi
fi

if [ -z "${CONFIG_DONE:-}" ]; then
  # Use Python to scan config.yaml and present choices
  PY_OUTPUT=$("$VENV_DIR/bin/python3" << 'PYEOF' 2>&1
import json, os, sys

hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
config_path = os.path.join(hermes_home, "config.yaml")

if not os.path.exists(config_path):
    print("NO_CONFIG")
    sys.exit(0)

try:
    import yaml
except ImportError:
    print("NO_YAML")
    sys.exit(0)

with open(config_path) as f:
    cfg = yaml.safe_load(f) or {}

providers = cfg.get("custom_providers", [])
eligible = []
for p in providers:
    if not isinstance(p, dict) or not p.get("name"):
        continue
    name = p["name"]
    url = p.get("base_url", "")
    if name == "proxy-relay" or name.endswith("-proxied"):
        continue
    if ":4002" in url:
        continue
    eligible.append(p)

if not eligible:
    print("NONE")
    sys.exit(0)

for i, p in enumerate(eligible, 1):
    model = p.get("model", "")
    key = p.get("api_key", "")
    key_display = f"{key[:6]}...{key[-4:]}" if len(key) > 8 else ("(none)" if not key else "****")
    print(f"PROVIDER|{i}|{p['name']}|{p.get('base_url','')}|{key_display}|{model}")

print("COUNT|" + str(len(eligible)))
json.dump({"eligible": eligible}, open("/tmp/_relay_providers.json", "w"))
PYEOF
)

  if [ "$PY_OUTPUT" = "NO_CONFIG" ]; then
    echo "  No ~/.hermes/config.yaml found."
    echo "  Will ask for upstream details manually."
    MANUAL_CONFIG=true
  elif [ "$PY_OUTPUT" = "NO_YAML" ]; then
    echo "  PyYAML not available — will ask for upstream details manually."
    MANUAL_CONFIG=true
  elif [ "$PY_OUTPUT" = "NONE" ]; then
    echo "  No eligible providers found in your Hermes config."
    echo "  Will ask for upstream details manually."
    MANUAL_CONFIG=true
  else
    # Parse the provider listing
    echo "  Found eligible providers from your Hermes config:"
    echo ""
    echo "$PY_OUTPUT" | while IFS='|' read -r tag idx name url key model; do
      [ "$tag" = "PROVIDER" ] && printf "     %s. %s\n        URL: %s\n        Key: %s\n        Model: %s\n" "$idx" "$name" "$url" "$key" "$model"
    done

    COUNT=$(echo "$PY_OUTPUT" | grep "^COUNT|" | cut -d'|' -f2)

    echo ""
    echo -n "  Clone provider [1-$COUNT]: "
    read -r CHOICE

    if [ -n "$CHOICE" ] && [ "$CHOICE" -ge 1 ] 2>/dev/null && [ "$CHOICE" -le "$COUNT" ] 2>/dev/null; then
      # Read the selected provider from temp file
      PROVIDER_JSON=$("$VENV_DIR/bin/python3" -c "
import json
data = json.load(open('/tmp/_relay_providers.json'))
p = data['eligible'][$((CHOICE - 1))]
print(json.dumps(p))
" 2>/dev/null || echo "")

      if [ -n "$PROVIDER_JSON" ]; then
        # Extract values
        ORIG_NAME=$(echo "$PROVIDER_JSON" | "$VENV_DIR/bin/python3" -c "import sys,json; print(json.load(sys.stdin)['name'])")
        ORIG_URL=$(echo "$PROVIDER_JSON" | "$VENV_DIR/bin/python3" -c "import sys,json; print(json.load(sys.stdin).get('base_url',''))")
        ORIG_KEY=$(echo "$PROVIDER_JSON" | "$VENV_DIR/bin/python3" -c "import sys,json; print(json.load(sys.stdin).get('api_key',''))")

        # Infer auth type
        AUTH_TYPE="bearer"
        LC_NAME=$(echo "$ORIG_NAME" | tr '[:upper:]' '[:lower:]')
        if echo "$LC_NAME" | grep -qE "opencode|oc-zen|zen" || [ "$ORIG_KEY" = "public" ]; then
          AUTH_TYPE="x-api-key"
        fi

        echo ""
        echo -n "  Auth type (bearer/x-api-key) [${AUTH_TYPE}]: "
        read -r AUTH_OVERRIDE
        [ -n "$AUTH_OVERRIDE" ] && AUTH_TYPE="$AUTH_OVERRIDE"

        # Write relay config.json
        cat > "${RELAY_CONFIG_DIR}/config.json" << CONFIGEOF
{
  "UPSTREAM_BASE": "${ORIG_URL}",
  "UPSTREAM_API_KEY": "${ORIG_KEY}",
  "UPSTREAM_AUTH_TYPE": "${AUTH_TYPE}",
  "RELAY_PORT": ${RELAY_PORT},
  "MAX_CONCURRENT_UPSTREAM": 10,
  "MODEL_FILTER_PATTERN": ".*",
  "LOG_LEVEL": "INFO"
}
CONFIGEOF
        chmod 600 "${RELAY_CONFIG_DIR}/config.json"

        # Write Hermes config.yaml entry via Python (safe yaml.dump)
        "$VENV_DIR/bin/python3" << PYHERMES
import json, os, yaml

hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
config_path = os.path.join(hermes_home, "config.yaml")

with open(config_path) as f:
    cfg = yaml.safe_load(f) or {}

orig_name = "${ORIG_NAME}"
new_name = f"{orig_name}-proxied"

providers = cfg.setdefault("custom_providers", [])

# Check if already exists
for p in providers:
    if isinstance(p, dict) and p.get("name") == new_name:
        print(f"EXISTS|{new_name}")
        break
else:
    providers.append({
        "name": new_name,
        "base_url": "http://localhost:${RELAY_PORT}/v1",
        "api_key": "relay-key",
        "model": "auto",
    })
    with open(config_path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
    print(f"WRITTEN|{new_name}")

# Print original provider status
orig_exists = any(isinstance(p, dict) and p.get("name") == orig_name for p in providers)
print(f"ORIGINAL|{orig_name}|{orig_exists}")
PYHERMES

        HERMES_RESULT=$("$VENV_DIR/bin/python3" -c "
import json, os, yaml
hermes_home = os.environ.get('HERMES_HOME', os.path.expanduser('~/.hermes'))
config_path = os.path.join(hermes_home, 'config.yaml')
with open(config_path) as f:
    cfg = yaml.safe_load(f) or {}
result = {'written': False, 'original_exists': False}
for p in cfg.get('custom_providers', []):
    if isinstance(p, dict) and p.get('name') == '${ORIG_NAME}-proxied':
        result['written'] = True
    if isinstance(p, dict) and p.get('name') == '${ORIG_NAME}':
        result['original_exists'] = True
print(json.dumps(result))
" 2>/dev/null || echo '{"written":false,"original_exists":false}')

        WAS_WRITTEN=$(echo "$HERMES_RESULT" | "$VENV_DIR/bin/python3" -c "import sys,json; print(json.load(sys.stdin).get('written', False))")
        ORIG_EXISTS=$(echo "$HERMES_RESULT" | "$VENV_DIR/bin/python3" -c "import sys,json; print(json.load(sys.stdin).get('original_exists', False))")

        echo ""
        if [ "$WAS_WRITTEN" = "True" ]; then
          ok "Created Hermes provider: ${ORIG_NAME} → ${ORIG_NAME}-proxied"
        else
          ok "Hermes provider ${ORIG_NAME}-proxied already exists"
        fi
        [ "$ORIG_EXISTS" = "True" ] && ok "Original provider '${ORIG_NAME}' untouched"
        ok "Relay config written: ${RELAY_CONFIG_DIR}/config.json"
        ok "Auth type: ${AUTH_TYPE}"
      else
        echo "  Error reading provider. Falling back to manual."
        MANUAL_CONFIG=true
      fi
    else
      echo "  Invalid choice. Falling back to manual."
      MANUAL_CONFIG=true
    fi
    rm -f /tmp/_relay_providers.json
  fi

  # Manual fallback — no eligible providers, or user chose invalid
  if [ "${MANUAL_CONFIG:-false}" = "true" ]; then
    echo ""
    echo "  Enter upstream details manually:"
    echo ""
    echo -n "  Upstream URL [http://localhost:4000/v1]: "
    read -r MANUAL_URL; MANUAL_URL="${MANUAL_URL:-http://localhost:4000/v1}"
    echo -n "  API key [sk-test]: "
    read -r MANUAL_KEY; MANUAL_KEY="${MANUAL_KEY:-sk-test}"
    echo "  Auth type: 1) bearer  2) x-api-key"
    echo -n "  Choice [1]: "
    read -r AUTH_CHOICE
    [ "$AUTH_CHOICE" = "2" ] && MANUAL_AUTH="x-api-key" || MANUAL_AUTH="bearer"

    cat > "${RELAY_CONFIG_DIR}/config.json" << CONFIGEOF
{
  "UPSTREAM_BASE": "${MANUAL_URL}",
  "UPSTREAM_API_KEY": "${MANUAL_KEY}",
  "UPSTREAM_AUTH_TYPE": "${MANUAL_AUTH}",
  "RELAY_PORT": ${RELAY_PORT},
  "MAX_CONCURRENT_UPSTREAM": 10,
  "MODEL_FILTER_PATTERN": ".*",
  "LOG_LEVEL": "INFO"
}
CONFIGEOF
    chmod 600 "${RELAY_CONFIG_DIR}/config.json"
    ok "Relay config written: ${RELAY_CONFIG_DIR}/config.json"

    # Manual mode: can't auto-write Hermes entry without knowing provider name
    info "No Hermes provider entry auto-created (manual upstream)."
    info "After setup, run this in Hermes: /relay setup list → /relay setup clone <N>"
  fi
fi

# ══════════════════════════════════════════════════════════════════
#  6. Systemd user service
# ══════════════════════════════════════════════════════════════════
head "6/7 — Systemd service (optional)"

HAS_SYSTEMD=false
if command -v systemctl &>/dev/null && systemctl --user &>/dev/null 2>&1; then
  HAS_SYSTEMD=true
fi

DO_SVC=false
if $HAS_SYSTEMD && [ ! -f "$SYSTEMD_UNIT" ]; then
  echo -n "  Install systemd --user service? [Y/n]: "
  read -r SVC_CHOICE
  [[ ! "$SVC_CHOICE" =~ ^[Nn]$ ]] && DO_SVC=true
elif $HAS_SYSTEMD && [ -f "$SYSTEMD_UNIT" ]; then
  ok "Systemd unit exists"
  echo -n "  Reinstall? [y/N]: "
  read -r REINSTALL
  [[ "$REINSTALL" =~ ^[Yy]$ ]] && DO_SVC=true
fi

if $DO_SVC; then
  mkdir -p "$SERVICE_DIR"
  cat > "$SYSTEMD_UNIT" << SERVICEEOF
[Unit]
Description=Hermes Proxy Relay — SOCKS5 proxy rotation for LLM APIs
Documentation=https://github.com/omiinaya/hermes-proxy-relay
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStartPre=${VENV_DIR}/bin/python ${REPO_ROOT}/relay/relay.py --check
ExecStart=${VENV_DIR}/bin/python ${REPO_ROOT}/relay/relay.py
Restart=on-failure
RestartSec=5
RestartSteps=3
RestartMaxDelaySec=30
Environment=PROXY_LIST=${RELAY_CONFIG_DIR}/proxies.txt
Environment=RELAY_CONFIG=${RELAY_CONFIG_DIR}/config.json
WorkingDirectory=${REPO_ROOT}

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=${RELAY_CONFIG_DIR} ${REPO_ROOT}/relay

[Install]
WantedBy=default.target
SERVICEEOF

  systemctl --user daemon-reload
  ok "Systemd unit written"

  echo -n "  Start and enable now? [Y/n]: "
  read -r START_CHOICE
  if [[ ! "$START_CHOICE" =~ ^[Nn]$ ]]; then
    systemctl --user enable --now hermes-proxy-relay.service 2>/dev/null || true
    sleep 2
    if systemctl --user is-active hermes-proxy-relay.service &>/dev/null; then
      ok "Service is running"
    else
      info "Service installed but not running. Check: systemctl --user status hermes-proxy-relay"
    fi
  fi

  if loginctl show-user "$USER" 2>/dev/null | grep -q "Linger=no"; then
    echo ""
    info "Linger disabled — service stops on logout."
    echo -n "  Enable linger? [Y/n]: "
    read -r LINGER_CHOICE
    if [[ ! "$LINGER_CHOICE" =~ ^[Nn]$ ]]; then
      sudo loginctl enable-linger "$USER" 2>/dev/null && ok "Linger enabled" || \
        err "Run: sudo loginctl enable-linger $USER"
    fi
  fi
elif ! $HAS_SYSTEMD; then
  info "systemd not available. Run relay as foreground:"
  info "  PROXY_LIST=${RELAY_CONFIG_DIR}/proxies.txt ${VENV_DIR}/bin/python ${REPO_ROOT}/relay/relay.py"
fi

# ══════════════════════════════════════════════════════════════════
#  7. Verify + Next Steps
# ══════════════════════════════════════════════════════════════════
head "7/7 — Verification"

"$VENV_DIR/bin/python3" -c "import fastapi, httpx, uvicorn" &>/dev/null && \
  ok "Python venv: all dependencies available" || \
  err "Python deps missing — re-run: $VENV_DIR/bin/pip install -r $REPO_ROOT/requirements.txt"

[ -f "$REPO_ROOT/relay/relay.py" ] && \
  ok "Relay script: $REPO_ROOT/relay/relay.py ($(wc -l < "$REPO_ROOT/relay/relay.py") lines)" || \
  err "Relay script missing"

! $RELAY_ONLY && [ -L "$PLUGIN_DIR" ] && ok "Hermes plugin installed"

[ -f "${RELAY_CONFIG_DIR}/config.json" ] && \
  ok "Relay config: ${RELAY_CONFIG_DIR}/config.json" || \
  info "No config.json yet — run /relay setup clone after setup"

$HAS_SYSTEMD && [ -f "$SYSTEMD_UNIT" ] && systemctl --user is-active hermes-proxy-relay.service &>/dev/null && \
  ok "Systemd service: active"

# Health check if relay is running
if curl -sf "http://localhost:${RELAY_PORT}/health" &>/dev/null 2>&1; then
  HEALTH_STATUS=$(curl -sf "http://localhost:${RELAY_PORT}/health" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"proxy pool: {d['pool_stats']['available']}/{d['pool_stats']['total']} available, upstream: {d['upstream_base']}\")" 2>/dev/null || echo "relay responding")
  ok "Relay health check passed: ${HEALTH_STATUS}"
fi

echo ""
echo "  ${BOLD}═══════════════════════════════════════════${NC}"
echo "  ${BOLD}  ✅ Setup complete!${NC}"
echo "  ${BOLD}═══════════════════════════════════════════${NC}"
echo ""

if $RELAY_ONLY; then
  cat << NEXT
  ${BOLD}1.${NC} Edit your proxy list: ${BOLD}${RELAY_CONFIG_DIR}/proxies.txt${NC}
  ${BOLD}2.${NC} Start the relay: ${BOLD}${VENV_DIR}/bin/python ${REPO_ROOT}/relay/relay.py${NC}
  ${BOLD}3.${NC} Verify: ${BOLD}curl -s http://localhost:${RELAY_PORT}/health${NC}
  ${BOLD}4.${NC} Add Hermes provider manually (see README.md)

NEXT
else
  cat << NEXT
  ${BOLD}1.${NC} Edit your proxy list with real SOCKS5 URLs:
     ${BOLD}✎ ${RELAY_CONFIG_DIR}/proxies.txt${NC}

  ${BOLD}2.${NC} Restart the Hermes gateway (from outside):
     ${BOLD}hermes gateway restart${NC}

  ${BOLD}3.${NC} Start the relay:
     ${BOLD}PROXY_LIST=${RELAY_CONFIG_DIR}/proxies.txt ${VENV_DIR}/bin/python ${REPO_ROOT}/relay/relay.py${NC}

  $(if $HAS_SYSTEMD && [ -f "$SYSTEMD_UNIT" ]; then echo "     Or via systemd: ${BOLD}systemctl --user start hermes-proxy-relay${NC}"; fi)

  ${BOLD}4.${NC} Verify:
     ${BOLD}curl -s http://localhost:${RELAY_PORT}/health${NC}

  ${BOLD}5.${NC} In Hermes, switch to the proxied provider:
     ${BOLD}/model $(echo "${ORIG_NAME:-provider}" | sed 's/[^a-zA-Z0-9_-]//g')-proxied${NC}

  ${BOLD}Plugin commands (post-setup):${NC}
     /relay setup list       — list cloneable providers
     /relay setup clone <N>  — clone a different provider later
     /relay status           — check health and pool state

NEXT
fi
