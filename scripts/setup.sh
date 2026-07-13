#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────
# Hermes Proxy Relay — Setup Script
#
# Usage:
#   ./setup.sh                          Full install (relay + Hermes plugin)
#   ./setup.sh --relay-only             Install relay only (no Hermes plugin)
#   ./setup.sh --help                   This message
#
# What it does:
#   1. Checks prerequisites (python3, pip, hermes CLI)
#   2. Creates Python venv + installs deps
#   3. Symlinks the Hermes plugin + enables it (unless --relay-only)
#   4. Creates ~/.hermes/proxy-relay/ directory structure
#   5. Optionally writes relay config.json (asks for upstream URL + key)
#   6. Creates a placeholder proxy list file
#   7. Optionally installs a systemd --user service so relay survives logout
#   8. Prints next steps
#
# This script is idempotent — safe to re-run.
# ────────────────────────────────────────────────────────────────────
set -euo pipefail

# ══════════════════════════════════════════════════════════════════
#  Config
# ══════════════════════════════════════════════════════════════════

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

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'
ok()  { echo -e " ${GREEN}✓${NC} $1"; }
info(){ echo -e " ${YELLOW}ℹ${NC} $1"; }
err() { echo -e " ${RED}✗${NC} $1"; }
head(){ echo -e "\n${BOLD}── $1 ──${NC}\n"; }

# Parse args
RELAY_ONLY=false
for arg in "$@"; do
    case "$arg" in
        --relay-only) RELAY_ONLY=true ;;
        --help|-h)
            sed -n '3,16p' "$0" | sed 's/^# //'
            exit 0
            ;;
    esac
done

echo ""
echo "  ╭──────────────────────────────────────────────╮"
echo "  │         Hermes Proxy Relay — Setup           │"
echo "  ╰──────────────────────────────────────────────╯"

# ══════════════════════════════════════════════════════════════════
#  1. Prerequisites
# ══════════════════════════════════════════════════════════════════
head "1/8 — Checking prerequisites"

MISSING=false

# Python 3
if command -v python3 &>/dev/null; then
    PYTHON=$(command -v python3)
    ok "Python 3: $($PYTHON --version 2>&1)"
else
    err "python3 not found. Install it: sudo apt install python3 python3-pip python3-venv"
    MISSING=true
fi

# pip
if $MISSING || python3 -m pip --version &>/dev/null; then
    ok "pip: $(python3 -m pip --version 2>&1 | head -1)"
else
    err "pip not found. Install it: sudo apt install python3-pip"
    MISSING=true
fi

# Git
if command -v git &>/dev/null; then
    ok "git: $(git --version 2>&1 | head -1)"
else
    err "git not found. Install it: sudo apt install git"
    MISSING=true
fi

# Hermes CLI
HERMES_FOUND=false
if command -v hermes &>/dev/null; then
    HERMES=$(command -v hermes)
    HERMES_VER=$($HERMES --version 2>/dev/null || echo "yes")
    ok "Hermes: $HERMES_VER"
    HERMES_FOUND=true
else
    if $RELAY_ONLY; then
        info "Hermes CLI not found (--relay-only, skipping)"
        HERMES_FOUND=false
    else
        err "Hermes CLI not found. Install: curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash"
        err "Or re-run with --relay-only to skip the Hermes plugin"
        MISSING=true
    fi
fi

if $MISSING; then
    echo ""
    err "Fix the missing prerequisites and re-run."
    exit 1
fi

# ══════════════════════════════════════════════════════════════════
#  2. Python venv + install deps
# ══════════════════════════════════════════════════════════════════
head "2/8 — Creating Python virtual environment"

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
else
    info "No requirements.txt found — skipping pip install"
fi

# Make relay.py executable
chmod +x "$REPO_ROOT/relay/relay.py" 2>/dev/null || true

# ══════════════════════════════════════════════════════════════════
#  3. Hermes Plugin (skip if --relay-only)
# ══════════════════════════════════════════════════════════════════
head "3/8 — Installing Hermes plugin"

if $RELAY_ONLY; then
    info "Skipping Hermes plugin (--relay-only)"
else
    # Create plugin symlink
    if [ -L "$PLUGIN_DIR" ] || [ -d "$PLUGIN_DIR" ]; then
        ok "Plugin symlink exists at $PLUGIN_DIR"
    else
        mkdir -p "$(dirname "$PLUGIN_DIR")"
        ln -sf "$REPO_ROOT/plugin" "$PLUGIN_DIR"
        ok "Plugin symlinked: $REPO_ROOT/plugin → $PLUGIN_DIR"
    fi

    # Enable plugin
    if $HERMES_FOUND; then
        if $HERMES plugins list 2>/dev/null | grep -q proxy-relay.*enabled; then
            ok "Plugin already enabled"
        else
            info "Enabling plugin..."
            $HERMES plugins enable proxy-relay 2>/dev/null && ok "Plugin enabled" || \
                info "Could not auto-enable. Run: hermes plugins enable proxy-relay"
        fi
        info "Restart gateway: hermes gateway restart (after configuring)"
    fi
fi

# ══════════════════════════════════════════════════════════════════
#  4. Config directory
# ══════════════════════════════════════════════════════════════════
head "4/8 — Creating config directory"

mkdir -p "$RELAY_CONFIG_DIR"
chmod 700 "$RELAY_CONFIG_DIR"
ok "Config directory: $RELAY_CONFIG_DIR"

# ══════════════════════════════════════════════════════════════════
#  5. Relay config.json (interactive)
# ══════════════════════════════════════════════════════════════════
head "5/8 — Relay configuration"

# Check if config already exists
if [ -f "${RELAY_CONFIG_DIR}/config.json" ]; then
    CURRENT_UPSTREAM=$(python3 -c "import json; d=json.load(open('${RELAY_CONFIG_DIR}/config.json')); print(d.get('UPSTREAM_BASE',''))" 2>/dev/null)
    echo -e "  Existing config found → upstream: ${CURRENT_UPSTREAM:-"(unknown)"}"
    echo -n "  Overwrite? [y/N] "
    read -r OVERWRITE
    if [[ ! "$OVERWRITE" =~ ^[Yy]$ ]]; then
        ok "Keeping existing config.json"
        RELAY_CONFIG_EXISTS=true
    else
        RELAY_CONFIG_EXISTS=false
    fi
else
    RELAY_CONFIG_EXISTS=false
fi

if ! $RELAY_CONFIG_EXISTS; then
    echo ""
    echo "  Enter the upstream API details (the service to proxy through):"
    echo ""

    # Upstream URL
    DEFAULT_URL="http://localhost:4000/v1"
    echo -n "  Upstream URL [$DEFAULT_URL]: "
    read -r UPSTREAM_URL
    UPSTREAM_URL="${UPSTREAM_URL:-$DEFAULT_URL}"

    # API Key
    echo ""
    echo "  Enter the API key for the upstream service."
    echo "  ⚠️  This is stored in plaintext at ${RELAY_CONFIG_DIR}/config.json (chmod 600)."
    echo -n "  API key [sk-test]: "
    read -r API_KEY
    API_KEY="${API_KEY:-sk-test}"

    # Auth type
    echo ""
    echo "  Auth type for the upstream:"
    echo "    1) bearer   — Authorization: Bearer <key> (OpenAI, most APIs)"
    echo "    2) x-api-key — x-api-key: <key> (OpenCode Zen, some APIs)"
    echo -n "  Choice [1]: "
    read -r AUTH_CHOICE
    if [ "$AUTH_CHOICE" = "2" ]; then
        AUTH_TYPE="x-api-key"
    else
        AUTH_TYPE="bearer"
    fi

    # Proxy list file path
    echo ""
    echo "  Path to your SOCKS5 proxy list file."
    echo "  If you don't have one yet, a placeholder will be created."
    DEFAULT_PROXY_LIST="${RELAY_CONFIG_DIR}/proxies.txt"
    echo -n "  Proxy list path [$DEFAULT_PROXY_LIST]: "
    read -r PROXY_LIST_PATH
    PROXY_LIST_PATH="${PROXY_LIST_PATH:-$DEFAULT_PROXY_LIST}"

    # Write config.json
    cat > "${RELAY_CONFIG_DIR}/config.json" << CONFIGEOF
{
  "UPSTREAM_BASE": "${UPSTREAM_URL}",
  "UPSTREAM_API_KEY": "${API_KEY}",
  "UPSTREAM_AUTH_TYPE": "${AUTH_TYPE}",
  "RELAY_PORT": ${RELAY_PORT},
  "MAX_CONCURRENT_UPSTREAM": 10,
  "MODEL_FILTER_PATTERN": ".*",
  "LOG_LEVEL": "INFO"
}
CONFIGEOF
    chmod 600 "${RELAY_CONFIG_DIR}/config.json"
    ok "Relay config written to ${RELAY_CONFIG_DIR}/config.json"

    # Write proxy list placeholder if needed
    if [ ! -f "$PROXY_LIST_PATH" ]; then
        mkdir -p "$(dirname "$PROXY_LIST_PATH")"
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
        ok "Placeholder proxy list created at $PROXY_LIST_PATH"
        info "👉 EDIT THIS FILE with your real SOCKS5 proxy URLs"
    fi

    # Also write env example file for reference
    cat > "${RELAY_CONFIG_DIR}/env.example" << ENVEOF
# Relay can also be configured via env vars (override config.json):
#
export UPSTREAM_BASE="${UPSTREAM_URL}"
export UPSTREAM_API_KEY="${API_KEY}"
export UPSTREAM_AUTH_TYPE="${AUTH_TYPE}"
export RELAY_PORT=${RELAY_PORT}
export PROXY_LIST="${PROXY_LIST_PATH}"
ENVEOF
    chmod 600 "${RELAY_CONFIG_DIR}/env.example"
fi

# ══════════════════════════════════════════════════════════════════
#  6. Systemd user service
# ══════════════════════════════════════════════════════════════════
head "6/8 — Service management"

HAS_SYSTEMD=false
if command -v systemctl &>/dev/null && systemctl --user &>/dev/null 2>&1; then
    HAS_SYSTEMD=true
fi

if $HAS_SYSTEMD; then
    echo -n "  Install systemd --user service? [Y/n]: "
    read -r SVC_CHOICE
    if [[ "$SVC_CHOICE" =~ ^[Nn]$ ]]; then
        info "Skipping systemd service"
    else
        info "Writing systemd unit..."
        mkdir -p "$SERVICE_DIR"
        cat > "$SYSTEMD_UNIT" << SERVICEEOF
[Unit]
Description=Hermes Proxy Relay — SOCKS5 proxy rotation for LLM APIs
Documentation=https://github.com/omiinaya/hermes-proxy-relay
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${VENV_DIR}/bin/python ${REPO_ROOT}/relay/relay.py
Restart=on-failure
RestartSec=5
Environment=PROXY_LIST=${RELAY_CONFIG_DIR}/proxies.txt
WorkingDirectory=${REPO_ROOT}

# Hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=${RELAY_CONFIG_DIR} ${REPO_ROOT}/relay

[Install]
WantedBy=default.target
SERVICEEOF

        systemctl --user daemon-reload
        ok "Systemd unit written to $SYSTEMD_UNIT"

        echo -n "  Start and enable the service now? [Y/n]: "
        read -r START_CHOICE
        if [[ ! "$START_CHOICE" =~ ^[Nn]$ ]]; then
            systemctl --user enable --now hermes-proxy-relay.service 2>/dev/null || true
            sleep 2
            if systemctl --user is-active hermes-proxy-relay.service &>/dev/null; then
                ok "hermes-proxy-relay.service is running"
            else
                info "Service installed but not running. Check: systemctl --user status hermes-proxy-relay"
                info "Logs: journalctl --user -u hermes-proxy-relay -n 20 --no-pager"
            fi
        fi

        # Check linger
        if loginctl show-user "$USER" 2>/dev/null | grep -q "Linger=no"; then
            echo ""
            info "Linger is disabled — the service will stop when you log out."
            echo -n "  Enable linger? (keeps service running after logout) [Y/n]: "
            read -r LINGER_CHOICE
            if [[ ! "$LINGER_CHOICE" =~ ^[Nn]$ ]]; then
                sudo loginctl enable-linger "$USER" 2>/dev/null && ok "Linger enabled" || \
                    err "Could not enable linger. Run manually: sudo loginctl enable-linger $USER"
            fi
        fi
    fi
else
    info "systemd not available. The relay will run in the foreground."
    info "  Start it:   PROXY_LIST=${RELAY_CONFIG_DIR}/proxies.txt ${VENV_DIR}/bin/python ${REPO_ROOT}/relay/relay.py"
    info "  Background: nohup PROXY_LIST=... python relay/relay.py &"
fi

# ══════════════════════════════════════════════════════════════════
#  7. Verify
# ══════════════════════════════════════════════════════════════════
head "7/8 — Verification"

# Check Venv
if [ -f "$VENV_DIR/bin/python3" ] && "$VENV_DIR/bin/python3" -c "import fastapi, httpx, uvicorn" &>/dev/null; then
    ok "Python venv: all dependencies available"
else
    err "Python venv: missing dependencies — re-run or manually: $VENV_DIR/bin/pip install -r $REPO_ROOT/requirements.txt"
fi

# Check relay script
if [ -f "$REPO_ROOT/relay/relay.py" ]; then
    ok "Relay script: $REPO_ROOT/relay/relay.py ($(wc -l < "$REPO_ROOT/relay/relay.py") lines)"
else
    err "Relay script not found at $REPO_ROOT/relay/relay.py"
fi

# Check plugin
if ! $RELAY_ONLY; then
    if [ -L "$PLUGIN_DIR" ] || [ -d "$PLUGIN_DIR" ]; then
        ok "Hermes plugin: $PLUGIN_DIR"
    else
        info "Hermes plugin not installed (use --relay-only or run the full setup)"
    fi
fi

# Check config
if [ -f "${RELAY_CONFIG_DIR}/config.json" ]; then
    ok "Relay config: ${RELAY_CONFIG_DIR}/config.json"
else
    err "Relay config not written — config.json missing"
fi

# Check systemd
if $HAS_SYSTEMD && [ -f "$SYSTEMD_UNIT" ]; then
    if systemctl --user is-active hermes-proxy-relay.service &>/dev/null; then
        ok "Systemd service: active (PID $(systemctl --user show -p MainPID hermes-proxy-relay.service 2>/dev/null | cut -d= -f2))"
    else
        info "Systemd service: installed but inactive. Start: systemctl --user start hermes-proxy-relay"
    fi
fi

# ══════════════════════════════════════════════════════════════════
#  8. Next Steps
# ══════════════════════════════════════════════════════════════════
head "8/8 — Next Steps"

cat << NEXTSTEPS

  ${BOLD}✅ Setup complete!${NC}

  ${BOLD}What's been done:${NC}
  • Python virtual environment created at $VENV_DIR
  • Dependencies installed (fastapi, httpx, uvicorn)
  • Config directory: $RELAY_CONFIG_DIR
  $($RELAY_ONLY || echo "  • Hermes plugin installed and enabled")

  ${BOLD}To start using the relay:${NC}

  1. Edit your proxy list with real SOCKS5 URLs:
     ${BOLD}vi ${RELAY_CONFIG_DIR}/proxies.txt${NC}

  2. Start the relay:
     ${BOLD}${VENV_DIR}/bin/python ${REPO_ROOT}/relay/relay.py${NC}

  3. Verify it's running:
     ${BOLD}curl -s http://localhost:${RELAY_PORT}/health${NC}

  $($RELAY_ONLY && cat << RELAYONLY
  ${BOLD}Configure Hermes (manual):${NC}
  Add to ~/.hermes/config.yaml:
    custom_providers:
    - name: proxy-relay
      base_url: http://localhost:${RELAY_PORT}/v1
      api_key: relay-key
      model: auto
  Then: hermes gateway restart

RELAYONLY
)
  $($RELAY_ONLY || cat << PLUGINPATH
  ${BOLD}In Hermes:${NC}
  • /relay status — check relay health
  • /relay setup list — see providers you can clone
  • /relay setup clone <N> — clone a provider with proxy routing

  Need to restart the gateway for plugin changes:
  ${BOLD}hermes gateway restart${NC} (from outside the gateway)

PLUGINPATH
)
  ${BOLD}Manage the relay:${NC}
  • Health:    curl -s http://localhost:${RELAY_PORT}/health
  • Models:    curl -s http://localhost:${RELAY_PORT}/v1/models
  • Chat:      curl -s -X POST http://localhost:${RELAY_PORT}/v1/chat/completions \\
                -H "Content-Type: application/json" \\
                -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":false}'

  $($HAS_SYSTEMD && [ -f "$SYSTEMD_UNIT" ] && cat << SYSTEMDDOCS
  ${BOLD}Systemd commands:${NC}
  • systemctl --user status hermes-proxy-relay
  • systemctl --user stop hermes-proxy-relay
  • systemctl --user restart hermes-proxy-relay
  • journalctl --user -u hermes-proxy-relay -n 50 --no-pager

SYSTEMDDOCS
)
  ${BOLD}Need help?${NC}
  • AGENTS.md — full AI agent onboarding
  • CLAUDE.md — quickstart for Claude Code
  • README.md — architecture and examples
NEXTSTEPS
