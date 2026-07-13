#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────
# Hermes Proxy Relay — one-command setup
# ────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
VENV_DIR="${HERMES_HOME}/proxy-relay-venv"
PLUGIN_DIR="${HERMES_HOME}/plugins/proxy-relay"
ENV_FILE="${HERMES_HOME}/.env"
CONFIG_FILE="${HERMES_HOME}/config.yaml"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()  { echo -e " ${GREEN}✓${NC} $1"; }
info(){ echo -e " ${YELLOW}ℹ${NC} $1"; }
err() { echo -e " ${RED}✗${NC} $1"; }

echo ""
echo "  ╭──────────────────────────────────────╮"
echo "  │   Hermes Proxy Relay — Setup         │"
echo "  ╰──────────────────────────────────────╯"
echo ""

# ── Check Hermes is installed ────────────────────────────────────
if ! command -v hermes &>/dev/null; then
    err "Hermes is not installed. Install it first:"
    err "  curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash"
    exit 1
fi
ok "Hermes found: $(hermes --version 2>/dev/null || echo 'unknown')"

# ── Python venv ──────────────────────────────────────────────────
if [ -d "$VENV_DIR" ]; then
    ok "Virtual env exists at $VENV_DIR"
else
    info "Creating venv at $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
    ok "Virtual env created"
fi

# ── Install deps ─────────────────────────────────────────────────
info "Installing Python dependencies..."
"$VENV_DIR/bin/pip" install -q -r "$REPO_ROOT/requirements.txt"
ok "Dependencies installed"

# ── Hermes Plugin ────────────────────────────────────────────────
if [ -L "$PLUGIN_DIR" ] || [ -d "$PLUGIN_DIR" ]; then
    ok "Plugin directory exists at $PLUGIN_DIR"
else
    info "Installing plugin..."
    ln -sf "$REPO_ROOT/plugin" "$PLUGIN_DIR"
    hermes plugins enable proxy-relay 2>/dev/null || true
    ok "Plugin installed & enabled"
fi

# ── Env vars ─────────────────────────────────────────────────────
NEED_ENV=false

if ! grep -q "^UPSTREAM_BASE=" "$ENV_FILE" 2>/dev/null; then
    err "UPSTREAM_BASE not set in $ENV_FILE"
    NEED_ENV=true
fi
if ! grep -q "^UPSTREAM_API_KEY=" "$ENV_FILE" 2>/dev/null; then
    err "UPSTREAM_API_KEY not set in $ENV_FILE"
    NEED_ENV=true
fi
if ! grep -q "^PROXY_LIST=" "$ENV_FILE" 2>/dev/null && \
   ! grep -q "^PROXY_LIST_ENV=" "$ENV_FILE" 2>/dev/null; then
    err "PROXY_LIST or PROXY_LIST_ENV not set in $ENV_FILE"
    NEED_ENV=true
fi

if [ "$NEED_ENV" = true ]; then
    echo ""
    info "Add these to $ENV_FILE:"
    echo "  UPSTREAM_BASE=https://api.opencode-zen.com/v1"
    echo "  UPSTREAM_API_KEY=public"
    echo "  UPSTREAM_AUTH_TYPE=x-api-key"
    echo "  PROXY_LIST=/path/to/proxies.txt"
    echo ""
    info "Then restart: hermes gateway restart"
fi

# ── Check proxy list file ────────────────────────────────────────
PROXY_LIST_VAL=""
if [ -n "${PROXY_LIST:-}" ]; then
    PROXY_LIST_VAL="$PROXY_LIST"
elif grep -q "^PROXY_LIST=" "$ENV_FILE" 2>/dev/null; then
    PROXY_LIST_VAL=$(grep "^PROXY_LIST=" "$ENV_FILE" | cut -d'=' -f2)
fi

if [ -n "$PROXY_LIST_VAL" ] && [ -f "$PROXY_LIST_VAL" ]; then
    COUNT=$(wc -l < "$PROXY_LIST_VAL")
    ok "Proxy list file: $PROXY_LIST_VAL ($COUNT proxies)"
fi

# ── Reload gateway ───────────────────────────────────────────────
if [ "$NEED_ENV" = false ]; then
    echo ""
    info "Setup complete! Starting relay..."
    echo ""
    echo "  1. Start the relay:"
    echo "     cd $REPO_ROOT/relay"
    echo "     source $VENV_DIR/bin/activate"
    echo "     python relay.py"
    echo ""
    echo "  2. In Hermes, run: /relay status"
    echo "  3. Or use hermes chat -m auto"
    echo ""
fi
