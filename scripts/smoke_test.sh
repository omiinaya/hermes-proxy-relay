#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────
# Hermes Proxy Relay — Smoke Test
#
# Starts the relay on a test port, hits every endpoint, verifies
# responses, then shuts down. Uses a dummy upstream and a dead proxy
# so no real network calls are needed. Exit code 0 = all good.
#
# Usage:
#   ./scripts/smoke_test.sh
# ────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=4997
BASE="http://localhost:${PORT}"
LOG_FILE="$(mktemp)"

PYTHON="${PYTHON:-python3}"

echo "── Hermes Proxy Relay smoke test ────────────────────────"
echo "Port: ${PORT}"

# ── Start relay ────────────────────────────────────────────────
RELAY_PORT=${PORT} \
UPSTREAM_BASE="https://test.example.com/v1" \
UPSTREAM_API_KEY="smoke-test-key" \
PROXY_LIST_ENV="socks5://u1:p1@127.0.0.1:9" \
ADMIN_API_KEY="smoke-admin" \
"${PYTHON}" "${REPO_ROOT}/relay/relay.py" >"$LOG_FILE" 2>&1 &
RELAY_PID=$!

cleanup() {
  kill "$RELAY_PID" 2>/dev/null || true
  wait "$RELAY_PID" 2>/dev/null || true
  rm -f "$LOG_FILE"
}
trap cleanup EXIT

# Wait for startup
for _ in $(seq 1 20); do
  if curl -sf "${BASE}/health" >/dev/null 2>&1; then break; fi
  sleep 0.5
done

PASS=0
FAIL=0

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  ✓ ${desc} (${actual})"
    PASS=$((PASS + 1))
  else
    echo "  ✗ ${desc}: expected ${expected}, got ${actual}"
    FAIL=$((FAIL + 1))
  fi
}

# ── Health ────────────────────────────────────────────────────
code=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/health")
check "GET /health" "200" "$code"

version=$(curl -s "${BASE}/health" | "${PYTHON}" -c "import sys,json; print(json.load(sys.stdin)['version'])")
check "health.version matches" "1.3.0" "$version"

status=$(curl -s "${BASE}/health" | "${PYTHON}" -c "import sys,json; print(json.load(sys.stdin)['status'])")
check "health.status" "ok" "$status"

# ── Models ────────────────────────────────────────────────────
code=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/v1/models")
check "GET /v1/models" "200" "$code"

# ── Admin auth ────────────────────────────────────────────────
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${BASE}/admin/clear-cooldowns")
check "admin no key" "403" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${BASE}/admin/clear-cooldowns" \
  -H "X-Admin-Key: wrong")
check "admin wrong key" "403" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${BASE}/admin/clear-cooldowns" \
  -H "X-Admin-Key: smoke-admin")
check "admin correct key" "200" "$code"

# ── Chat (dead proxy → 502) ───────────────────────────────────
# Clear cooldowns first so the proxy is warm
curl -s -X POST "${BASE}/admin/clear-cooldowns" -H "X-Admin-Key: smoke-admin" >/dev/null
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${BASE}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}')
check "POST chat (dead proxy)" "502" "$code"

# ── Streaming (dead proxy → 502) ──────────────────────────────
curl -s -X POST "${BASE}/admin/clear-cooldowns" -H "X-Admin-Key: smoke-admin" >/dev/null
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${BASE}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stream":true}')
check "POST chat stream (dead proxy)" "502" "$code"

# ── Admin reset-proxy (404 for unknown) ───────────────────────
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${BASE}/admin/reset-proxy" \
  -H "Content-Type: application/json" -H "X-Admin-Key: smoke-admin" \
  -d '{"url":"socks5://unknown:1080"}')
check "admin reset unknown proxy" "404" "$code"

# ── Version flag ──────────────────────────────────────────────
version_out=$("${PYTHON}" "${REPO_ROOT}/relay/relay.py" --version)
check "relay.py --version" "Hermes Proxy Relay v1.3.0" "$version_out"

# ── Config check flag ────────────────────────────────────────
check_code=$(RELAY_PORT="${PORT}" \
  UPSTREAM_BASE="https://test.example.com/v1" \
  UPSTREAM_API_KEY="smoke-test-key" \
  PROXY_LIST_ENV="socks5://u1:p1@127.0.0.1:9" \
  "${PYTHON}" "${REPO_ROOT}/relay/relay.py" --check >/dev/null 2>&1; echo $?)
check "relay.py --check (valid config)" "0" "$check_code"

echo ""
echo "── Result: ${PASS} passed, ${FAIL} failed ─────────────────"
[ "$FAIL" -eq 0 ] && echo "✅ SMOKE TEST PASSED" || echo "❌ SMOKE TEST FAILED"
exit "$FAIL"
