#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────
# Hermes Proxy Relay — Quick Benchmark
#
# Starts the relay and measures non-streaming request latency/throughput
# against a mocked upstream (all requests will fail with 502 since the
# proxy is dead — this measures RELAY overhead, not upstream).
#
# Usage:
#   ./scripts/benchmark.sh [requests] [concurrency]
# ────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=4996
BASE="http://localhost:${PORT}"
PYTHON="${PYTHON:-python3}"

REQUESTS="${1:-100}"
CONCURRENCY="${2:-10}"

echo "── Hermes Proxy Relay benchmark ───────────────────────────"
echo "Requests: ${REQUESTS}  Concurrency: ${CONCURRENCY}"

RELAY_PORT=${PORT} \
UPSTREAM_BASE="https://test.example.com/v1" \
UPSTREAM_API_KEY="bench-key" \
PROXY_LIST_ENV="socks5://u1:p1@127.0.0.1:9" \
ADMIN_API_KEY="" \
RELAY_SHUTDOWN_DRAIN_SECONDS="0" \
"${PYTHON}" "${REPO_ROOT}/relay/relay.py" >/tmp/relay_bench.log 2>&1 &
RELAY_PID=$!

cleanup() {
  kill "$RELAY_PID" 2>/dev/null || true
  wait "$RELAY_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
  curl -sf "${BASE}/health" >/dev/null 2>&1 && break
  sleep 0.5
done

# Warm up one request
curl -s -X POST "${BASE}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}' >/dev/null 2>&1 || true
# Clear cooldown so the warm-up 502 doesn't cool the proxy for the bench
curl -s -X POST "${BASE}/admin/clear-cooldowns" >/dev/null 2>&1 || true

# Concurrent benchmark using xargs
START=$(date +%s.%N)
seq "${REQUESTS}" | xargs -P "${CONCURRENCY}" -I{} \
  curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST "${BASE}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}' \
  > /tmp/relay_bench_codes.txt 2>/dev/null || true
END=$(date +%s.%N)

ELAPSED=$(echo "$END - $START" | bc 2>/dev/null || python3 -c "print($END - $START)")
RPS=$(python3 -c "print(f'{$REQUESTS / $ELAPSED:.1f}')")

TOTAL=$(wc -l < /tmp/relay_bench_codes.txt 2>/dev/null || echo 0)
CODES=$(sort /tmp/relay_bench_codes.txt 2>/dev/null | uniq -c | tr '\n' ' ')

echo ""
echo "── Results ────────────────────────────────────────────────"
echo "Elapsed:      ${ELAPSED}s"
echo "Throughput:   ${RPS} req/s"
echo "Responses:    ${TOTAL} (${CODES:-none})"
echo "Health check: $(curl -s --max-time 5 ${BASE}/health | ${PYTHON} -c 'import sys,json; d=json.load(sys.stdin); print(f"{d[\"request_stats\"][\"total\"]} total reqs, {d[\"request_stats\"][\"ok\"]} ok, {d[\"request_stats\"][\"errors\"]} errors")' 2>/dev/null || echo 'unavailable')"
echo ""
echo "NOTE: with a dead proxy, requests quickly hit the all-cooling 429 fast"
echo "      path (no upstream I/O) — this measures the relay's request-processing"
echo "      ceiling. Real-world throughput depends on proxy + upstream latency."
