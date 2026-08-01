# ────────────────────────────────────────────────────────────────────
# Hermes Proxy Relay — Dockerfile
# ────────────────────────────────────────────────────────────────────
# Build:  docker build -t hermes-proxy-relay .
# Run:    docker run -d -p 4002:4002 \
#           -v ~/.hermes/proxy-relay:/data/config \
#           -e PROXY_LIST=/data/config/proxies.txt \
#           -e RELAY_CONFIG=/data/config/config.json \
#           hermes-proxy-relay
# ────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# ── Runtime stage ─────────────────────────────────────────────────
FROM python:3.11-slim

RUN addgroup --system relay && adduser --system --ingroup relay relay

WORKDIR /app

# Copy deps from builder
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Copy relay code
COPY relay/ relay/

USER relay

EXPOSE 4002

ENV RELAY_PORT=4002 \
    RELAY_CONFIG=/data/config/config.json \
    UPSTREAM_BASE="" \
    UPSTREAM_API_KEY="" \
    UPSTREAM_AUTH_TYPE="bearer" \
    MAX_CONCURRENT_UPSTREAM=10 \
    MAX_REQUEST_RETRIES=3 \
    SEMAPHORE_WAIT_SECONDS=30.0 \
    CONSECUTIVE_ERROR_THRESHOLD=3 \
    PERMANENT_COOLDOWN_SECONDS=86400 \
    MAX_RETRY_AFTER_SECONDS=3600 \
    PROXY_HEALTH_CHECK_INTERVAL=60 \
    PROXY_HEALTH_CHECK_URL="http://httpbin.org/ip" \
    HEALTH_FAIL_THRESHOLD=3 \
    MAX_BODY_SIZE=104857600 \
    MODEL_FILTER_PATTERN=".*" \
    LOG_LEVEL="INFO" \
    PROXY_LIST="" \
    PROXY_LIST_ENV="" \
    ADMIN_API_KEY="" \
    CLIENT_API_KEY=""

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:4002/health', timeout=5)" || exit 1

ENTRYPOINT ["python3", "relay/relay.py"]
