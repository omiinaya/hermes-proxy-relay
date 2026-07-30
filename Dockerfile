FROM python:3.11-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Runtime ─────────────────────────────────────────────────────────
FROM python:3.11-slim

RUN addgroup --system relay && adduser --system --ingroup relay relay

WORKDIR /app

# Copy only the relay module (not setup scripts, tests, plugin, mcp)
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY relay/ relay/

USER relay

EXPOSE 4002

ENV RELAY_PORT=4002 \
    UPSTREAM_BASE="" \
    UPSTREAM_API_KEY="" \
    UPSTREAM_AUTH_TYPE="bearer" \
    MAX_CONCURRENT_UPSTREAM=10 \
    MODEL_FILTER_PATTERN=".*" \
    LOG_LEVEL="INFO" \
    PROXY_LIST="" \
    PROXY_LIST_ENV="" \
    ADMIN_API_KEY=""

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:4002/health')" || exit 1

ENTRYPOINT ["python", "relay/relay.py"]
