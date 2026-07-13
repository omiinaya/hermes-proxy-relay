"""Hermes Proxy Relay — FastAPI relay with SOCKS5 rotation and 429 cooldown.

Runs a local OpenAI-compatible HTTP server that proxies requests through
a pool of SOCKS5 proxies, with dynamic rate-limit cooldown.

Usage:
    UPSTREAM_BASE=https://api.openai.com/v1 \
    UPSTREAM_API_KEY=sk-... \
    PROXY_LIST=/path/to/proxies.txt \
    python relay.py
"""

import asyncio
from datetime import datetime, timezone
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse

from .cooldown_pool import CooldownPool

# ── Config ──────────────────────────────────────────────────────────

UPSTREAM_BASE = os.environ.get("UPSTREAM_BASE", "").rstrip("/")
UPSTREAM_API_KEY = os.environ.get("UPSTREAM_API_KEY", "")
UPSTREAM_AUTH_TYPE = os.environ.get("UPSTREAM_AUTH_TYPE", "bearer").lower()
RELAY_PORT = int(os.environ.get("RELAY_PORT", "4002"))
MAX_CONCURRENT_UPSTREAM = int(os.environ.get("MAX_CONCURRENT_UPSTREAM", "10"))
MODEL_FILTER_PATTERN = os.environ.get("MODEL_FILTER_PATTERN", ".*")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Proxy list sources (checked in order)
PROXY_LIST_FILE = os.environ.get("PROXY_LIST", "")
PROXY_LIST_ENV = os.environ.get("PROXY_LIST_ENV", "")

# ── Logging ─────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("proxy-relay")

# ── Pool & Semaphore ───────────────────────────────────────────────

pool = CooldownPool()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPSTREAM)
_model_filter_re = re.compile(MODEL_FILTER_PATTERN)

# Request tracking
_request_count = {"total": 0, "ok": 0, "errors": 0}
_request_lock = asyncio.Lock()


def _load_proxies_from_file(path: str) -> list[str]:
    """Load proxy URLs from a text file (one per line)."""
    proxies = []
    try:
        with open(os.path.expanduser(path)) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    proxies.append(line)
        logger.info(f"Loaded {len(proxies)} proxies from {path}")
    except Exception as e:
        logger.error(f"Failed to load proxies from {path}: {e}")
    return proxies


def _load_proxies_from_env(env_val: str) -> list[str]:
    """Load proxy URLs from comma-separated env var."""
    proxies = [u.strip() for u in env_val.split(",") if u.strip()]
    logger.info(f"Loaded {len(proxies)} proxies from PROXY_LIST_ENV")
    return proxies


def _init_pool():
    """Initialize the proxy pool from configured sources."""
    proxies = []
    if PROXY_LIST_FILE:
        proxies = _load_proxies_from_file(PROXY_LIST_FILE)
    if not proxies and PROXY_LIST_ENV:
        proxies = _load_proxies_from_env(PROXY_LIST_ENV)
    if not proxies:
        logger.warning("No proxies configured — relay will return 503 for all requests")
    pool.reload(proxies)


# ── HTTP Client ────────────────────────────────────────────────────

async def _make_client(proxy_url: str) -> httpx.AsyncClient:
    """Create an httpx AsyncClient routed through a SOCKS5 proxy."""
    transport = httpx.AsyncHTTPTransport(proxy=proxy_url)
    return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(60.0))


def _build_headers(original: dict) -> dict:
    """Build upstream request headers, handling auth translation."""
    headers = {}
    skip_auth = False

    for key, val in original.items():
        lkey = key.lower()
        if lkey == "authorization":
            # Strip incoming auth — we'll add our own
            skip_auth = True
            continue
        if lkey in ("content-length", "host", "connection"):
            continue
        headers[key] = val

    # Add upstream auth
    if UPSTREAM_AUTH_TYPE == "x-api-key":
        headers["x-api-key"] = UPSTREAM_API_KEY
    else:
        headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"

    return headers


async def _proxy_request(
    method: str,
    path: str,
    body: bytes | None,
    headers: dict,
    query_string: str,
) -> Response | StreamingResponse:
    """Proxy a single request through the pool."""
    async with _request_lock:
        _request_count["total"] += 1

    # Select proxy (round-robin with cooldown)
    proxy_entry = pool.next()

    if proxy_entry is None:
        # All cooling — fail fast
        logger.warning("All proxies cooling, returning 429")
        return JSONResponse(
            status_code=429,
            content={
                "error": {
                    "message": "All proxies are in rate-limit cooldown. Try again later.",
                    "type": "rate_limit_error",
                    "code": "all_proxies_cooling",
                }
            },
            headers={"Retry-After": "30"},
        )

    upstream_url = f"{UPSTREAM_BASE}{path}"
    if query_string:
        upstream_url += f"?{query_string}"

    async with semaphore:
        try:
            async with await _make_client(proxy_entry.url) as client:
                req_headers = _build_headers(dict(headers))

                # Determine if streaming
                is_stream = False
                if body:
                    try:
                        parsed = json.loads(body)
                        if parsed.get("stream", False):
                            is_stream = True
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass

                if is_stream:
                    return await _proxy_stream(
                        client, method, upstream_url, req_headers, body,
                        proxy_entry,
                    )
                else:
                    return await _proxy_single(
                        client, method, upstream_url, req_headers, body,
                        proxy_entry,
                    )

        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            pool.record_timeout(proxy_entry)
            async with _request_lock:
                _request_count["errors"] += 1
            logger.warning(f"Proxy {proxy_entry.url} connect failed: {e}")
            return JSONResponse(
                status_code=502,
                content={
                    "error": {
                        "message": f"Proxy connection failed: {e}",
                        "type": "proxy_error",
                        "code": "proxy_connect_failed",
                    }
                },
            )

        except Exception as e:
            pool.record_timeout(proxy_entry)
            async with _request_lock:
                _request_count["errors"] += 1
            logger.error(f"Unexpected error on proxy {proxy_entry.url}: {e}")
            return JSONResponse(
                status_code=502,
                content={
                    "error": {
                        "message": f"Upstream error: {e}",
                        "type": "upstream_error",
                    }
                },
            )


async def _proxy_single(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: dict,
    body: bytes | None,
    proxy_entry,
) -> Response:
    """Non-streaming proxy call."""
    resp = await client.request(method, url, headers=headers, content=body)

    if resp.status_code == 429:
        retry_after = _parse_retry_after(resp.headers)
        pool.record_429(proxy_entry, retry_after)
        async with _request_lock:
            _request_count["errors"] += 1
        logger.warning(
            f"429 on proxy {proxy_entry.url} — cooling for {retry_after}s"
        )
    elif resp.status_code >= 400:
        async with _request_lock:
            _request_count["errors"] += 1
        pool.record_timeout(proxy_entry)
    else:
        pool.record_success(proxy_entry)
        async with _request_lock:
            _request_count["ok"] += 1

    # Build response
    resp_headers = {}
    for key, val in resp.headers.items():
        lkey = key.lower()
        if lkey in ("transfer-encoding", "content-encoding", "content-length"):
            continue
        resp_headers[key] = val

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=resp_headers,
        media_type=resp.headers.get("content-type"),
    )


async def _proxy_stream(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: dict,
    body: bytes | None,
    proxy_entry,
) -> StreamingResponse:
    """Streaming proxy call."""

    async def _generate() -> AsyncGenerator[bytes, None]:
        nonlocal proxy_entry
        try:
            async with client.stream(
                method, url, headers=headers, content=body
            ) as resp:
                if resp.status_code == 429:
                    retry_after = _parse_retry_after(resp.headers)
                    pool.record_429(proxy_entry, retry_after)
                    async with _request_lock:
                        _request_count["errors"] += 1
                    yield json.dumps({
                        "error": {
                            "message": "Rate limited via proxy",
                            "type": "rate_limit_error",
                            "code": "proxy_429",
                        }
                    }).encode()
                    return
                elif resp.status_code >= 400:
                    pool.record_timeout(proxy_entry)
                    async with _request_lock:
                        _request_count["errors"] += 1
                else:
                    pool.record_success(proxy_entry)
                    async with _request_lock:
                        _request_count["ok"] += 1

                async for chunk in resp.aiter_bytes():
                    yield chunk
        except Exception as e:
            pool.record_timeout(proxy_entry)
            async with _request_lock:
                _request_count["errors"] += 1
            logger.error(f"Stream error on {proxy_entry.url}: {e}")
            yield json.dumps({
                "error": {"message": f"Stream error: {e}", "type": "stream_error"}
            }).encode()

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


def _parse_retry_after(headers) -> int:
    """Parse Retry-After header, supporting both seconds and HTTP-date."""
    raw = headers.get("retry-after", "")
    if not raw:
        return 60
    try:
        return int(raw)
    except ValueError:
        # HTTP-date format — not commonly used by LLM APIs but handle it
        try:
            from email.utils import parsedate_to_datetime
            parsed = parsedate_to_datetime(raw)
            return int((parsed - datetime.now()).total_seconds())
        except Exception:
            return 60


def _model_allowed(model_name: str) -> bool:
    """Check if a model name passes the filter pattern."""
    return bool(_model_filter_re.search(model_name))


# ── FastAPI App ────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_pool()
    logger.info(
        f"Proxy Relay started on :{RELAY_PORT} "
        f"→ {UPSTREAM_BASE} "
        f"({pool.total} proxies, semaphore={MAX_CONCURRENT_UPSTREAM})"
    )
    yield
    logger.info("Proxy Relay shutting down")


app = FastAPI(
    title="Hermes Proxy Relay",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    """Health check with pool stats."""
    stats = pool.stats()
    return {
        "status": "ok" if stats["available"] > 0 else "degraded",
        "pool_stats": stats,
        "upstream_base": UPSTREAM_BASE,
        "models_available": len(MODELS_CACHE) if MODELS_CACHE else 0,
        "request_stats": dict(_request_count),
        "semaphore": {"max": MAX_CONCURRENT_UPSTREAM, "used": semaphore._value},
    }


@app.get("/v1/models")
async def list_models():
    """List available models from upstream, filtered."""
    if not UPSTREAM_BASE:
        return {"object": "list", "data": []}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            headers = {}
            if UPSTREAM_AUTH_TYPE == "x-api-key":
                headers["x-api-key"] = UPSTREAM_API_KEY
            else:
                headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"

            resp = await client.get(
                f"{UPSTREAM_BASE}/models", headers=headers
            )
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                filtered = [m for m in data if _model_allowed(m.get("id", ""))]
                _update_models_cache(filtered)
                return {"object": "list", "data": filtered}
            return {"object": "list", "data": list(MODELS_CACHE)}
    except Exception as e:
        logger.warning(f"Failed to fetch models: {e}")
        return {"object": "list", "data": list(MODELS_CACHE)}


# Models cache (updated periodically)
MODELS_CACHE: list[dict] = []


def _update_models_cache(models: list[dict]):
    global MODELS_CACHE
    MODELS_CACHE = models


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.body()
    headers = dict(request.headers)
    return await _proxy_request(
        "POST",
        "/chat/completions",
        body,
        headers,
        request.url.query or "",
    )


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_all(path: str, request: Request):
    """Generic proxy for all /v1/* paths."""
    body = await request.body() if request.method in ("POST", "PUT", "PATCH") else None
    headers = dict(request.headers)
    return await _proxy_request(
        request.method,
        f"/{path}",
        body,
        headers,
        request.url.query or "",
    )


# ── Entry Point ────────────────────────────────────────────────────

def main():
    """Run the relay server."""
    import uvicorn
    uvicorn.run(
        "relay.relay:app",
        host="0.0.0.0",
        port=RELAY_PORT,
        log_level=LOG_LEVEL.lower(),
    )


if __name__ == "__main__":
    main()
