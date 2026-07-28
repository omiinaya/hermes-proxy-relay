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
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse


# ╔══════════════════════════════════════════════════════════════════╗
# ║  CooldownPool — proxy rotation with dynamic 429 cooldown       ║
# ╚══════════════════════════════════════════════════════════════════╝

@dataclass
class ProxyEntry:
    """A single proxy with cooldown state."""
    url: str
    cooldown_until: float = 0.0  # time.monotonic() when cooling; 0 = ready
    last_error: str = ""
    consecutive_errors: int = 0      # tracks connection-level failures (timeouts, 4xx/5xx)
    consecutive_429: int = 0         # tracks rate-limit responses separately
    total_ok: int = 0
    total_429: int = 0
    permanently_dead: bool = False   # True after N consecutive connection-level failures


class CooldownPool:
    """Thread-safe pool of proxies with dynamic Retry-After cooldown.

    After a 429 response, the proxy is cooled for the exact duration
    specified by the upstream Retry-After header. While cooling, the
    proxy is skipped on all subsequent requests. When ALL proxies are
    cooling, next() returns None (caller returns 429 immediately).
    """

    def __init__(self, proxies: list[str] | None = None):
        self._lock = threading.Lock()
        self._proxies: list[ProxyEntry] = []
        self._index = -1  # first next() increments to 0
        self._all_time_ok = 0
        self._all_time_429 = 0
        if proxies:
            for p in proxies:
                self._proxies.append(ProxyEntry(url=p))

    @property
    def total(self) -> int:
        return len(self._proxies)

    @property
    def available_count(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for p in self._proxies if p.cooldown_until <= now)

    @property
    def cooling_count(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for p in self._proxies if p.cooldown_until > now)

    @property
    def all_cooling(self) -> bool:
        now = time.monotonic()
        with self._lock:
            return all(p.cooldown_until > now for p in self._proxies)

    def next(self) -> Optional[ProxyEntry]:
        now = time.monotonic()
        with self._lock:
            if not self._proxies:
                return None
            if all(p.cooldown_until > now for p in self._proxies):
                return None
            n = len(self._proxies)
            for _ in range(n):
                self._index = (self._index + 1) % n
                candidate = self._proxies[self._index]
                if candidate.cooldown_until <= now:
                    return candidate
            return None

    def record_429(self, proxy: ProxyEntry, retry_after: int = 60):
        now = time.monotonic()
        with self._lock:
            proxy.cooldown_until = now + max(retry_after, 10)
            proxy.consecutive_429 += 1
            proxy.total_429 += 1
            proxy.last_error = f"429 rate limited (cooling {max(retry_after, 10)}s)"
            self._all_time_429 += 1

    def record_timeout(self, proxy: ProxyEntry):
        now = time.monotonic()
        with self._lock:
            proxy.consecutive_errors += 1
            if proxy.consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                proxy.cooldown_until = now + PERMANENT_COOLDOWN_SECONDS
                proxy.permanently_dead = True
                proxy.last_error = (
                    f"Permanent failure after {proxy.consecutive_errors} "
                    f"consecutive errors (cooling {PERMANENT_COOLDOWN_SECONDS}s)"
                )
                logger.warning(
                    f"Proxy {proxy.url} MARKED PERMANENTLY UNAVAILABLE "
                    f"({proxy.consecutive_errors} consecutive errors, "
                    f"cooling {PERMANENT_COOLDOWN_SECONDS}s)"
                )
            else:
                proxy.cooldown_until = now + 30
                proxy.last_error = (
                    f"Temporary failure ({proxy.consecutive_errors}/"
                    f"{CONSECUTIVE_ERROR_THRESHOLD} consecutive)"
                )

    def record_permanent_failure(self, proxy: ProxyEntry, reason: str = ""):
        """Explicitly mark a proxy as permanently failed (e.g., API-reported exhaustion)."""
        now = time.monotonic()
        with self._lock:
            proxy.cooldown_until = now + PERMANENT_COOLDOWN_SECONDS
            proxy.permanently_dead = True
            proxy.consecutive_errors += 1
            proxy.last_error = reason or f"Permanent failure (cooling {PERMANENT_COOLDOWN_SECONDS}s)"
            logger.warning(
                f"Proxy {proxy.url} PERMANENTLY DEACTIVATED: {proxy.last_error}"
            )

    def record_success(self, proxy: ProxyEntry):
        with self._lock:
            proxy.consecutive_errors = 0
            proxy.consecutive_429 = 0
            proxy.total_ok += 1
            proxy.permanently_dead = False
            proxy.last_error = ""
            self._all_time_ok += 1

    def stats(self) -> dict:
        now = time.monotonic()
        with self._lock:
            short_cool = []
            perm_cool = []
            for p in self._proxies:
                remaining = max(0, p.cooldown_until - now)
                if remaining > 0:
                    entry = {
                        "proxy": p.url,
                        "remaining_s": int(remaining),
                        "total_429": p.total_429,
                        "total_ok": p.total_ok,
                        "last_error": p.last_error,
                    }
                    if p.permanently_dead or remaining >= PERMANENT_COOLDOWN_SECONDS // 2:
                        perm_cool.append(entry)
                    else:
                        short_cool.append(entry)
            return {
                "total": len(self._proxies),
                "available": sum(1 for p in self._proxies if p.cooldown_until <= now),
                "cooling": len(short_cool),
                "permanently_failed": len(perm_cool),
                "cooling_details": sorted(short_cool, key=lambda x: x["remaining_s"]),
                "permanently_failed_details": sorted(perm_cool, key=lambda x: x["remaining_s"]),
                "all_time_ok": self._all_time_ok,
                "all_time_429": self._all_time_429,
            }

    def reload(self, proxies: list[str]):
        now = time.monotonic()
        with self._lock:
            old_map = {p.url: p for p in self._proxies}
            new_list = []
            for url in proxies:
                existing = old_map.get(url)
                if existing:
                    new_list.append(existing)
                else:
                    new_list.append(ProxyEntry(url=url, cooldown_until=now))
            self._proxies = new_list
            self._index = -1

    def clear_cooldowns(self):
        now = time.monotonic()
        with self._lock:
            for p in self._proxies:
                p.cooldown_until = now
                p.consecutive_errors = 0
                p.consecutive_429 = 0
                p.permanently_dead = False
                p.last_error = ""

    def reset_proxy(self, proxy_url: str) -> bool:
        """Reset a single proxy's cooldown and error state. Returns True if found."""
        now = time.monotonic()
        with self._lock:
            for p in self._proxies:
                if p.url == proxy_url:
                    p.cooldown_until = now
                    p.consecutive_errors = 0
                    p.consecutive_429 = 0
                    p.permanently_dead = False
                    p.last_error = ""
                    return True
            return False

    def reset_by_errors(self, min_consecutive: int) -> int:
        """Reset all proxies that have at least min_consecutive errors. Returns count."""
        now = time.monotonic()
        count = 0
        with self._lock:
            for p in self._proxies:
                if p.permanently_dead and p.consecutive_errors >= min_consecutive:
                    p.cooldown_until = now
                    p.consecutive_errors = 0
                    p.consecutive_429 = 0
                    p.permanently_dead = False
                    p.last_error = ""
                    count += 1
        return count


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Config (from env vars, or --config JSON)                      ║
# ╚══════════════════════════════════════════════════════════════════╝

def _load_config_file(path: str) -> dict:
    """Load config from a JSON file (written by the Hermes plugin)."""
    try:
        p = os.path.expanduser(path)
        if os.path.exists(p):
            with open(p) as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load config file {path}: {e}")
    return {}


_DEFAULT_CONFIG = {
    "UPSTREAM_BASE": "",
    "UPSTREAM_API_KEY": "",
    "UPSTREAM_AUTH_TYPE": "bearer",
    "RELAY_PORT": 4002,
    "MAX_CONCURRENT_UPSTREAM": 10,
    "MODEL_FILTER_PATTERN": ".*",
    "LOG_LEVEL": "INFO",
    "PROXY_LIST": "",
    "PROXY_LIST_ENV": "",
    "CONSECUTIVE_ERROR_THRESHOLD": 3,
    "PERMANENT_COOLDOWN_SECONDS": 86400,
}


def _merge_config(file_config: dict) -> dict:
    """Env vars take precedence over file config."""
    cfg = dict(_DEFAULT_CONFIG)
    cfg.update(file_config)
    for key in cfg:
        env_val = os.environ.get(key)
        if env_val is not None and env_val != "":
            cfg[key] = env_val
    return cfg


# Config file path (from --config CLI arg, or env, or default location)
_CONFIG_PATH = os.environ.get(
    "RELAY_CONFIG",
    os.path.expanduser("~/.hermes/proxy-relay/config.json"),
)
_file_cfg = _load_config_file(_CONFIG_PATH) if _CONFIG_PATH else {}
_merged = _merge_config(_file_cfg)

UPSTREAM_BASE = str(_merged["UPSTREAM_BASE"]).rstrip("/")
UPSTREAM_API_KEY = str(_merged["UPSTREAM_API_KEY"])
UPSTREAM_AUTH_TYPE = str(_merged["UPSTREAM_AUTH_TYPE"]).lower()
RELAY_PORT = int(_merged["RELAY_PORT"])
MAX_CONCURRENT_UPSTREAM = int(_merged["MAX_CONCURRENT_UPSTREAM"])
MODEL_FILTER_PATTERN = str(_merged["MODEL_FILTER_PATTERN"])
LOG_LEVEL = str(_merged["LOG_LEVEL"]).upper()
PROXY_LIST_FILE = os.environ.get("PROXY_LIST", str(_merged.get("PROXY_LIST", "")))
PROXY_LIST_ENV = os.environ.get("PROXY_LIST_ENV", str(_merged.get("PROXY_LIST_ENV", "")))
CONSECUTIVE_ERROR_THRESHOLD = int(os.environ.get("CONSECUTIVE_ERROR_THRESHOLD",
    str(_merged.get("CONSECUTIVE_ERROR_THRESHOLD", 3))))
PERMANENT_COOLDOWN_SECONDS = int(os.environ.get("PERMANENT_COOLDOWN_SECONDS",
    str(_merged.get("PERMANENT_COOLDOWN_SECONDS", 86400))))

# ╔══════════════════════════════════════════════════════════════════╗
# ║  Logging                                                       ║
# ╚══════════════════════════════════════════════════════════════════╝

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("proxy-relay")

# ╔══════════════════════════════════════════════════════════════════╗
# ║  Global state                                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

pool = CooldownPool()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPSTREAM)
_model_filter_re = re.compile(MODEL_FILTER_PATTERN)
_request_count = {"total": 0, "ok": 0, "errors": 0}
_request_lock = asyncio.Lock()
MODELS_CACHE: list[dict] = []


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
    proxies = []
    if PROXY_LIST_FILE:
        proxies = _load_proxies_from_file(PROXY_LIST_FILE)
    if not proxies and PROXY_LIST_ENV:
        proxies = _load_proxies_from_env(PROXY_LIST_ENV)
    if not proxies:
        logger.warning("No proxies configured — relay will return 503 for all requests")
    pool.reload(proxies)


def _update_models_cache(models: list[dict]):
    global MODELS_CACHE
    MODELS_CACHE = models


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Proxy helpers                                                 ║
# ╚══════════════════════════════════════════════════════════════════╝

async def _make_client(proxy_url: str) -> httpx.AsyncClient:
    transport = httpx.AsyncHTTPTransport(proxy=proxy_url)
    return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(60.0))


def _build_headers(original: dict) -> dict:
    """Forward client headers, stripping those the relay manages itself.

    The relay is responsible for its own upstream content negotiation:
    httpx auto-decompresses gzip/deflate/brotli responses, and we
    strip Content-Encoding from responses so the client always gets
    uncompressed data. Passing Accept-Encoding from the client would
    risk codecs httpx doesn't handle (e.g. zstd) being returned
    compressed without the header to signal it.
    """
    headers = {}
    for key, val in original.items():
        lkey = key.lower()
        if lkey == "authorization":
            continue
        if lkey in ("content-length", "host", "connection", "accept-encoding"):
            continue
        headers[key] = val
    if UPSTREAM_AUTH_TYPE == "x-api-key":
        headers["x-api-key"] = UPSTREAM_API_KEY
    else:
        headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"
    return headers


def _parse_retry_after(headers) -> int:
    raw = headers.get("retry-after", "")
    if not raw:
        return 60
    try:
        return int(raw)
    except ValueError:
        try:
            from email.utils import parsedate_to_datetime
            parsed = parsedate_to_datetime(raw)
            return int((parsed - datetime.now(timezone.utc)).total_seconds())
        except Exception:
            return 60


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Proxy request logic (streaming + single-shot)                  ║
# ╚══════════════════════════════════════════════════════════════════╝


def _model_allowed(model_name: str) -> bool:
    return bool(_model_filter_re.search(model_name))


async def _proxy_request(
    method: str,
    path: str,
    body: bytes | None,
    headers: dict,
    query_string: str,
) -> Response | StreamingResponse:
    async with _request_lock:
        _request_count["total"] += 1

    proxy_entry = pool.next()
    if proxy_entry is None:
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
                is_stream = False
                if body:
                    # Byte-level stream detection avoids parsing the full JSON
                    # body, which can be several MB for vision requests with
                    # base64-encoded images. This is ~100x faster for large bodies.
                    body_lower = body.lower()
                    is_stream = (
                        b'"stream":true' in body_lower
                        or b'"stream": true' in body_lower
                    )

                if is_stream:
                    return await _proxy_stream(client, method, upstream_url,
                                               req_headers, body, proxy_entry)
                else:
                    return await _proxy_single(client, method, upstream_url,
                                              req_headers, body, proxy_entry)

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


async def _proxy_single(client, method, url, headers, body, proxy_entry) -> Response:
    """Single-shot proxy: forward request, decompress response, relay headers.

    Strips Content-Encoding, Transfer-Encoding, and Content-Length from
    response headers because httpx auto-decompresses gzip/deflate/brotli
    and the response body length changes.
    """
    resp = await client.request(method, url, headers=headers, content=body)

    if resp.status_code == 429:
        retry_after = _parse_retry_after(resp.headers)
        pool.record_429(proxy_entry, retry_after)
        async with _request_lock:
            _request_count["errors"] += 1
        logger.warning(f"429 on {proxy_entry.url} — cooling for {retry_after}s")
    elif resp.status_code >= 400:
        async with _request_lock:
            _request_count["errors"] += 1
        pool.record_timeout(proxy_entry)
    else:
        pool.record_success(proxy_entry)
        async with _request_lock:
            _request_count["ok"] += 1

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


async def _proxy_stream(client, method, url, headers, body, proxy_entry) -> StreamingResponse:
    """Streaming proxy: forward chunked response, relaying upstream headers.

    Uses client.send(req, stream=True) instead of client.stream() so the
    upstream response headers are available before the StreamingResponse
    is constructed — this lets us forward x-request-id, openai-*,
    x-ratelimit-*, and other headers that clients rely on.
    """
    req = client.build_request(method, url, headers=headers, content=body)
    resp = await client.send(req, stream=True)

    # Build filtered response headers from the upstream response
    resp_headers = {}
    for key, val in resp.headers.items():
        lkey = key.lower()
        if lkey in ("transfer-encoding", "content-encoding", "content-length"):
            continue
        # Let FastAPI's Response/media_type set content-type to avoid duplicates
        if lkey == "content-type":
            continue
        resp_headers[key] = val

    # ── Non-stream error responses ───────────────────────────────
    if resp.status_code == 429:
        retry_after = _parse_retry_after(resp.headers)
        pool.record_429(proxy_entry, retry_after)
        async with _request_lock:
            _request_count["errors"] += 1
        error_body = await resp.aread()
        await resp.aclose()
        return Response(
            content=error_body,
            status_code=429,
            headers=resp_headers,
            media_type="application/json",
        )

    if resp.status_code >= 400:
        pool.record_timeout(proxy_entry)
        async with _request_lock:
            _request_count["errors"] += 1
        error_body = await resp.aread()
        await resp.aclose()
        return Response(
            content=error_body,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type="application/json",
        )

    # ── Success — stream the body ────────────────────────────────
    pool.record_success(proxy_entry)
    async with _request_lock:
        _request_count["ok"] += 1

    async def _generate():
        try:
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
        finally:
            await resp.aclose()

    return StreamingResponse(
        _generate(),
        status_code=resp.status_code,
        headers=resp_headers,
        media_type=resp.headers.get("content-type", "text/event-stream"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  FastAPI app + routes                                           ║
# ╚══════════════════════════════════════════════════════════════════╝


@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_pool()
    logger.info(
        f"Proxy Relay started on :{RELAY_PORT} "
        f"\u2192 {UPSTREAM_BASE} "
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
    stats = pool.stats()
    return {
        "status": "ok" if stats["available"] > 0 else "degraded",
        "pool_stats": stats,
        "upstream_base": UPSTREAM_BASE,
        "models_available": len(MODELS_CACHE) if MODELS_CACHE else 0,
        "request_stats": dict(_request_count),
        "semaphore": {"max": MAX_CONCURRENT_UPSTREAM, "used": MAX_CONCURRENT_UPSTREAM - semaphore._value},
    }


@app.get("/v1/models")
async def list_models():
    if not UPSTREAM_BASE:
        return {"object": "list", "data": []}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            headers = {}
            if UPSTREAM_AUTH_TYPE == "x-api-key":
                headers["x-api-key"] = UPSTREAM_API_KEY
            else:
                headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"
            resp = await client.get(f"{UPSTREAM_BASE}/models", headers=headers)
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                filtered = [m for m in data if _model_allowed(m.get("id", ""))]
                _update_models_cache(filtered)
                return {"object": "list", "data": filtered}
            return {"object": "list", "data": list(MODELS_CACHE)}
    except Exception as e:
        logger.warning(f"Failed to fetch models: {e}")
        return {"object": "list", "data": list(MODELS_CACHE)}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.body()
    headers = dict(request.headers)
    return await _proxy_request(
        "POST", "/chat/completions", body, headers, request.url.query or "",
    )


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_all(path: str, request: Request):
    body = await request.body() if request.method in ("POST", "PUT", "PATCH") else None
    headers = dict(request.headers)
    return await _proxy_request(
        request.method, f"/{path}", body, headers, request.url.query or "",
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Admin endpoints                                               ║
# ╚══════════════════════════════════════════════════════════════════╝


@app.post("/admin/clear-cooldowns")
async def admin_clear_cooldowns():
    """Reset ALL proxies to available (clears temporary AND permanent cooldowns)."""
    pool.clear_cooldowns()
    logger.info("All proxy cooldowns cleared (admin)")
    return {
        "status": "ok",
        "message": "All cooldowns cleared",
        "proxies_total": pool.total,
        "available": pool.available_count,
    }


@app.post("/admin/reset-proxy")
async def admin_reset_proxy(request: Request):
    """Reset a single proxy by URL. Body: {\"url\": \"socks5://...\"}"""
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})
    url = data.get("url", "")
    if not url:
        return JSONResponse(status_code=400, content={"error": "Body must include 'url' field"})
    if pool.reset_proxy(url):
        logger.info(f"Proxy reset (admin): {url}")
        return {"status": "ok", "message": f"Proxy reset: {url}"}
    return JSONResponse(
        status_code=404,
        content={"error": f"Proxy not found in pool: {url}"},
    )


@app.post("/admin/reload-proxies")
async def admin_reload_proxies():
    """Reload the proxy list from the configured file/env."""
    _init_pool()
    logger.info(f"Proxy list reloaded (admin): {pool.total} proxies")
    return {
        "status": "ok",
        "message": f"Proxy list reloaded",
        "proxies_total": pool.total,
        "available": pool.available_count,
    }


@app.post("/admin/reset-by-errors")
async def admin_reset_by_errors(request: Request):
    """Reset all proxies that have been permanently failed.
    Body: {\"min_consecutive\": 3} (optional, defaults to CONSECUTIVE_ERROR_THRESHOLD)"""
    try:
        data = await request.json() if request.headers.get("content-length") else {}
    except Exception:
        data = {}
    min_errs = data.get("min_consecutive", CONSECUTIVE_ERROR_THRESHOLD)
    reset_count = pool.reset_by_errors(min_errs)
    logger.info(f"Reset {reset_count} permanently-failed proxies (admin)")
    return {"status": "ok", "message": f"Reset {reset_count} proxies"}
def main():
    """Entry point. Supports --config <path> for config file override."""
    import argparse
    parser = argparse.ArgumentParser(description="Hermes Proxy Relay")
    parser.add_argument(
        "--config", "-c",
        default=os.environ.get("RELAY_CONFIG", ""),
        help="Path to JSON config file (default: ~/.hermes/proxy-relay/config.json)",
    )
    args = parser.parse_args()

    # Re-merge if --config was passed (overrides env/cached)
    if args.config:
        global UPSTREAM_BASE, UPSTREAM_API_KEY, UPSTREAM_AUTH_TYPE
        global RELAY_PORT, MAX_CONCURRENT_UPSTREAM, MODEL_FILTER_PATTERN, LOG_LEVEL
        global PROXY_LIST_FILE, PROXY_LIST_ENV, _CONFIG_PATH
        global CONSECUTIVE_ERROR_THRESHOLD, PERMANENT_COOLDOWN_SECONDS
        _CONFIG_PATH = os.path.expanduser(args.config)
        _file_cfg = _load_config_file(_CONFIG_PATH)
        _merged = _merge_config(_file_cfg)
        UPSTREAM_BASE = str(_merged["UPSTREAM_BASE"]).rstrip("/")
        UPSTREAM_API_KEY = str(_merged["UPSTREAM_API_KEY"])
        UPSTREAM_AUTH_TYPE = str(_merged["UPSTREAM_AUTH_TYPE"]).lower()
        RELAY_PORT = int(_merged["RELAY_PORT"])
        MAX_CONCURRENT_UPSTREAM = int(_merged["MAX_CONCURRENT_UPSTREAM"])
        MODEL_FILTER_PATTERN = str(_merged["MODEL_FILTER_PATTERN"])
        LOG_LEVEL = str(_merged["LOG_LEVEL"]).upper()
        PROXY_LIST_FILE = os.environ.get("PROXY_LIST", str(_merged.get("PROXY_LIST", "")))
        PROXY_LIST_ENV = os.environ.get("PROXY_LIST_ENV", str(_merged.get("PROXY_LIST_ENV", "")))
        CONSECUTIVE_ERROR_THRESHOLD = int(os.environ.get("CONSECUTIVE_ERROR_THRESHOLD",
            str(_merged.get("CONSECUTIVE_ERROR_THRESHOLD", 3))))
        PERMANENT_COOLDOWN_SECONDS = int(os.environ.get("PERMANENT_COOLDOWN_SECONDS",
            str(_merged.get("PERMANENT_COOLDOWN_SECONDS", 86400))))

    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=RELAY_PORT,
        log_level=LOG_LEVEL.lower(),
        reload=False,
    )


if __name__ == "__main__":
    main()
