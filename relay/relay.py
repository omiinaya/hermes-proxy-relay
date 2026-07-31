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
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
import json
import logging
import os
import re
import sys
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
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
    avg_latency_ms: float = 0.0      # moving average response time
    last_latency_ms: float = 0.0     # last request latency
    latency_samples: int = 0         # number of latency samples collected


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

    def record_latency(self, proxy: ProxyEntry, latency_ms: float):
        """Record a latency sample for the proxy (moving average)."""
        with self._lock:
            proxy.last_latency_ms = latency_ms
            proxy.latency_samples += 1
            n = proxy.latency_samples
            proxy.avg_latency_ms = (
                (proxy.avg_latency_ms * (n - 1) + latency_ms) / n
            )

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
                        "avg_latency_ms": round(p.avg_latency_ms, 1) if p.latency_samples > 0 else None,
                        "last_latency_ms": round(p.last_latency_ms, 1) if p.latency_samples > 0 else None,
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
                "avg_latency_ms": round(
                    sum(p.avg_latency_ms * p.latency_samples for p in self._proxies)
                    / max(sum(p.latency_samples for p in self._proxies), 1), 1
                ) if any(p.latency_samples > 0 for p in self._proxies) else 0.0,
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
    "ADMIN_API_KEY": "",
    "MAX_REQUEST_RETRIES": 3,
    "PROXY_HEALTH_CHECK_INTERVAL": 60,
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
ADMIN_API_KEY = str(os.environ.get("ADMIN_API_KEY", str(_merged.get("ADMIN_API_KEY", ""))))
MAX_REQUEST_RETRIES = int(os.environ.get("MAX_REQUEST_RETRIES",
    str(_merged.get("MAX_REQUEST_RETRIES", 3))))
PROXY_HEALTH_CHECK_INTERVAL = int(os.environ.get("PROXY_HEALTH_CHECK_INTERVAL",
    str(_merged.get("PROXY_HEALTH_CHECK_INTERVAL", 60))))

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
# Byte-level stream detection: matches {"stream": true} with any JSON
# whitespace between key, colon, and value. Requires a JSON delimiter
# (comma/brace/bracket) after `true` so `"stream": "true-string"` doesn't
# false-positive. Note: works on the lowercased body.
_STREAM_RE = re.compile(rb'"stream"\s*:\s*true(?=\s*[,}\]])')
_request_count = {"total": 0, "ok": 0, "errors": 0}
_request_lock = asyncio.Lock()
MODELS_CACHE: list[dict] = []
MODELS_CACHE_UPDATED: float = 0.0  # time.monotonic() of last refresh
MODELS_CACHE_TTL: float = 300.0   # refresh every 5 minutes

# Shared httpx client pool (one client per proxy URL, for non-streaming requests)
# OrderedDict so the LRU order is maintained (move_to_end on reuse).
_client_pool: dict[str, httpx.AsyncClient] = OrderedDict()
_client_pool_lock = asyncio.Lock()
_CLIENT_POOL_MAX = 100  # max concurrent clients to keep alive
_START_TIME: float = time.monotonic()
_stream_shutdown_event = asyncio.Event()
_PROXY_HEALTH_TASK: asyncio.Task | None = None  # background health checker

# Version — single source of truth
VERSION = "1.2.0"

# Simple in-memory rate limiter for admin endpoints
_admin_rate_hits: dict[str, list[float]] = defaultdict(list)
_admin_rate_lock = asyncio.Lock()
_ADMIN_RATE_LIMIT = 20    # max requests
_ADMIN_RATE_WINDOW = 60   # per 60 seconds
_ADMIN_RATE_MAX_IPS = 1000  # prune stale IP entries above this many


def _load_proxies_from_file(path: str) -> list[str]:
    """Load proxy URLs from a text file (one per line)."""
    proxies = []
    try:
        with open(os.path.expanduser(path)) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if _validate_proxy_url(line):
                        proxies.append(line)
                    else:
                        logger.warning(f"Skipping invalid proxy URL: {line}")
        logger.info(f"Loaded {len(proxies)} proxies from {path}")
    except Exception as e:
        logger.error(f"Failed to load proxies from {path}: {e}")
    return proxies


def _load_proxies_from_env(env_val: str) -> list[str]:
    """Load proxy URLs from comma-separated env var."""
    proxies = []
    for u in env_val.split(","):
        u = u.strip()
        if u:
            if _validate_proxy_url(u):
                proxies.append(u)
            else:
                logger.warning(f"Skipping invalid proxy URL from env: {u}")
    logger.info(f"Loaded {len(proxies)} proxies from PROXY_LIST_ENV")
    return proxies


def _validate_proxy_url(url: str) -> bool:
    """Basic proxy URL validation. Accepts socks5://, socks5h://, http://, https://."""
    if not url or len(url) > 500:
        return False
    import re as _re
    pattern = _re.compile(
        r'^(socks5|socks5h|http|https)://'
        r'([^:@/]+:[^:@/]+@)?'  # optional user:pass
        r'[a-zA-Z0-9.-]+'       # hostname
        r'(:\d{1,5})?'          # optional port
        r'(/.*)?$',             # optional path
    )
    return bool(pattern.match(url))


def _init_pool():
    proxies = []
    if PROXY_LIST_FILE:
        proxies = _load_proxies_from_file(PROXY_LIST_FILE)
    if not proxies and PROXY_LIST_ENV:
        proxies = _load_proxies_from_env(PROXY_LIST_ENV)
    if not proxies:
        logger.warning("No proxies configured — relay will return 503 for all requests")
    # Deduplicate — duplicate URLs would create duplicate pool entries that
    # waste slots and get tried twice in the retry loop.
    seen: set[str] = set()
    unique = []
    for p in proxies:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    if len(unique) != len(proxies):
        logger.warning(f"Deduplicated {len(proxies) - len(unique)} duplicate proxy URL(s)")
    pool.reload(unique)


async def _auto_star():
    """If GITHUB_TOKEN is set, auto-star omiinaya/hermes-proxy-relay.

    Skips if the token owner is the repo author (omiinaya) or if the
    repo is already starred. Runs once at startup. Silent on failure.
    """
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "hermes-proxy-relay/1.0",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Get authenticated user
            resp = await client.get("https://api.github.com/user", headers=headers)
            if resp.status_code != 200:
                logger.debug(f"Auto-star: failed to get user ({resp.status_code})")
                return
            login = resp.json().get("login", "").lower()

            # Skip if the token owner is the repo author
            if login == "omiinaya":
                logger.debug("Auto-star: token owner is repo author — skipping")
                return

            # Check if already starred
            resp = await client.get(
                "https://api.github.com/user/starred/omiinaya/hermes-proxy-relay",
                headers=headers,
            )
            if resp.status_code == 204:
                logger.debug("Auto-star: already starred — skipping")
                return

            # Star the repo
            if resp.status_code == 404:
                resp = await client.put(
                    "https://api.github.com/user/starred/omiinaya/hermes-proxy-relay",
                    headers=headers,
                )
                if resp.status_code == 204:
                    logger.info("⭐ Auto-starred omiinaya/hermes-proxy-relay (thanks!)")
                else:
                    logger.debug(f"Auto-star: PUT returned {resp.status_code}")
    except Exception as e:
        logger.debug(f"Auto-star: {type(e).__name__}: {e}")


def _update_models_cache(models: list[dict]):
    global MODELS_CACHE, MODELS_CACHE_UPDATED
    MODELS_CACHE = models
    MODELS_CACHE_UPDATED = time.monotonic()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Proxy helpers                                                 ║
# ╚══════════════════════════════════════════════════════════════════╝

async def _get_client(proxy_url: str) -> httpx.AsyncClient:
    """Get a shared httpx client for the given proxy URL.

    Clients are reused across requests for connection pooling.
    Only used for non-streaming requests — streaming gets dedicated clients.
    Pool is capped at _CLIENT_POOL_MAX — least-recently-used clients evicted
    first (true LRU: reusing a client moves it to the back of the order).
    """
    async with _client_pool_lock:
        client = _client_pool.get(proxy_url)
        if client is None:
            # If pool is at cap, evict the least-recently-used client
            if len(_client_pool) >= _CLIENT_POOL_MAX:
                evict_url, evict_client = next(iter(_client_pool.items()))
                try:
                    await evict_client.aclose()
                except Exception:
                    pass
                del _client_pool[evict_url]
                logger.debug(f"Evicted client for {evict_url} (pool at cap)")

            transport = httpx.AsyncHTTPTransport(proxy=proxy_url)
            client = httpx.AsyncClient(
                transport=transport,
                timeout=httpx.Timeout(60.0),
            )
            _client_pool[proxy_url] = client
        else:
            # LRU: re-inserting moves this URL to the back of the dict order
            # so it's evicted only after less-recently-used clients.
            _client_pool.move_to_end(proxy_url)
        return client


async def _make_streaming_client(proxy_url: str) -> httpx.AsyncClient:
    """Create a dedicated client for streaming (generator-owned lifecycle)."""
    transport = httpx.AsyncHTTPTransport(proxy=proxy_url)
    return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(60.0))


async def _close_all_clients():
    """Close all shared httpx clients (call on shutdown/pool reload)."""
    async with _client_pool_lock:
        for url, client in _client_pool.items():
            try:
                await client.aclose()
            except Exception:
                pass
        _client_pool.clear()


async def _prune_client_pool(active_urls: set[str]):
    """Close shared clients for proxies no longer in the pool.

    Called after a proxy list reload — removed proxies shouldn't keep
    their pooled connections alive.
    """
    async with _client_pool_lock:
        stale = [url for url in _client_pool if url not in active_urls]
        for url in stale:
            try:
                await _client_pool[url].aclose()
            except Exception:
                pass
            del _client_pool[url]
        if stale:
            logger.info(f"Pruned {len(stale)} pooled client(s) for removed proxies")


async def _proxy_health_check():
    """Background task: periodically test each proxy's connectivity.

    Attempts a connection through each proxy to verify it's alive.
    Dead proxies are marked as permanently failed — BUT only if at
    least one other proxy succeeded in the same sweep. If every proxy
    fails at once, the health target (httpbin.org) is likely down or
    the network is partitioned — marking all proxies dead would be
    wrong, so they're left alive and a warning is logged.
    """
    while True:
        try:
            await asyncio.sleep(PROXY_HEALTH_CHECK_INTERVAL)
            if pool.total == 0:
                continue

            healthy = 0
            failures: list[tuple[ProxyEntry, str]] = []
            for entry in list(pool._proxies):
                if entry.permanently_dead:
                    continue
                try:
                    transport = httpx.AsyncHTTPTransport(proxy=entry.url)
                    async with httpx.AsyncClient(
                        transport=transport, timeout=httpx.Timeout(10.0)
                    ) as test_client:
                        resp = await test_client.get(
                            "http://httpbin.org/ip", timeout=10.0
                        )
                        if resp.status_code < 500:
                            healthy += 1
                        else:
                            failures.append((entry, "Health check returned 5xx"))
                except Exception:
                    failures.append((entry, "Health check connection failed"))

            if failures and healthy == 0:
                # Everything failed — the health target is probably down,
                # not the proxies. Don't nuke the pool.
                logger.warning(
                    f"Health check: ALL {len(failures)} proxies failed — "
                    f"health target may be unreachable; leaving proxies alive"
                )
            elif failures:
                for entry, reason in failures:
                    pool.record_permanent_failure(entry, reason=reason)
                    logger.warning(
                        f"Health check: proxy {entry.url} — "
                        f"marked permanently unavailable ({reason})"
                    )
                logger.info(
                    f"Health check: {healthy} healthy, {len(failures)} failed "
                    f"({pool.available_count}/{pool.total} available)"
                )
            elif healthy:
                logger.info(
                    f"Health check: {healthy} healthy "
                    f"({pool.available_count}/{pool.total} available)"
                )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Health check error: {e}")


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
        # Strip relay-managed headers so they never reach the upstream:
        # content-length (recomputed by httpx), host (upstream's own),
        # connection (transport-managed), accept-encoding (we negotiate),
        # x-admin-key (relay's own admin auth — must not leak upstream).
        if lkey in ("content-length", "host", "connection", "accept-encoding", "x-admin-key"):
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


async def _check_admin_rate_limit(ip: str) -> bool:
    """Check if IP has exceeded the admin rate limit. Returns True if allowed."""
    now = time.monotonic()
    async with _admin_rate_lock:
        # Bounded memory: if many distinct IPs have hit admin endpoints,
        # prune stale entries for ALL of them. Prevents unbounded growth
        # from a spoofed/fan-out client flood.
        if len(_admin_rate_hits) > _ADMIN_RATE_MAX_IPS:
            cutoff = now - _ADMIN_RATE_WINDOW
            stale_ips = [
                k for k, v in _admin_rate_hits.items()
                if not any(t > cutoff for t in v)
            ]
            for k in stale_ips:
                del _admin_rate_hits[k]

        hits = _admin_rate_hits[ip]
        # Prune old entries
        cutoff = now - _ADMIN_RATE_WINDOW
        _admin_rate_hits[ip] = [t for t in hits if t > cutoff]
        if len(_admin_rate_hits[ip]) >= _ADMIN_RATE_LIMIT:
            return False
        _admin_rate_hits[ip].append(now)
        return True


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

    if not UPSTREAM_BASE:
        logger.error("UPSTREAM_BASE is empty — cannot proxy request")
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "message": "Upstream base URL is not configured. Set UPSTREAM_BASE.",
                    "type": "configuration_error",
                    "code": "upstream_not_configured",
                }
            },
        )

    upstream_url = f"{UPSTREAM_BASE}{path}"
    if query_string:
        upstream_url += f"?{query_string}"

    req_headers = _build_headers(dict(headers))
    is_stream = False
    if body:
        body_lower = body.lower()
        is_stream = _STREAM_RE.search(body_lower) is not None

    # Streaming requests get one attempt with a dedicated client
    if is_stream:
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

        async with semaphore:
            streaming_client = None
            try:
                streaming_client = await _make_streaming_client(proxy_entry.url)
                return await _proxy_stream(streaming_client, method, upstream_url,
                                           req_headers, body, proxy_entry)
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                pool.record_timeout(proxy_entry)
                async with _request_lock:
                    _request_count["errors"] += 1
                if streaming_client is not None:
                    await streaming_client.aclose()
                logger.warning(f"Stream proxy {proxy_entry.url} connect failed: {e}")
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
                if streaming_client is not None:
                    await streaming_client.aclose()
                logger.error(f"Unexpected stream error on {proxy_entry.url}: {e}")
                return JSONResponse(
                    status_code=502,
                    content={
                        "error": {
                            "message": f"Upstream error: {e}",
                            "type": "upstream_error",
                        }
                    },
                )

    # Non-streaming: retry with different proxies on transient failure
    last_error = None
    attempt = 0
    tried_urls: set[str] = set()

    while attempt < MAX_REQUEST_RETRIES:
        proxy_entry = pool.next()
        if proxy_entry is None:
            if last_error:
                # All proxies cooled during retries — return the last error
                break
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

        # Skip if we already tried this proxy — all proxies exhausted
        if proxy_entry.url in tried_urls:
            if len(tried_urls) >= pool.total:
                # Every proxy has been tried and returned retryable errors —
                # don't spin forever when MAX_REQUEST_RETRIES > pool size
                logger.warning(
                    f"All {pool.total} proxies tried without success, "
                    f"stopping retry loop"
                )
                break
            continue
        tried_urls.add(proxy_entry.url)
        attempt += 1

        async with semaphore:
            try:
                client = await _get_client(proxy_entry.url)
                resp = await _proxy_single(client, method, upstream_url,
                                          req_headers, body, proxy_entry)
                # Success or final error (4xx from upstream) — return immediately
                if resp.status_code < 500 or resp.status_code == 429:
                    return resp
                # 5xx upstream error — retryable
                last_error = resp
                logger.warning(
                    f"Upstream 5xx on {proxy_entry.url} "
                    f"({resp.status_code}), retrying... "
                    f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                )
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                pool.record_timeout(proxy_entry)
                async with _request_lock:
                    _request_count["errors"] += 1
                last_error = JSONResponse(
                    status_code=502,
                    content={
                        "error": {
                            "message": f"Proxy connection failed: {e}",
                            "type": "proxy_error",
                            "code": "proxy_connect_failed",
                        }
                    },
                )
                logger.warning(
                    f"Proxy {proxy_entry.url} connect failed: {e} "
                    f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                )
            except Exception as e:
                pool.record_timeout(proxy_entry)
                async with _request_lock:
                    _request_count["errors"] += 1
                last_error = JSONResponse(
                    status_code=502,
                    content={
                        "error": {
                            "message": f"Upstream error: {e}",
                            "type": "upstream_error",
                        }
                    },
                )
                logger.warning(
                    f"Unexpected error on {proxy_entry.url}: {e} "
                    f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                )

    # All retries exhausted
    if last_error:
        logger.error(
            f"Request failed after {attempt}/{MAX_REQUEST_RETRIES} attempts "
            f"across {len(tried_urls)} proxies"
        )
        return last_error

    # If no retries happened and still no proxy (all cooling mid-loop)
    logger.warning("All proxies cooling after retry, returning 429")
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


async def _proxy_single(client, method, url, headers, body, proxy_entry) -> Response:
    """Single-shot proxy: forward request, decompress response, relay headers.

    Strips Content-Encoding, Transfer-Encoding, and Content-Length from
    response headers because httpx auto-decompresses gzip/deflate/brotli
    and the response body length changes.
    """
    t0 = time.monotonic()
    resp = await client.request(method, url, headers=headers, content=body)
    latency_ms = (time.monotonic() - t0) * 1000

    if resp.status_code == 429:
        retry_after = _parse_retry_after(resp.headers)
        pool.record_429(proxy_entry, retry_after)
        async with _request_lock:
            _request_count["errors"] += 1
        logger.warning(f"429 on {proxy_entry.url} — cooling for {retry_after}s")
    elif resp.status_code >= 400:
        async with _request_lock:
            _request_count["errors"] += 1
        # Only cool the proxy for proxy-related 4xx (407 proxy auth,
        # 408 request timeout, 425 too early). Client errors (400/401/
        # 403/404/422...) are NOT the proxy's fault — relay them without
        # degrading the pool, otherwise a single bad client request
        # rotates through and cools every proxy.
        if resp.status_code in (407, 408, 425):
            pool.record_timeout(proxy_entry)
    else:
        pool.record_success(proxy_entry)
        async with _request_lock:
            _request_count["ok"] += 1

    # Record latency for non-429 success
    if resp.status_code < 400:
        pool.record_latency(proxy_entry, latency_ms)

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
        await client.aclose()
        return Response(
            content=error_body,
            status_code=429,
            headers=resp_headers,
            media_type="application/json",
        )

    if resp.status_code >= 400:
        # Only cool for proxy-related 4xx (see _proxy_single for rationale)
        if resp.status_code in (407, 408, 425):
            pool.record_timeout(proxy_entry)
        async with _request_lock:
            _request_count["errors"] += 1
        error_body = await resp.aread()
        await resp.aclose()
        await client.aclose()
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
                if _stream_shutdown_event.is_set():
                    yield json.dumps({
                        "error": {"message": "Server shutting down", "type": "shutdown_error"}
                    }).encode()
                    return
                yield chunk
        except Exception as e:
            pool.record_timeout(proxy_entry)
            async with _request_lock:
                _request_count["errors"] += 1
            logger.error(f"Stream error on {proxy_entry.url}: {type(e).__name__}: {e}")
            yield json.dumps({
                "error": {"message": f"Stream error: {e}", "type": "stream_error"}
            }).encode()
        finally:
            await resp.aclose()
            await client.aclose()

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
    global _START_TIME, _PROXY_HEALTH_TASK
    _START_TIME = time.monotonic()
    # Reset shutdown flag — a restarted process must not inherit a
    # set event from a previous run (otherwise all streams error out).
    _stream_shutdown_event.clear()

    # Warn if no API key configured
    if not UPSTREAM_API_KEY:
        logger.warning("UPSTREAM_API_KEY is empty — requests will fail authentication")
    if not UPSTREAM_BASE:
        logger.warning("UPSTREAM_BASE is empty — relay has no upstream target")
    if not PROXY_LIST_FILE and not PROXY_LIST_ENV:
        logger.warning("No proxy list configured — relay will 429/503 all requests")

    _init_pool()
    asyncio.create_task(_auto_star())
    logger.info(
        f"Proxy Relay started on :{RELAY_PORT} "
        f"\u2192 {UPSTREAM_BASE} "
        f"({pool.total} proxies, semaphore={MAX_CONCURRENT_UPSTREAM})"
    )

    # Start background health checker
    _PROXY_HEALTH_TASK = asyncio.create_task(_proxy_health_check())

    yield

    logger.info("Proxy Relay shutting down")
    _stream_shutdown_event.set()
    # Drain window: give in-flight streams a chance to observe the
    # shutdown signal before closing clients. Configurable so tests
    # (and impatient operators) can skip the wait.
    shutdown_drain = int(os.environ.get("RELAY_SHUTDOWN_DRAIN_SECONDS", "5"))
    if shutdown_drain > 0:
        await asyncio.sleep(shutdown_drain)
    if _PROXY_HEALTH_TASK is not None:
        _PROXY_HEALTH_TASK.cancel()
        try:
            await _PROXY_HEALTH_TASK
        except asyncio.CancelledError:
            pass
    await _close_all_clients()


app = FastAPI(
    title="Hermes Proxy Relay",
    version=VERSION,
    lifespan=lifespan,
)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Request logging middleware                                     ║
# ╚══════════════════════════════════════════════════════════════════╝


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log structured request info with timing."""
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = (time.monotonic() - start) * 1000
    logger.info(
        f"{request.method} {request.url.path} "
        f"\u2192 {response.status_code} "
        f"({duration_ms:.0f}ms)"
    )
    return response


# ╔══════════════════════════════════════════════════════════════════╗
# ║  CORS middleware (allow web clients)                            ║
# ╚══════════════════════════════════════════════════════════════════╝


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Admin auth middleware (optional) ────────────────────────────
@app.middleware("http")
async def admin_auth(request: Request, call_next):
    """If ADMIN_API_KEY is set, require X-Admin-Key header on /admin/* routes."""
    if request.url.path.startswith("/admin/") and ADMIN_API_KEY:
        provided = request.headers.get("x-admin-key", "")
        if provided != ADMIN_API_KEY:
            return JSONResponse(
                status_code=403,
                content={"error": "Invalid or missing admin key. Set X-Admin-Key header."},
            )
    return await call_next(request)


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
        "uptime_seconds": int(time.monotonic() - _START_TIME),
        "version": VERSION,
        "shared_clients": len(_client_pool),
    }


@app.get("/v1/models")
async def list_models():
    if not UPSTREAM_BASE:
        return {"object": "list", "data": []}

    # Check cache freshness
    now = time.monotonic()
    if MODELS_CACHE and (now - MODELS_CACHE_UPDATED) < MODELS_CACHE_TTL:
        return {"object": "list", "data": list(MODELS_CACHE)}

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
    except Exception as e:
        logger.warning(f"Failed to refresh models: {e}")

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


@app.get("/admin/upstream-health")
async def admin_upstream_health(request: Request):
    """Check if the upstream API is reachable through the relay.

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _check_admin_rate_limit(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    if not UPSTREAM_BASE:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": "No upstream configured"},
        )

    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            headers = {}
            if UPSTREAM_AUTH_TYPE == "x-api-key":
                headers["x-api-key"] = UPSTREAM_API_KEY
            else:
                headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"
            resp = await client.get(f"{UPSTREAM_BASE}/models", headers=headers)
            latency_ms = (time.monotonic() - t0) * 1000
            return {
                "status": "ok" if resp.status_code < 500 else "degraded",
                "upstream": UPSTREAM_BASE,
                "upstream_status": resp.status_code,
                "latency_ms": round(latency_ms, 1),
                "models_count": len(resp.json().get("data", [])) if resp.status_code == 200 else 0,
            }
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "upstream": UPSTREAM_BASE,
                "error": str(e),
                "latency_ms": round(latency_ms, 1),
            },
        )


@app.post("/admin/clear-cooldowns")
async def admin_clear_cooldowns(request: Request):
    """Reset ALL proxies to available (clears temporary AND permanent cooldowns).

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _check_admin_rate_limit(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
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
    """Reset a single proxy by URL. Body: {\"url\": \"socks5://...\"}

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _check_admin_rate_limit(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
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
async def admin_reload_proxies(request: Request):
    """Reload the proxy list from the configured file/env.

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _check_admin_rate_limit(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    _init_pool()
    await _prune_client_pool({p.url for p in pool._proxies})
    logger.info(f"Proxy list reloaded (admin): {pool.total} proxies")
    return {
        "status": "ok",
        "message": "Proxy list reloaded",
        "proxies_total": pool.total,
        "available": pool.available_count,
    }


@app.post("/admin/reset-by-errors")
async def admin_reset_by_errors(request: Request):
    """Reset all proxies that have been permanently failed.
    Body: {\"min_consecutive\": 3} (optional, defaults to CONSECUTIVE_ERROR_THRESHOLD)

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _check_admin_rate_limit(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    try:
        data = await request.json() if request.headers.get("content-length") != "0" else {}
    except Exception:
        data = {}
    min_errs = data.get("min_consecutive", CONSECUTIVE_ERROR_THRESHOLD)
    reset_count = pool.reset_by_errors(min_errs)
    logger.info(f"Reset {reset_count} permanently-failed proxies (admin)")
    return {"status": "ok", "message": f"Reset {reset_count} proxies"}


def _run_config_check():
    """Validate configuration without starting the server.

    Exits non-zero if critical config (upstream, proxies) is missing.
    Prints a report with warnings and errors.
    """
    problems = []

    def report(level: str, msg: str):
        if level == "ERROR":
            problems.append(msg)
            print(f"  ✗ {msg}")
        else:
            print(f"  ⚠ {msg}")

    print(f"Hermes Proxy Relay v{VERSION} — configuration check")
    print("")

    if not UPSTREAM_BASE:
        report("ERROR", "UPSTREAM_BASE is empty — relay cannot proxy requests")
    else:
        print(f"  ✓ UPSTREAM_BASE: {UPSTREAM_BASE}")

    if not UPSTREAM_API_KEY:
        report("WARNING", "UPSTREAM_API_KEY is empty — upstream auth will fail")

    if UPSTREAM_AUTH_TYPE not in ("bearer", "x-api-key"):
        report("ERROR", f"Invalid UPSTREAM_AUTH_TYPE: {UPSTREAM_AUTH_TYPE!r} (expected bearer or x-api-key)")

    proxies = []
    if PROXY_LIST_FILE:
        proxies = _load_proxies_from_file(PROXY_LIST_FILE)
        print(f"  ✓ Proxy file: {PROXY_LIST_FILE} ({len(proxies)} proxies)")
    if not proxies and PROXY_LIST_ENV:
        proxies = _load_proxies_from_env(PROXY_LIST_ENV)
        print(f"  ✓ PROXY_LIST_ENV: {len(proxies)} proxies")
    if not proxies:
        # Warning (not error) — the relay still serves /health and /admin,
        # just 429s proxied requests until a proxy list is configured.
        report("WARNING", "No proxies configured (PROXY_LIST / PROXY_LIST_ENV) — relay will 429/503 all requests")

    print("")
    if problems:
        print(f"Configuration has {len(problems)} error(s) — fix before starting.")
        sys.exit(1)
    print("Configuration OK.")


def main():
    """Entry point. Supports --config <path> for config file override."""
    import argparse
    parser = argparse.ArgumentParser(description="Hermes Proxy Relay")
    parser.add_argument(
        "--version", "-V",
        action="store_true",
        help="Show version and exit",
    )
    parser.add_argument(
        "--config", "-c",
        default=os.environ.get("RELAY_CONFIG", ""),
        help="Path to JSON config file (default: ~/.hermes/proxy-relay/config.json)",
    )
    parser.add_argument(
        "--check", "-C",
        action="store_true",
        help="Validate configuration and exit without starting the server",
    )
    args = parser.parse_args()

    if args.version:
        print(f"Hermes Proxy Relay v{VERSION}")
        sys.exit(0)

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
        ADMIN_API_KEY = str(os.environ.get("ADMIN_API_KEY", str(_merged.get("ADMIN_API_KEY", ""))))  # noqa: F841

    if args.check:
        _run_config_check()
        sys.exit(0)

    import uvicorn

    # Graceful shutdown on SIGTERM/SIGINT
    try:
        import signal as _signal
        _signal.signal(_signal.SIGTERM, lambda *_: logger.info("SIGTERM received, shutting down...") or sys.exit(0))
        _signal.signal(_signal.SIGINT, lambda *_: logger.info("SIGINT received, shutting down...") or sys.exit(0))
    except Exception:
        pass

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=RELAY_PORT,
        log_level=LOG_LEVEL.lower(),
        reload=False,
    )


if __name__ == "__main__":
    main()
