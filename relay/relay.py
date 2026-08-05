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
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timezone
from urllib.parse import unquote_plus
import json
import logging
import os
import re
import secrets
import sys
import threading
import time
import weakref
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


def _mask_proxy_url(url: str) -> str:
    """Redact credentials from a proxy URL for display/logging.

    `socks5://user:pass@host:1080` → `socks5://***@host:1080`.
    Scheme-less URLs (`user:pass@host:1080`) are also masked — the
    rpartition handles both, so credentials never survive into health
    responses, logs, or MCP output.
    """
    if not url:
        return url
    if "@" in url:
        # Mask everything before the LAST @, preserving the scheme when
        # present (socks5://***@host:1080) — and handling scheme-less
        # URLs (user:pass@host:1080 → ***@host:1080) that would otherwise
        # fall through the old partition("://") check and leak raw
        # credentials into /health, logs, and MCP output.
        scheme, sep, rest = url.partition("://")
        if sep and rest and "@" in rest:
            return f"{scheme}://***@{rest.rpartition('@')[2]}"
        return f"***@{url.rpartition('@')[2]}"
    return url


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
            n = len(self._proxies)
            for _ in range(n):
                self._index = (self._index + 1) % n
                candidate = self._proxies[self._index]
                # permanently_dead proxies stay out of rotation until
                # explicitly revived (admin reset) or the health checker
                # verifies them — otherwise a real client request pays
                # the rediscovery cost (connect timeout + retry) every
                # time the 24h cooldown expires, for a proxy the health
                # checker already wrote off.
                if candidate.cooldown_until <= now and not candidate.permanently_dead:
                    return self._maybe_skip_slow(now, n)
            return None

    def _maybe_skip_slow(self, now: float, n: int) -> ProxyEntry:
        """Latency-aware selection: prefer a faster proxy over a slow one.

        When LATENCY_SKIP_THRESHOLD_MS > 0 and the round-robin candidate is
        measurably slower than the threshold, scan for a faster available
        proxy (unknown-latency proxies count as fast — no data, no bias).
        Falls back to the candidate when nothing faster exists. The scan is
        bounded to the pool size and only runs when the knob is enabled
        (default 0 = pure round-robin, zero overhead).
        """
        candidate = self._proxies[self._index]
        if LATENCY_SKIP_THRESHOLD_MS <= 0 or candidate.latency_samples == 0:
            return candidate
        if candidate.avg_latency_ms <= LATENCY_SKIP_THRESHOLD_MS:
            return candidate
        start = self._index
        for _ in range(n - 1):
            self._index = (self._index + 1) % n
            alt = self._proxies[self._index]
            if alt.cooldown_until <= now and not alt.permanently_dead:
                if alt.latency_samples == 0 or alt.avg_latency_ms <= LATENCY_SKIP_THRESHOLD_MS:
                    return alt
        # No faster alternative — restore the round-robin position and serve
        # the slow proxy rather than failing the request.
        self._index = start
        return candidate

    def record_429(self, proxy: ProxyEntry, retry_after: int = 60):
        now = time.monotonic()
        # Guard against absurd/hostile Retry-After values. A huge int
        # (e.g. `Retry-After: 999…9` from a misconfigured CDN) would
        # overflow the float addition below and take the whole request
        # down; a year-long cooldown would remove the proxy from rotation
        # forever. Clamp to a sane upper bound.
        try:
            cooldown = max(int(retry_after), 10)
        except (ValueError, TypeError):
            cooldown = 60
        cooldown = min(cooldown, MAX_RETRY_AFTER_SECONDS)
        with self._lock:
            proxy.cooldown_until = now + cooldown
            proxy.consecutive_429 += 1
            proxy.total_429 += 1
            proxy.last_error = f"429 rate limited (cooling {cooldown}s)"
            self._all_time_429 += 1

    def record_transient(self, proxy: ProxyEntry, message: str = "Transient failure"):
        """Cool the proxy briefly for a transient error WITHOUT counting it
        toward permanent death.

        A slow/flaky upstream (httpx.ReadTimeout, RemoteProtocolError)
        through a healthy proxy is not the proxy's fault — permanently
        deactivating a good proxy because the upstream occasionally
        stalls would disable the whole pool. Only connect-level failures
        (proxy-attributable) increment consecutive_errors.
        """
        now = time.monotonic()
        with self._lock:
            proxy.cooldown_until = now + 30
            proxy.last_error = f"{message} (30s cooldown, not counted)"

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
                    f"Proxy {_mask_proxy_url(proxy.url)} MARKED PERMANENTLY UNAVAILABLE "
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
                f"Proxy {_mask_proxy_url(proxy.url)} PERMANENTLY DEACTIVATED: {proxy.last_error}"
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
                        # Never expose user:pass@ credentials — /health is
                        # unauthenticated and proxies.txt may contain them.
                        "proxy": _mask_proxy_url(p.url),
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
        """Reset all permanently-dead proxies. Returns count.

        `min_consecutive` is retained for API compatibility but a proxy
        is reset whenever it is permanently_dead: the health-check kill
        path (`record_permanent_failure`) marks death with
        consecutive_errors=1, which would never reach the old >= threshold
        and silently left health-killed proxies unrecoverable.
        """
        now = time.monotonic()
        count = 0
        with self._lock:
            for p in self._proxies:
                if p.permanently_dead:
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
    # Bounded backlog for the concurrency semaphore. When this many
    # requests are ALREADY waiting for a permit, further requests fail
    # fast with 503 instead of queueing — bursts drain up to the cap,
    # then excess load is shed immediately. 0 = unlimited (old behavior).
    "MAX_QUEUED_REQUESTS": 100,
    # Hold the concurrency permit for the WHOLE stream (true) or only for
    # connection setup (false). Holding it caps concurrent streams at
    # MAX_CONCURRENT_UPSTREAM and protects the upstream queue from
    # saturation (observed: opencode-zen free tier 503s "queue is full"
    # when too many parallel streams hit it). Setting false is an
    # opt-in escape hatch for max throughput — it trades upstream-queue
    # safety for unbounded stream concurrency.
    "HOLD_PERMIT_FOR_STREAM": "true",
    # Max simultaneous probes per health-check sweep. A 250-proxy pool
    # must neither serialize 250 x probe-time (minutes per sweep) nor
    # fire 250 concurrent upstream requests (rate-limit bait).
    "HEALTH_CHECK_CONCURRENCY": 20,
    # uvicorn worker processes. 1 = single process (pool state shared in
    # memory). >1 = each worker has its OWN pool/cooldown/client state —
    # cooldowns are NOT shared across workers (opt-in scaling lever).
    "RELAY_WORKERS": 1,
    # Inbound connection caps (passed to uvicorn). 0 = uvicorn default
    # (unlimited concurrency, backlog 2048). Guards against FD exhaustion
    # from a burst/slow-loris flood BEFORE the semaphore backlog logic runs.
    "RELAY_MAX_CONNECTIONS": 0,
    "RELAY_BACKLOG": 0,
    # Upstream timeouts (seconds). Connect and read are decoupled: a slow
    # first-token upstream or a stream with a long inter-token gap must not
    # be killed by a single fixed timeout. Applies per-chunk between bytes
    # on streams, per-full-read on single-shot requests.
    "UPSTREAM_CONNECT_TIMEOUT": 15,
    "UPSTREAM_READ_TIMEOUT": 120,
    # How long a pooled client may sit IDLE before it is proactively closed
    # (stale-keep-alive prevention). A connection that the proxy/upstream
    # silently closed while idle is reaped BEFORE reuse instead of failing
    # on a dead socket and mis-attributing a healthy proxy as down.
    # 0 = disabled (never reap by age).
    "CLIENT_IDLE_TTL": 120,
    # Max upstream RESPONSE bytes accepted for single-shot requests.
    # Guards memory from a runaway upstream (bodies already capped via
    # MAX_BODY_SIZE; responses were not). 0 = unlimited.
    "MAX_RESPONSE_SIZE": 200 * 1024 * 1024,
    "MODEL_FILTER_PATTERN": ".*",
    "LOG_LEVEL": "INFO",
    "PROXY_LIST": "",
    "PROXY_LIST_ENV": "",
    "CONSECUTIVE_ERROR_THRESHOLD": 3,
    "PERMANENT_COOLDOWN_SECONDS": 86400,
    # Upper bound for Retry-After cooldowns (seconds) — clamps hostile/
    # absurd values so one misbehaving upstream can't remove a proxy
    # from rotation for years.
    "MAX_RETRY_AFTER_SECONDS": 3600,
    "ADMIN_API_KEY": "",
    "CLIENT_API_KEY": "",
    "MAX_REQUEST_RETRIES": 3,
    "SEMAPHORE_WAIT_SECONDS": 30.0,
    # Retry attempts wait only this long for a concurrency slot. The FIRST
    # attempt waits SEMAPHORE_WAIT_SECONDS (requests queueing for capacity
    # can wait); retries after a failure fail fast instead of stacking more
    # 30s waits on top of an already-failing request.
    "RETRY_SEMAPHORE_WAIT_SECONDS": 2.0,
    # Exponential backoff between retry attempts (seconds). Base × 2^(n-1),
    # capped at RETRY_BACKOFF_MAX. 0 = no backoff (immediate retries).
    "RETRY_BACKOFF_BASE": 0.1,
    "RETRY_BACKOFF_MAX": 1.0,
    # Latency-aware proxy selection. When > 0, a proxy whose measured
    # avg_latency_ms exceeds this is skipped in favor of a faster available
    # one (falling back to it only when nothing faster exists). 0 = pure
    # round-robin (default, preserves pre-1.7 behavior).
    "LATENCY_SKIP_THRESHOLD_MS": 0,
    # Log every non-/health request at INFO. Disable for minimum overhead
    # at very high request rates (the middleware timing/redaction is skipped).
    "RELAY_LOG_REQUESTS": "true",
    "PROXY_HEALTH_CHECK_INTERVAL": 60,
    "PROXY_HEALTH_CHECK_URL": "http://httpbin.org/ip",
    # Consecutive health-check failures before a proxy is permanently marked
    # dead. Guards against a proxy network that blocks the health target but
    # works fine for the real upstream.
    "HEALTH_FAIL_THRESHOLD": 3,
    # Max request body size in bytes. The relay reads bodies fully into
    # memory (needed for cross-proxy retries), so an unbounded body is a
    # memory-exhaustion risk on open relays. 100MB generously covers
    # vision requests with large base64 images.
    "MAX_BODY_SIZE": 100 * 1024 * 1024,
    # ── Smart auth switching ────────────────────────────────────────
    # Detects upstream auth-method changes (e.g. OpenCode Zen flipping
    # x-api-key → Bearer) and self-heals. ONLY a 401 counts as an auth
    # signal — 5xx/429/connection errors never trigger a switch. On N
    # consecutive 401s, alternate auth types are probed with the same
    # API key against /models; a candidate returning 200 twice is
    # adopted, the current request is retried with it, and the verified
    # type is persisted so restarts keep the fix.
    "AUTH_SWITCH_ENABLED": "true",
    "AUTH_SWITCH_CANDIDATES": "bearer,x-api-key",
    "AUTH_SWITCH_TRIGGER_THRESHOLD": 3,
    "AUTH_SWITCH_PROBE_SUCCESSES": 2,
    "AUTH_SWITCH_COOLDOWN_S": 300,
    "AUTH_SWITCH_MAX_PER_WINDOW": 3,
    "AUTH_SWITCH_WINDOW_S": 3600,
    "AUTH_STATE_PATH": "~/.hermes/proxy-relay/auth_state.json",
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
# See _DEFAULT_CONFIG for semantics.
MAX_QUEUED_REQUESTS = int(_merged["MAX_QUEUED_REQUESTS"])
HOLD_PERMIT_FOR_STREAM = str(_merged["HOLD_PERMIT_FOR_STREAM"]).lower() in ("1", "true", "yes", "on")
HEALTH_CHECK_CONCURRENCY = int(_merged["HEALTH_CHECK_CONCURRENCY"])
RELAY_WORKERS = int(_merged["RELAY_WORKERS"])
RELAY_MAX_CONNECTIONS = int(_merged["RELAY_MAX_CONNECTIONS"])
RELAY_BACKLOG = int(_merged["RELAY_BACKLOG"])
UPSTREAM_CONNECT_TIMEOUT = float(_merged["UPSTREAM_CONNECT_TIMEOUT"])
UPSTREAM_READ_TIMEOUT = float(_merged["UPSTREAM_READ_TIMEOUT"])
CLIENT_IDLE_TTL = float(_merged["CLIENT_IDLE_TTL"])
MAX_RESPONSE_SIZE = int(_merged["MAX_RESPONSE_SIZE"])
MODEL_FILTER_PATTERN = str(_merged["MODEL_FILTER_PATTERN"])
LOG_LEVEL = str(_merged["LOG_LEVEL"]).upper()
# NOTE: `or` (not bare os.environ.get) everywhere below — an env var set
# to "" must behave as UNSET, not as an override. Otherwise `ADMIN_API_KEY=`
# silently disables file-configured admin auth, and the numeric int() calls
# crash at startup with int("") ValueError.
PROXY_LIST_FILE = os.environ.get("PROXY_LIST") or str(_merged.get("PROXY_LIST", ""))
PROXY_LIST_ENV = os.environ.get("PROXY_LIST_ENV") or str(_merged.get("PROXY_LIST_ENV", ""))
CONSECUTIVE_ERROR_THRESHOLD = int(os.environ.get("CONSECUTIVE_ERROR_THRESHOLD") or
    str(_merged.get("CONSECUTIVE_ERROR_THRESHOLD", 3)))
PERMANENT_COOLDOWN_SECONDS = int(os.environ.get("PERMANENT_COOLDOWN_SECONDS") or
    str(_merged.get("PERMANENT_COOLDOWN_SECONDS", 86400)))
# Upper bound for Retry-After cooldowns (seconds). Clamps hostile/absurd
# values so a single misbehaving upstream can't remove a proxy from
# rotation for years.
MAX_RETRY_AFTER_SECONDS = int(os.environ.get("MAX_RETRY_AFTER_SECONDS") or
    str(_merged.get("MAX_RETRY_AFTER_SECONDS", 3600)))
ADMIN_API_KEY = str(os.environ.get("ADMIN_API_KEY") or _merged.get("ADMIN_API_KEY", ""))
# Optional client auth for /v1/* proxied requests. When set, clients must
# present it as `Authorization: Bearer <key>` or `X-API-Key: <key>`.
# Prevents the relay from acting as an open proxy that burns upstream
# credits when bound to a non-local interface.
CLIENT_API_KEY = str(os.environ.get("CLIENT_API_KEY") or _merged.get("CLIENT_API_KEY", ""))
MAX_REQUEST_RETRIES = int(os.environ.get("MAX_REQUEST_RETRIES") or
    str(_merged.get("MAX_REQUEST_RETRIES", 3)))
RETRY_SEMAPHORE_WAIT_SECONDS = float(os.environ.get("RETRY_SEMAPHORE_WAIT_SECONDS") or
    str(_merged.get("RETRY_SEMAPHORE_WAIT_SECONDS", 2.0)))
RETRY_BACKOFF_BASE = float(os.environ.get("RETRY_BACKOFF_BASE") or
    str(_merged.get("RETRY_BACKOFF_BASE", 0.1)))
RETRY_BACKOFF_MAX = float(os.environ.get("RETRY_BACKOFF_MAX") or
    str(_merged.get("RETRY_BACKOFF_MAX", 1.0)))
LATENCY_SKIP_THRESHOLD_MS = float(os.environ.get("LATENCY_SKIP_THRESHOLD_MS") or
    str(_merged.get("LATENCY_SKIP_THRESHOLD_MS", 0)))
RELAY_LOG_REQUESTS = str(os.environ.get("RELAY_LOG_REQUESTS") or
    str(_merged.get("RELAY_LOG_REQUESTS", "true"))).lower() in ("1", "true", "yes", "on")
# Max seconds a request waits for a concurrency slot before returning 503.
# Prevents clients hanging indefinitely when all semaphore slots are busy.
SEMAPHORE_WAIT_SECONDS = float(os.environ.get("SEMAPHORE_WAIT_SECONDS") or
    str(_merged.get("SEMAPHORE_WAIT_SECONDS", 30.0)))
PROXY_HEALTH_CHECK_INTERVAL = int(os.environ.get("PROXY_HEALTH_CHECK_INTERVAL") or
    str(_merged.get("PROXY_HEALTH_CHECK_INTERVAL", 60)))
# Target URL for background proxy health checks. Any reachable endpoint
# that returns <500 works; use something fast and reliable near your
# proxies (defaults to httpbin, a public service).
PROXY_HEALTH_CHECK_URL = str(os.environ.get("PROXY_HEALTH_CHECK_URL") or
    str(_merged.get("PROXY_HEALTH_CHECK_URL", "http://httpbin.org/ip")))
# Consecutive health-check failures before permanent death (see checker)
HEALTH_FAIL_THRESHOLD = int(os.environ.get("HEALTH_FAIL_THRESHOLD") or
    str(_merged.get("HEALTH_FAIL_THRESHOLD", 3)))
# Max request body size in bytes — bodies over this get 413 before being
# read into memory. Prevents memory exhaustion on open relays.
MAX_BODY_SIZE = int(os.environ.get("MAX_BODY_SIZE") or
    str(_merged.get("MAX_BODY_SIZE", 100 * 1024 * 1024)))

# ── Smart auth switching ────────────────────────────────────────────
# Detects upstream auth-method changes (e.g. OpenCode Zen flipping
# x-api-key → Bearer) and self-heals WITHOUT manual intervention. ONLY
# a 401 counts as an auth signal (the request REACHED upstream and was
# rejected for credentials) — 5xx (server issue), 429 (rate limit), and
# connection errors (proxy/network) never trigger a switch. On N
# consecutive 401s, alternate auth types are probed with the same API
# key against /models; a candidate returning 200 twice is adopted, the
# current request is retried with it, and the verified type is persisted
# so restarts keep the fix.
AUTH_SWITCH_ENABLED = str(os.environ.get("AUTH_SWITCH_ENABLED") or
    str(_merged.get("AUTH_SWITCH_ENABLED", "true"))).lower() in ("1", "true", "yes", "on")
AUTH_SWITCH_CANDIDATES = [c.strip().lower() for c in str(
    os.environ.get("AUTH_SWITCH_CANDIDATES") or
    str(_merged.get("AUTH_SWITCH_CANDIDATES", "bearer,x-api-key"))
).split(",") if c.strip()]
AUTH_SWITCH_TRIGGER_THRESHOLD = int(os.environ.get("AUTH_SWITCH_TRIGGER_THRESHOLD") or
    str(_merged.get("AUTH_SWITCH_TRIGGER_THRESHOLD", 3)))
AUTH_SWITCH_PROBE_SUCCESSES = int(os.environ.get("AUTH_SWITCH_PROBE_SUCCESSES") or
    str(_merged.get("AUTH_SWITCH_PROBE_SUCCESSES", 2)))
AUTH_SWITCH_COOLDOWN_S = int(os.environ.get("AUTH_SWITCH_COOLDOWN_S") or
    str(_merged.get("AUTH_SWITCH_COOLDOWN_S", 300)))
AUTH_SWITCH_MAX_PER_WINDOW = int(os.environ.get("AUTH_SWITCH_MAX_PER_WINDOW") or
    str(_merged.get("AUTH_SWITCH_MAX_PER_WINDOW", 3)))
AUTH_SWITCH_WINDOW_S = int(os.environ.get("AUTH_SWITCH_WINDOW_S") or
    str(_merged.get("AUTH_SWITCH_WINDOW_S", 3600)))
AUTH_STATE_PATH = os.path.expanduser(str(os.environ.get("AUTH_STATE_PATH") or
    str(_merged.get("AUTH_STATE_PATH", "~/.hermes/proxy-relay/auth_state.json"))))

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
# Bound the semaphore was created with — recreating it when the config
# changes (hot-reload) keeps the limit live instead of silently stale.
_semaphore_max = MAX_CONCURRENT_UPSTREAM
_model_filter_re = re.compile(MODEL_FILTER_PATTERN)
# Byte-level stream detection: matches {"stream": true} with any JSON
# whitespace between key, colon, and value, case-insensitively (raw bytes —
# no full-body .lower() copy). Requires a JSON delimiter (comma/brace/
# bracket) after `true` so `"stream": "true-string"` doesn't false-positive.
_STREAM_RE = re.compile(rb'"stream"\s*:\s*true(?=\s*[,}\]])', re.IGNORECASE)
# Bodies up to this size are parsed as JSON for PRECISE top-level stream
# detection (avoids the nested-key false positive); larger bodies fall
# back to the byte scan (parsing multi-MB vision JSON is too expensive).
_STREAM_JSON_PARSE_LIMIT = 256 * 1024
_request_count = {"total": 0, "ok": 0, "errors": 0, "auth_failed": 0}
# Counters are plain stats; the critical section (a single dict increment)
# has no await and is atomic under the asyncio loop AND the GIL, so a
# threading.Lock is cheap and strictly safer than the former module-global
# asyncio.Lock (which was not thread-safe and could bind to a stale loop).
_request_count_lock = threading.Lock()
# Requests currently queued waiting for a concurrency permit (bounded by
# MAX_QUEUED_REQUESTS in _acquire_semaphore).
_waiting_count = 0


def _inc_counter(key: str) -> None:
    """Increment a request-stat counter (thread-safe, stats only).

    The critical section is a single dict increment with no await — atomic
    under the asyncio loop and under the GIL — so a plain increment behind
    a cheap threading.Lock is sufficient. Counters are informational
    (/health), not load-bearing.
    """
    with _request_count_lock:
        _request_count[key] += 1
MODELS_CACHE: list[dict] = []
MODELS_CACHE_UPDATED: float = 0.0  # time.monotonic() of last refresh
MODELS_CACHE_TTL: float = 300.0   # refresh every 5 minutes

# Shared httpx client pool (one client per proxy URL, for non-streaming requests)
# OrderedDict so the LRU order is maintained (move_to_end on reuse).
_client_pool: dict[str, httpx.AsyncClient] = OrderedDict()
_client_pool_lock = asyncio.Lock()
_CLIENT_POOL_MAX = 100  # max concurrent clients to keep alive
# In-flight usage per pooled client URL. A client checked out via
# _borrow_client must never be evicted/closed while a request uses it —
# closing an in-use client aborts the request and the error gets attributed
# to the proxy (spurious cooldown).
_client_in_use: dict[str, int] = defaultdict(int)
# time.monotonic() of the last borrow per pooled client URL (for
# CLIENT_IDLE_TTL stale-keep-alive reaping).
_client_last_used: dict[str, float] = {}
_START_TIME: float = time.monotonic()
_stream_shutdown_event = asyncio.Event()
_PROXY_HEALTH_TASK: asyncio.Task | None = None  # background health checker

# Version — single source of truth
VERSION = "1.7.0"

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
                        logger.warning(f"Skipping invalid proxy URL: {_mask_proxy_url(line)}")
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
                logger.warning(f"Skipping invalid proxy URL from env: {_mask_proxy_url(u)}")
    logger.info(f"Loaded {len(proxies)} proxies from PROXY_LIST_ENV")
    return proxies


def _validate_proxy_url(url: str) -> bool:
    """Basic proxy URL validation. Accepts socks5://, socks5h://, http://, https://.

    Supports IPv4, hostnames, and IPv6 in bracket notation ([::1]:1080).
    Rejects invalid port ranges (:0, :99999) so broken proxies don't
    waste pool slots and retry attempts.
    """
    if not url or len(url) > 500:
        return False
    import re as _re
    pattern = _re.compile(
        r'^(socks5|socks5h|http|https)://'
        r'([^:@/]+:[^:@/]+@)?'          # optional user:pass
        r'(?:\[[^\]@]+\]|'               # IPv6 in brackets (incl. zone ids), OR
        r'[a-zA-Z0-9.-]+)'               # hostname / IPv4
        r'(:(\d{1,5}))?'                 # optional port (captured for range check)
        r'(/.*)?$',                      # optional path
    )
    m = pattern.match(url)
    if not m:
        return False
    port = m.group(4)
    if port is not None:
        p = int(port)
        if p < 1 or p > 65535:
            return False
    return True


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
    """If RELAY_AUTO_STAR=1 AND GITHUB_TOKEN is set, auto-star the repo.

    Explicit opt-in only — an ambient GITHUB_TOKEN on a dev box must not
    cause unexpected writes to the user's GitHub account. Skips if the
    token owner is the repo author (omiinaya) or if the repo is already
    starred. Runs once at startup. Silent on failure.
    """
    if os.environ.get("RELAY_AUTO_STAR", "") != "1":
        return
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

async def _aclose_quietly(client) -> None:
    """Best-effort client close — never propagates transport errors."""
    try:
        await client.aclose()
    except Exception:
        pass


def _reap_stale_clients_locked(now: float, keep_url: str | None) -> list:
    """Collect pooled clients idle > CLIENT_IDLE_TTL (LRU order) for closing.

    MUST be called with _client_pool_lock held. Reaps from the LRU head
    (least recently used) while the head is idle AND stale — everything
    behind it is fresher, so the scan is O(number reaped). The keep_url
    client (about to be borrowed) is never reaped; the caller refreshes its
    last-used stamp. Stale-keep-alive prevention: a connection the
    proxy/upstream silently closed while idle is closed BEFORE reuse instead
    of failing on a dead socket and mis-attributing a healthy proxy as down.
    """
    reaped: list = []
    if CLIENT_IDLE_TTL <= 0:
        return reaped
    while _client_pool:
        url = next(iter(_client_pool))  # LRU head
        if url == keep_url:
            break
        if _client_in_use.get(url, 0) > 0:
            break  # in-use clients are never closed mid-flight
        if (now - _client_last_used.get(url, 0.0)) < CLIENT_IDLE_TTL:
            break  # head is fresh → the rest are fresher
        reaped.append(_client_pool.pop(url))
        _client_last_used.pop(url, None)
    if reaped:
        logger.debug(f"Reaped {len(reaped)} stale idle pooled client(s) (idle > {CLIENT_IDLE_TTL}s)")
    return reaped


async def _get_client(proxy_url: str, mark_in_use: bool = False) -> httpx.AsyncClient:
    """Get a shared httpx client for the given proxy URL.

    Clients are reused across requests (streaming AND single-shot) for
    connection pooling. Pool is capped at _CLIENT_POOL_MAX — least-recently-
    used IDLE clients evicted first (true LRU: reusing a client moves it to
    the back of the order). A client with in-flight requests is never
    evicted/closed — closing it would abort those requests and misattribute
    the failure to the proxy.

    Stale-keep-alive prevention: a client idle longer than CLIENT_IDLE_TTL
    is reaped before reuse (see _reap_stale_clients_locked). Evicted/reaped
    clients are closed OUTSIDE the pool lock — aclose() can drain and would
    otherwise serialize all other client acquisitions.

    When mark_in_use=True, the in-use counter is incremented under the
    same lock as the lookup — closing the TOCTOU where _prune_client_pool
    could observe 0 between the lookup and the increment.
    """
    to_close: list = []
    async with _client_pool_lock:
        now = time.monotonic()
        to_close.extend(_reap_stale_clients_locked(now, keep_url=proxy_url))
        client = _client_pool.get(proxy_url)
        if client is None:
            # If pool is at cap, evict the least-recently-used IDLE client.
            # Skip in-use entries (they'd abort live requests on close); if
            # every client is in use, let the pool temporarily exceed the cap
            # rather than kill a request.
            if len(_client_pool) >= _CLIENT_POOL_MAX:
                evict_url = None
                for url, _ in _client_pool.items():
                    if _client_in_use.get(url, 0) == 0:
                        evict_url = url
                        break
                if evict_url is None:
                    logger.debug(
                        f"Pool at cap ({_CLIENT_POOL_MAX}) but all clients in use — "
                        f"temporarily exceeding cap instead of aborting requests"
                    )
                else:
                    to_close.append(_client_pool.pop(evict_url))
                    _client_last_used.pop(evict_url, None)
                    logger.debug(f"Evicted idle client for {evict_url} (pool at cap)")

            transport = httpx.AsyncHTTPTransport(proxy=proxy_url)
            client = httpx.AsyncClient(
                transport=transport,
                timeout=httpx.Timeout(UPSTREAM_READ_TIMEOUT, connect=UPSTREAM_CONNECT_TIMEOUT),
            )
            _client_pool[proxy_url] = client
        else:
            # LRU: re-inserting moves this URL to the back of the dict order
            # so it's evicted only after less-recently-used clients.
            _client_pool.move_to_end(proxy_url)
        _client_last_used[proxy_url] = time.monotonic()
        if mark_in_use:
            _client_in_use[proxy_url] = _client_in_use.get(proxy_url, 0) + 1
    # Close evicted/reaped clients OUTSIDE the lock (aclose can drain).
    for c in to_close:
        await _aclose_quietly(c)
    return client


def _release_client_in_use(proxy_url: str) -> None:
    """Decrement a pooled client's in-use counter (borrow release).

    A client whose counter drops to 0 can be LRU-evicted or pruned again;
    a client still borrowed (counter > 0) is never closed mid-flight.
    """
    if _client_in_use.get(proxy_url, 0) > 0:
        _client_in_use[proxy_url] -= 1
        if _client_in_use[proxy_url] <= 0:
            del _client_in_use[proxy_url]


@asynccontextmanager
async def _borrow_client(proxy_url: str):
    """Get a pooled client, mark it in-use for the duration of the block.

    Usage: async with _borrow_client(url) as client: await _proxy_single(...)
    The in-use counter prevents eviction/close while the request is live.
    The counter is incremented INSIDE _get_client under _client_pool_lock so
    a concurrent _prune_client_pool can't observe 0 and close the client
    between the lookup and the increment (TOCTOU).
    """
    client = await _get_client(proxy_url, mark_in_use=True)
    try:
        yield client
    finally:
        _release_client_in_use(proxy_url)


async def _make_streaming_client(proxy_url: str) -> httpx.AsyncClient:
    """Borrow a POOLED client for streaming (generator-owned lifecycle).

    Streams reuse the shared per-proxy client — and its warm TCP/TLS/SOCKS5
    connection — instead of paying a fresh SOCKS5 handshake + TLS handshake
    per stream (the pre-1.6 behavior: one new client+transport per stream
    request, which thundered the event loop under burst load). The stream
    generator releases the borrow at stream end; the client stays pooled
    for the next request. Eviction skips in-use clients, so a live stream
    is never aborted.
    """
    return await _get_client(proxy_url, mark_in_use=True)


async def _close_all_clients():
    """Close all shared httpx clients (call on shutdown/pool reload)."""
    async with _client_pool_lock:
        clients = list(_client_pool.values())
        _client_pool.clear()
        _client_last_used.clear()
    # Close outside the lock (aclose can drain).
    for client in clients:
        await _aclose_quietly(client)


async def _prune_client_pool(active_urls: set[str]):
    """Close shared clients for proxies no longer in the pool.

    Called after a proxy list reload — removed proxies shouldn't keep
    their pooled connections alive. Clients with in-flight requests are
    NEVER closed mid-flight (that would abort the request and misattribute
    the failure to the proxy) — they are deferred and closed by a
    background task once their usage drains.
    """
    deferred: list[str] = []
    to_close: list = []
    async with _client_pool_lock:
        stale = [url for url in _client_pool if url not in active_urls]
        for url in stale:
            if _client_in_use.get(url, 0) > 0:
                deferred.append(url)
                continue
            to_close.append(_client_pool.pop(url))
            _client_last_used.pop(url, None)
        if stale:
            logger.info(f"Pruned {len(stale)} pooled client(s) for removed proxies")
    # Close outside the lock — aclose can drain and would serialize all
    # other client acquisitions.
    for c in to_close:
        await _aclose_quietly(c)
    for url in deferred:
        asyncio.create_task(_close_client_when_idle(url))


async def _close_client_when_idle(url: str, max_wait_s: float = 65.0):
    """Close a pooled client once its in-flight requests drain.

    Deferred-close for proxies removed from the pool while a request was
    still borrowing their client. Polls the in-use counter (bounded wait —
    a stuck request must not leak the task forever), then closes.

    `max_wait_s` (65) is deliberately above the 60s client timeout: a
    request in flight longer than the cap must NOT be force-closed
    mid-flight (the failure would be misattributed to the proxy).
    """
    deadline = time.monotonic() + max_wait_s
    while time.monotonic() < deadline:
        if _client_in_use.get(url, 0) <= 0:
            break
        await asyncio.sleep(0.1)
    async with _client_pool_lock:
        # Re-check under the lock: a concurrent borrower may have grabbed
        # this client after the loop's unlocked check (e.g. a URL re-added
        # by a second reload, or a pre-reload request borrowing late).
        # Closing it now would abort that in-flight request and misattribute
        # the failure to the proxy.
        if _client_in_use.get(url, 0) > 0:
            return
        client = _client_pool.pop(url, None)
        _client_last_used.pop(url, None)
    if client is not None:
        # Close outside the lock (aclose can drain).
        await _aclose_quietly(client)


async def _proxy_health_check():
    """Background task: periodically test each proxy's connectivity.

    Attempts a connection through each proxy to verify it's alive.
    Dead proxies are marked as permanently failed — BUT only if at
    least one other proxy succeeded in the same sweep. If every proxy
    fails at once, the health target is likely down or the network is
    partitioned — marking all proxies dead would be wrong, so they're
    left alive and a warning is logged.

    A single partial-sweep failure does NOT kill a proxy: some proxy
    networks whitelist only API domains and block generic targets like
    httpbin.org. A proxy is only marked permanently dead after
    HEALTH_FAIL_THRESHOLD consecutive failures in separate sweeps.
    When UPSTREAM_BASE is configured, the health check targets it
    (the endpoint proxies are actually used for), falling back to
    PROXY_HEALTH_CHECK_URL.
    """
    if PROXY_HEALTH_CHECK_INTERVAL <= 0:
        logger.info("Proxy health checker disabled (PROXY_HEALTH_CHECK_INTERVAL=0)")
        return
    # Consecutive health-check failures per proxy URL. Reset on success.
    health_fail_count: dict[str, int] = {}
    while True:
        # A hot-reload can set PROXY_HEALTH_CHECK_INTERVAL to 0 mid-run —
        # guard INSIDE the loop so the checker backs off instead of
        # spinning on asyncio.sleep(0) and hammering the target.
        if PROXY_HEALTH_CHECK_INTERVAL <= 0:
            await asyncio.sleep(60)
            continue
        try:
            await asyncio.sleep(PROXY_HEALTH_CHECK_INTERVAL)
            if pool.total == 0:
                continue

            # Prefer checking the real upstream — it's the endpoint the
            # proxies are actually used for. Fall back to the configured
            # target when UPSTREAM_BASE is empty.
            check_url = PROXY_HEALTH_CHECK_URL
            if UPSTREAM_BASE:
                check_url = f"{UPSTREAM_BASE}/models"

            healthy = 0
            failures: list[tuple[ProxyEntry, str]] = []
            now = time.monotonic()
            # Probe only proxies that need attention: permanently-dead (for
            # revival), cooling (verify recovery), or never-used (new/untested).
            # Healthy, recently-successful proxies are NOT hammered every sweep
            # — real traffic validates them, and this cuts upstream load to
            # ~zero when the pool is healthy (the old code probed every proxy
            # in the pool on every sweep, which was ~N requests/min of load
            # on the real upstream for no benefit).
            entries = [
                e for e in pool._proxies
                if e.permanently_dead or e.cooldown_until > now or e.total_ok == 0
            ]
            if not entries:
                # Fully healthy pool — clear any lingering failure counters and
                # skip the sweep (no upstream load when nothing needs checking).
                health_fail_count.clear()
                continue

            # Bounded-concurrency sweep. The old serial loop awaited each
            # proxy in turn — a 250-proxy pool took ~N × probe-time per
            # sweep (minutes when proxies stall). Probing ALL at once is
            # worse (250 concurrent upstream requests = rate-limit bait).
            # HEALTH_CHECK_CONCURRENCY (default 20) caps in-flight probes
            # so a sweep wall-time is ~N/concurrency × probe-time.
            probe_sem = asyncio.Semaphore(max(1, HEALTH_CHECK_CONCURRENCY))

            async def _probe(entry: ProxyEntry):
                nonlocal healthy
                async with probe_sem:
                    try:
                        transport = httpx.AsyncHTTPTransport(proxy=entry.url)
                        async with httpx.AsyncClient(
                            transport=transport, timeout=httpx.Timeout(10.0)
                        ) as test_client:
                            resp = await test_client.get(
                                check_url, timeout=10.0
                            )
                            if resp.status_code < 500:
                                # A previously-dead proxy that now responds is
                                # revived. next() skips permanently_dead
                                # proxies, so the health checker is the only
                                # automated verifier — "permanently dead"
                                # means dead until verified otherwise, not
                                # dead forever.
                                healthy += 1
                                if entry.permanently_dead:
                                    pool.record_success(entry)
                                    logger.info(
                                        f"Health check: proxy {_mask_proxy_url(entry.url)} "
                                        f"recovered — revived"
                                    )
                            else:
                                failures.append((entry, "Health check returned 5xx"))
                    except Exception:
                        failures.append((entry, "Health check connection failed"))

            # healthy/failures are only touched in no-await critical
            # sections (single increments/append + no awaits between
            # read and write), so they are atomic under the asyncio loop.
            await asyncio.gather(*(_probe(e) for e in entries))

            if failures and healthy == 0:
                # Everything failed — the health target is probably down,
                # not the proxies. Don't nuke the pool.
                logger.warning(
                    f"Health check: ALL {len(failures)} proxies failed — "
                    f"health target may be unreachable; leaving proxies alive"
                )
                # Reset counters — this was a target problem, not proxy problems
                for entry, _ in failures:
                    health_fail_count.pop(entry.url, None)
            elif failures:
                for entry, reason in failures:
                    count = health_fail_count.get(entry.url, 0) + 1
                    health_fail_count[entry.url] = count
                    if count >= HEALTH_FAIL_THRESHOLD:
                        pool.record_permanent_failure(entry, reason=reason)
                        logger.warning(
                            f"Health check: proxy {_mask_proxy_url(entry.url)} — "
                            f"marked permanently unavailable after {count} "
                            f"consecutive failures ({reason})"
                        )
                        health_fail_count.pop(entry.url, None)
                    else:
                        logger.warning(
                            f"Health check: proxy {_mask_proxy_url(entry.url)} "
                            f"failed ({count}/{HEALTH_FAIL_THRESHOLD} consecutive) — "
                            f"not yet marked dead ({reason})"
                        )
                logger.info(
                    f"Health check: {healthy} healthy, {len(failures)} failed "
                    f"({pool.available_count}/{pool.total} available)"
                )
            elif healthy:
                # Any proxy that now succeeds resets its failure counter
                for url in list(health_fail_count):
                    health_fail_count.pop(url, None)
                logger.info(
                    f"Health check: {healthy} healthy "
                    f"({pool.available_count}/{pool.total} available)"
                )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Health check error: {e}")


def _build_headers(original: dict, auth_type: str | None = None) -> dict:
    """Forward client headers, stripping those the relay manages itself.

    The relay is responsible for its own upstream content negotiation:
    httpx auto-decompresses gzip/deflate/brotli responses, and we
    strip Content-Encoding from responses so the client always gets
    uncompressed data. Passing Accept-Encoding from the client would
    risk codecs httpx doesn't handle (e.g. zstd) being returned
    compressed without the header to signal it.

    `auth_type` optionally overrides the active upstream auth type —
    used by the AuthSwitcher probe to test an alternate method with the
    same API key before committing to a switch.
    """
    headers = {}
    for key, val in original.items():
        lkey = key.lower()
        if lkey == "authorization":
            continue
        # Strip relay-managed headers so they never reach the upstream:
        # content-length (recomputed by httpx), host (upstream's own),
        # connection (transport-managed), accept-encoding (we negotiate),
        # x-admin-key (relay's own admin auth — must not leak upstream),
        # x-api-key (a client-supplied key must not override the relay's
        # upstream credential — unless the relay itself uses x-api-key auth,
        # in which case we inject our own below and the client's is dropped
        # either way), transfer-encoding (httpx re-frames the body).
        if lkey in ("content-length", "host", "connection", "accept-encoding",
                    "x-admin-key", "x-api-key", "transfer-encoding"):
            continue
        headers[key] = val
    at = (auth_type or UPSTREAM_AUTH_TYPE).lower()
    if at == "x-api-key":
        headers["x-api-key"] = UPSTREAM_API_KEY
    else:
        headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"
    return headers


def _parse_retry_after(headers) -> int:
    raw = headers.get("retry-after", "")
    if not raw:
        return 60
    try:
        # Clamp to a sane minimum — negative/zero would cool for ~0s and
        # defeat the rate-limiter's purpose. The upper clamp (in
        # record_429) protects against absurd values; here we also guard
        # int conversion so a hostile header can't raise OverflowError.
        try:
            secs = int(raw)
        except OverflowError:  # pragma: no cover — int(str) can't overflow
            return 60
        except ValueError:
            # Not an integer — fall through to HTTP-date parsing below.
            pass
        else:
            return max(secs, 10)
        # HTTP-date fallback (RFC 2822 / RFC 7231)
        from email.utils import parsedate_to_datetime
        parsed = parsedate_to_datetime(raw)
        # Naive dates (no timezone suffix) are HTTP-date → UTC.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        seconds = (parsed - datetime.now(timezone.utc)).total_seconds()
        return max(int(seconds), 10)
    except Exception:
        return 60


# ╔══════════════════════════════════════════════════════════════════╗
# ║  AuthSwitcher — smart upstream auth-type fallback               ║
# ╚══════════════════════════════════════════════════════════════════╝


class AuthSwitcher:
    """Detect upstream auth-method failures and auto-switch auth type.

    Only a 401 counts as an auth signal — it means the request REACHED
    the upstream and was rejected for credentials. 5xx (server issue),
    429 (rate limit), and connection errors (proxy/network) are NEVER
    auth failures and never trigger a switch.

    On N consecutive live 401s, probe alternate auth types with the SAME
    api key against a cheap endpoint (GET /models). Switch only when a
    candidate returns 200 on consecutive probes — positive evidence the
    method changed. If every candidate 401s, the key itself is dead
    (alert, no switch). If probes fail to connect, it was never an auth
    problem (no switch).

    Anti-flap rails: cooldown between probes, and a max number of
    switches per window — exceeding it latches a "flapping" alert and
    stops auto-switching until an operator intervenes.
    """

    def __init__(self, candidates=("bearer", "x-api-key"), trigger_threshold=3,
                 probe_successes=2, cooldown_s=300, max_per_window=3,
                 window_s=3600, state_path="", enabled=True):
        self.candidates = list(candidates)
        self.trigger_threshold = max(1, int(trigger_threshold))
        self.probe_successes = max(1, int(probe_successes))
        self.cooldown_s = max(0, int(cooldown_s))
        self.max_per_window = max(1, int(max_per_window))
        self.window_s = max(1, int(window_s))
        self.state_path = state_path
        self.enabled = enabled

        self._lock = asyncio.Lock()
        self._consecutive_401 = 0
        self._total_401 = 0
        self._probe_running = False
        self._last_probe_ts = 0.0
        self._switch_ts: deque[float] = deque()
        self._switch_history: list[dict] = []
        self._alert = None  # None | "key_revoked" | "flapping"
        self._probes_run = 0
        self._switches_done = 0

    # ── observation ────────────────────────────────────────────────

    def observe(self, status_code: int) -> None:
        """Record an upstream response status.

        401 → consecutive counter++ (and total). <400 → reset (a success
        breaks the streak). 4xx/5xx/429 are auth-agnostic: they neither
        count nor reset — a lone 403 between 401s must not clear the
        streak (auth may still be the problem), and a 429 must not count
        as an auth failure.
        """
        if not self.enabled:
            return
        if status_code == 401:
            self._consecutive_401 += 1
            self._total_401 += 1
        elif status_code < 400:
            self._consecutive_401 = 0

    def should_probe(self) -> bool:
        """True when the trigger threshold is crossed and rails allow a probe."""
        if not self.enabled:
            return False
        if self._consecutive_401 < self.trigger_threshold:
            return False
        now = time.monotonic()
        if now - self._last_probe_ts < self.cooldown_s:
            return False
        # Prune switch timestamps outside the window, then enforce the cap.
        cutoff = now - self.window_s
        while self._switch_ts and self._switch_ts[0] < cutoff:
            self._switch_ts.popleft()
        if len(self._switch_ts) >= self.max_per_window:
            self._alert = "flapping"
            return False
        return True

    # ── probing ────────────────────────────────────────────────────

    async def probe_and_switch(self) -> bool:
        """Probe alternate auth types; switch if one verifies.

        Returns True if UPSTREAM_AUTH_TYPE was changed (caller may retry
        the current request with the new auth).
        """
        if not self.enabled:
            return False
        async with self._lock:
            if self._probe_running:
                return False
            self._probe_running = True
            try:
                self._last_probe_ts = time.monotonic()
                self._probes_run += 1
                return await self._probe_and_switch_locked()
            finally:
                self._probe_running = False

    async def _probe_and_switch_locked(self) -> bool:
        current = UPSTREAM_AUTH_TYPE
        saw_rejected = False
        for cand in self.candidates:
            if cand == current:
                continue
            result = await self._probe_auth(cand)
            if result == "ok":
                self._commit_switch(current, cand)
                return True
            if result == "rejected":
                saw_rejected = True
                continue
            # inconclusive (connection/upstream) — NOT an auth problem
            logger.warning(
                f"AUTH SWITCH: probe of '{cand}' inconclusive "
                f"(connection/upstream error) — not switching; this is not an auth failure"
            )
            return False
        if saw_rejected:
            self._alert = "key_revoked"
            logger.error(
                f"AUTH SWITCH: every candidate ({', '.join(self.candidates)}) "
                f"rejected with 401 — the API key itself is likely revoked/expired; "
                f"manual intervention required"
            )
        return False

    async def _probe_auth(self, auth_type: str) -> str:
        """Probe upstream with a candidate auth type through the pool.

        Returns "ok" (200 seen), "rejected" (401 seen), or "inconclusive"
        (connection error / 5xx / 429 — not an auth signal).
        """
        if not UPSTREAM_BASE:
            return "inconclusive"
        url = f"{UPSTREAM_BASE}/models"
        # Gate the probe with the concurrency semaphore — it IS an upstream
        # call and must not bypass MAX_CONCURRENT_UPSTREAM (design rule: all
        # upstream-touching routes honor the gate). A short bounded wait; if
        # the relay is at capacity, defer the probe (inconclusive is never an
        # auth signal, so a skipped probe cannot cause a false switch).
        gate = await _acquire_semaphore(RETRY_SEMAPHORE_WAIT_SECONDS)
        if gate is None:
            return "inconclusive"
        try:
            return await self._probe_auth_gated(auth_type, url)
        finally:
            gate.release()

    async def _probe_auth_gated(self, auth_type: str, url: str) -> str:
        successes = 0
        tried: set[str] = set()
        for _ in range(self.probe_successes * 3):
            proxy_entry = pool.next()
            if proxy_entry is None or proxy_entry.url in tried:
                break
            tried.add(proxy_entry.url)
            headers = _build_headers({}, auth_type=auth_type)
            try:
                async with _borrow_client(proxy_entry.url) as client:
                    resp = await client.request("GET", url, headers=headers)
                    if resp.status_code == 200:
                        successes += 1
                        if successes >= self.probe_successes:
                            return "ok"
                    elif resp.status_code == 401:
                        return "rejected"
                    else:
                        return "inconclusive"
            except (httpx.ConnectError, httpx.ConnectTimeout):
                continue  # proxy-specific — try another
            except (httpx.ReadTimeout, httpx.RemoteProtocolError):
                return "inconclusive"
        return "inconclusive"

    # ── switching / persistence ────────────────────────────────────

    def _commit_switch(self, old: str, new: str) -> None:
        global UPSTREAM_AUTH_TYPE
        UPSTREAM_AUTH_TYPE = new
        now = time.monotonic()
        self._switch_ts.append(now)
        self._switches_done += 1
        self._consecutive_401 = 0
        self._switch_history.append({
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "from": old,
            "to": new,
        })
        self._alert = None
        self._save_state()
        logger.warning(
            f"AUTH SWITCH: upstream auth {old} → {new} after "
            f"{self._total_401} upstream 401s (probe-verified)"
        )

    def _save_state(self) -> None:
        if not self.state_path:
            return
        try:
            p = os.path.expanduser(self.state_path)
            os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
            with open(p, "w") as f:
                json.dump({
                    "auth_type": UPSTREAM_AUTH_TYPE,
                    "switched_at": self._switch_history[-1]["ts"] if self._switch_history else None,
                    "switch_count": self._switches_done,
                }, f, indent=2)
        except Exception as e:
            logger.warning(f"AUTH SWITCH: failed to persist auth state: {e}")

    def load_state(self) -> str | None:
        """Read the persisted auth type (last verified at runtime), if any."""
        if not self.state_path:
            return None
        try:
            p = os.path.expanduser(self.state_path)
            if not os.path.exists(p):
                return None
            with open(p) as f:
                data = json.load(f)
            t = str(data.get("auth_type", "")).lower()
            return t if t in self.candidates else None
        except Exception as e:
            logger.warning(f"AUTH SWITCH: failed to load auth state: {e}")
            return None

    def reconfigure(self, *, candidates=None, trigger_threshold=None,
                    probe_successes=None, cooldown_s=None, max_per_window=None,
                    window_s=None, state_path=None, enabled=None) -> None:
        """Update runtime knobs from a config reload."""
        if candidates:
            self.candidates = list(candidates)
        if trigger_threshold is not None:
            self.trigger_threshold = max(1, int(trigger_threshold))
        if probe_successes is not None:
            self.probe_successes = max(1, int(probe_successes))
        if cooldown_s is not None:
            self.cooldown_s = max(0, int(cooldown_s))
        if max_per_window is not None:
            self.max_per_window = max(1, int(max_per_window))
        if window_s is not None:
            self.window_s = max(1, int(window_s))
        if state_path is not None:
            self.state_path = state_path
        if enabled is not None:
            self.enabled = enabled

    def reset(self) -> None:
        """Clear counters/history (used by tests and config reload)."""
        self._consecutive_401 = 0
        self._total_401 = 0
        self._probe_running = False
        self._last_probe_ts = 0.0
        self._switch_ts.clear()
        self._switch_history.clear()
        self._alert = None
        self._probes_run = 0
        self._switches_done = 0

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "current_auth_type": UPSTREAM_AUTH_TYPE,
            "consecutive_401s": self._consecutive_401,
            "total_401s": self._total_401,
            "probes_run": self._probes_run,
            "switches": self._switches_done,
            "alert": self._alert,
            "candidates": list(self.candidates),
            "switch_history": list(self._switch_history[-5:]),
        }


# Module-level switcher (one per relay process). Reads AUTH_STATE_PATH at
# startup: a persisted type from a previous run reflects what the upstream
# actually accepted LAST time — trust it over config (which may predate an
# upstream flip), and let the live 401 detection re-verify.
auth_switcher = AuthSwitcher(
    candidates=AUTH_SWITCH_CANDIDATES,
    trigger_threshold=AUTH_SWITCH_TRIGGER_THRESHOLD,
    probe_successes=AUTH_SWITCH_PROBE_SUCCESSES,
    cooldown_s=AUTH_SWITCH_COOLDOWN_S,
    max_per_window=AUTH_SWITCH_MAX_PER_WINDOW,
    window_s=AUTH_SWITCH_WINDOW_S,
    state_path=AUTH_STATE_PATH,
    enabled=AUTH_SWITCH_ENABLED,
)
_stored_auth = auth_switcher.load_state()
if _stored_auth and _stored_auth != UPSTREAM_AUTH_TYPE:
    logger.warning(
        f"AUTH SWITCH: persisted state says upstream auth is '{_stored_auth}' "
        f"(config: '{UPSTREAM_AUTH_TYPE}') — using state value; update config to match"
    )
    UPSTREAM_AUTH_TYPE = _stored_auth


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


def _resize_semaphore() -> bool:
    """Recreate the concurrency semaphore if MAX_CONCURRENT_UPSTREAM changed.

    asyncio.Semaphore has no resize API; the only way to apply a new
    limit at runtime is to swap in a fresh semaphore. Existing holders
    keep their slot (they release into the old semaphore, which is then
    garbage collected) — new acquisitions observe the new limit.

    Returns True if the semaphore was recreated.
    """
    global semaphore, _semaphore_max
    if MAX_CONCURRENT_UPSTREAM == _semaphore_max:
        return False
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPSTREAM)
    _semaphore_max = MAX_CONCURRENT_UPSTREAM
    logger.info(
        f"Concurrency limit updated: {MAX_CONCURRENT_UPSTREAM} "
        f"(semaphore recreated)"
    )
    return True


async def _acquire_semaphore(timeout: float | None = None):
    """Acquire the upstream concurrency semaphore with a bounded wait.

    Returns the acquired semaphore (caller MUST release THAT object) or
    None on timeout. Returning the object is load-bearing: a concurrent
    config reload can swap the module-global `semaphore` mid-request, so
    releasing the global would release into the NEW semaphore and inflate
    its permits. Cancellation propagates — a client disconnect during the
    wait must cancel the request, not swallow it and continue to build a
    response for a dead socket.

    Backlog: MAX_QUEUED_REQUESTS bounds how many requests may QUEUE for a
    permit. When the queue is full, new requests fail fast (None → 503)
    instead of piling up behind long-held permits (streams hold theirs for
    the whole stream lifetime). This converts an unbounded pile-up into a
    bounded burst-drain: up to the cap queue, the rest are shed immediately.
    """
    global _waiting_count
    if MAX_QUEUED_REQUESTS > 0 and _waiting_count >= MAX_QUEUED_REQUESTS:
        logger.warning(
            f"Semaphore backlog full ({_waiting_count} waiting >= "
            f"MAX_QUEUED_REQUESTS={MAX_QUEUED_REQUESTS}) — failing fast"
        )
        return None
    _waiting_count += 1
    try:
        # Bind the global ONCE. A concurrent reload may swap `semaphore` at
        # an await point while this coroutine is mid-acquire — reading it
        # twice (once for acquire, once for return) would hand the caller a
        # permit from the OLD semaphore but return the NEW one, and releasing
        # the new one would over-credit its permits (TOCTOU).
        sem = semaphore
        if timeout is not None:
            task = asyncio.ensure_future(sem.acquire())
            try:
                await asyncio.wait_for(task, timeout=timeout)
            except asyncio.TimeoutError:
                if task.cancelled():
                    return None
                # wait_for cancelled the inner task, but the acquire may have
                # COMPLETED in the same tick the timeout fired — the permit
                # is then taken and nobody would release it (a permanent
                # capacity leak). Detect that race: if the task is done (not
                # merely cancelled), the permit is ours — hand it back so the
                # caller releases it.
                if task.done():
                    return sem
                # Unreachable with real asyncio.wait_for: it awaits the task
                # after cancelling it, so on TimeoutError the task is always
                # either cancelled (handled above) or done (race window above).
                return None  # pragma: no cover — defensive only
            except asyncio.CancelledError:
                # The OUTER task was cancelled (client disconnect) while
                # wait_for was pending. wait_for cancels the inner task and
                # re-raises here. If the acquire COMPLETED in the same tick,
                # the permit is taken but discarded — release it before
                # propagating the cancellation, or it leaks forever.
                if task.done() and not task.cancelled():
                    sem.release()
                raise
        else:
            await sem.acquire()
        return sem
    finally:
        _waiting_count -= 1


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Proxy request logic (streaming + single-shot)                  ║
# ╚══════════════════════════════════════════════════════════════════╝


def _model_allowed(model_name: str) -> bool:
    return bool(_model_filter_re.search(model_name))


# Query-string params whose values are secrets — redacted from logs.
_REDACT_QUERY_PARAMS = {
    "api_key", "apikey", "key", "token", "access_token", "auth",
    "authorization", "password", "secret", "signature", "sig",
    "x_api_key", "client_secret", "client_id",
}


def _redact_query(query: str) -> str:
    """Redact credential-like params from a query string for logging.

    Param names are normalized before matching — percent-encoded
    (`api%5Fkey`), dashed (`api-key`) and camelCase (`apiKey`) variants
    of a secret param would otherwise leak the value into logs.
    """
    if not query:
        return query
    parts = []
    for pair in query.split("&"):
        name, sep, value = pair.partition("=")
        if sep:
            # Normalize BEFORE matching: lowercase, dashes → underscores,
            # percent-encoding decoded. unquote_plus on a str never raises.
            normalized = unquote_plus(name.lower().replace("-", "_"))
            if normalized in _REDACT_QUERY_PARAMS:
                parts.append(f"{name}=***")
                continue
        parts.append(pair)
    return "&".join(parts)


def _client_key_valid(headers: dict) -> bool:
    """Check client auth headers against CLIENT_API_KEY.

    Accepts `Authorization: Bearer <key>` or `X-API-Key: <key>` (case-
    insensitive header names). Returns True when CLIENT_API_KEY is unset
    (auth disabled).
    """
    if not CLIENT_API_KEY:
        return True
    lowered = {k.lower(): v for k, v in headers.items()}
    auth = lowered.get("authorization", "")
    api_key_hdr = lowered.get("x-api-key", "")
    provided = ""
    if auth.lower().startswith("bearer "):
        provided = auth[len("Bearer "):].strip()
    elif api_key_hdr:
        provided = api_key_hdr.strip()
    # Constant-time compare — plain == would short-circuit on the first
    # mismatching byte, letting a local attacker measure key prefixes.
    return secrets.compare_digest(provided, CLIENT_API_KEY)


def _client_auth_error() -> JSONResponse:
    """Standard 401 response for missing/invalid client key."""
    return JSONResponse(
        status_code=401,
        content={
            "error": {
                "message": "Invalid or missing client API key.",
                "type": "authentication_error",
                "code": "invalid_client_key",
            }
        },
        headers={"WWW-Authenticate": "Bearer"},
    )


def _body_too_large(request: Request) -> bool:
    """Check Content-Length header against MAX_BODY_SIZE without reading body.

    Returns True when the declared body size exceeds the cap — the caller
    returns 413 without ever reading the body into memory (cheap pre-reject
    for oversized uploads).
    """
    if MAX_BODY_SIZE <= 0:
        return False
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_BODY_SIZE:
                return True
        except ValueError:
            pass
    return False


async def _read_body_capped(request: Request) -> bytes | None:
    """Read the request body, returning None when it exceeds MAX_BODY_SIZE.

    Uses the request stream to read at most MAX_BODY_SIZE+1 bytes — an
    oversized body never gets fully buffered. The extra byte is enough
    to detect the overrun. Falls back to request.body() ONLY when the
    stream failed before yielding anything (a partially-consumed stream
    can't be re-read — body() would raise "Stream consumed").
    """
    if MAX_BODY_SIZE <= 0:
        return await request.body()
    if _body_too_large(request):
        return None
    chunks = []
    total = 0
    stream_error = None
    try:
        async for chunk in request.stream():
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_BODY_SIZE:
                return None
    except Exception as e:
        stream_error = e
    if not chunks and stream_error is not None:
        # Stream failed before any bytes — safe to fall back to body()
        try:
            body = await request.body()
            if len(body) > MAX_BODY_SIZE:
                return None
            return body
        except Exception:
            return None
    if stream_error is not None:
        # Partial consumption + error → client disconnected mid-upload
        return None
    return b"".join(chunks)


def _detect_stream_request(body: bytes | None) -> bool:
    """Decide whether a request body asks for a streaming response.

    Small bodies: parse JSON and check the TOP-LEVEL `stream` key — precise.
    The byte-scan regex alone would false-positive on a nested
    `"stream": true` inside free-form fields (e.g. `"metadata":
    {"stream": true}` or a tool-schema property), routing a non-stream
    request through the chunked path.
    Large bodies (multi-MB vision JSON): parsing into an object tree is too
    expensive, so fall back to the byte scan — a single linear pass on raw
    bytes with no copy. `is True` (not bool()) preserves the regex
    semantics: only a literal boolean true counts; `"stream": "true-string"`
    must not.
    """
    if not body:
        return False
    if len(body) <= _STREAM_JSON_PARSE_LIMIT:
        try:
            return json.loads(body).get("stream") is True
        except Exception:
            # Not JSON / truncated — fall back to the byte scan.
            pass
    # Large body (or parse failure): avoid a full-body regex on the event
    # loop where possible. CPython's re already literal-prefix-optimizes the
    # scan, but for the common cases this is strictly cheaper:
    #   • "stream" key present (every real OpenAI payload) → locate the key
    #     with a fast C find, then regex only the small window after it.
    #   • key absent entirely → never worse than the old single pass for
    #     that case alone (one find + one fallback regex, both C-speed).
    # Case-preserving: a non-lowercase "Stream" key (regex is IGNORECASE)
    # in a huge body falls through to the full regex — rare, and correct.
    pos = body.find(b'"stream"')
    if pos != -1:
        # A legal `"stream": true` must be no more than a few bytes of JSON
        # whitespace after the key; a 256B window is generous for any real
        # payload without scanning from every occurrence.
        while pos != -1:
            if _STREAM_RE.search(body[pos:pos + 256]):
                return True
            pos = body.find(b'"stream"', pos + 1)
        return False
    # No lowercase key anywhere — could still be "STREAM": true upstream
    # (IGNORECASE semantics preserved via a full scan in this rare case).
    return _STREAM_RE.search(body) is not None


async def _proxy_request(
    method: str,
    path: str,
    body: bytes | None,
    headers: dict,
    query_string: str,
) -> Response | StreamingResponse:
    _inc_counter("total")

    # Optional client auth — prevents open-proxy abuse when the relay is
    # bound to a non-local interface. Clients present the key as
    # `Authorization: Bearer <key>` or `X-API-Key: <key>`.
    if not _client_key_valid(headers):
        logger.warning(
            f"Client auth failed for {method} {path} "
            f"(missing or invalid key)"
        )
        _inc_counter("auth_failed")
        return JSONResponse(
            status_code=401,
            content={
                "error": {
                    "message": "Invalid or missing client API key.",
                    "type": "authentication_error",
                    "code": "invalid_client_key",
                }
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

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
    is_stream = _detect_stream_request(body)

    # Streaming requests retry across proxies on connection failure, matching
    # the non-streaming path. The request body is fully in memory (bytes), so
    # a connect error before the response starts is safe to retry — no bytes
    # have reached the client yet. Once _proxy_stream returns a
    # StreamingResponse, the generator owns the client lifecycle and no
    # retry happens (a mid-stream failure would have already sent bytes).
    if is_stream:
        last_error = None
        attempt = 0
        tried_urls: set[str] = set()
        dup_scan = 0  # consecutive already-tried returns (rotation stall guard)

        while attempt < MAX_REQUEST_RETRIES:
            # Exponential backoff before a RETRY (not the first attempt) —
            # kinder to the upstream during a failure cascade.
            if last_error is not None and RETRY_BACKOFF_BASE > 0:
                await asyncio.sleep(min(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)), RETRY_BACKOFF_MAX))
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
                        f"stopping stream retry loop"
                    )
                    break
                dup_scan += 1
                if dup_scan >= pool.total:
                    logger.warning(
                        f"All untried proxies cooling, stopping stream retry loop "
                        f"({len(tried_urls)} tried, {pool.total} total)"
                    )
                    break
                continue
            tried_urls.add(proxy_entry.url)
            dup_scan = 0
            attempt += 1

            # First attempt may queue for capacity (full wait); retries fail
            # fast instead of stacking more long waits on a failing request.
            wait = SEMAPHORE_WAIT_SECONDS if last_error is None else RETRY_SEMAPHORE_WAIT_SECONDS
            acquired_sem = await _acquire_semaphore(wait)
            if acquired_sem is None:
                logger.warning(
                    f"Semaphore busy for {wait}s — returning 503 "
                    f"(concurrency={MAX_CONCURRENT_UPSTREAM})"
                )
                return JSONResponse(
                    status_code=503,
                    content={
                        "error": {
                            "message": "Relay at capacity — try again later.",
                            "type": "overloaded_error",
                            "code": "relay_at_capacity",
                        }
                    },
                    headers={"Retry-After": "10"},
                )
            semaphore_handed_off = False
            try:
                streaming_client = None
                try:
                    streaming_client = await _make_streaming_client(proxy_entry.url)
                    resp = await _proxy_stream(streaming_client, method, upstream_url,
                                               req_headers, body, proxy_entry,
                                               acquired_sem)
                    # _proxy_stream now owns the semaphore on EVERY return
                    # path: error Responses release it before returning,
                    # the success StreamingResponse releases it when its
                    # generator finishes (holding the slot for the whole
                    # stream — that's the point of the concurrency limit).
                    semaphore_handed_off = True
                    # Smart auth switching (see non-stream path): a 401 is
                    # an auth rejection, not a proxy/upstream failure. On
                    # the trigger threshold, probe alternate auth types and
                    # retry once with the verified type if a switch happens.
                    if AUTH_SWITCH_ENABLED:
                        auth_switcher.observe(resp.status_code)
                        if resp.status_code == 401 and auth_switcher.should_probe():
                            if await auth_switcher.probe_and_switch():
                                req_headers = _build_headers(dict(headers))
                                # The 401 error path in _proxy_stream already
                                # released the semaphore AND the pooled client
                                # borrow. Acquire a fresh slot + re-borrow the
                                # client for the retry — reusing either would
                                # double-release the semaphore or the borrow.
                                retry_sem = await _acquire_semaphore(SEMAPHORE_WAIT_SECONDS)
                                if retry_sem is not None:
                                    streaming_client = await _make_streaming_client(proxy_entry.url)
                                    resp = await _proxy_stream(
                                        streaming_client, method, upstream_url,
                                        req_headers, body, proxy_entry, retry_sem)
                                    # The retried stream (or its error
                                    # Response) owns/releases retry_sem; the
                                    # caller's finally must NOT release the
                                    # original (already released) semaphore —
                                    # the flag stays True for that reason.
                                else:
                                    logger.warning(
                                        "AUTH SWITCH: switched auth but semaphore "
                                        "busy — not retrying current stream"
                                    )
                    # _proxy_stream returns a StreamingResponse for success and a
                    # plain Response for 429/4xx/5xx error statuses. Retry on 5xx
                    # like the non-streaming path; everything else is final.
                    if resp.status_code < 500 or resp.status_code == 429:
                        return resp
                    last_error = resp
                    logger.warning(
                        f"Upstream 5xx on {_mask_proxy_url(proxy_entry.url)} "
                        f"({resp.status_code}), retrying... "
                        f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                    )
                except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                    pool.record_timeout(proxy_entry)
                    _inc_counter("errors")
                    if streaming_client is not None:
                        _release_client_in_use(proxy_entry.url)
                    last_error = JSONResponse(
                        status_code=502,
                        content={
                            "error": {
                                "message": "Proxy connection failed.",
                                "type": "proxy_error",
                                "code": "proxy_connect_failed",
                            }
                        },
                    )
                    logger.warning(
                        f"Stream proxy {_mask_proxy_url(proxy_entry.url)} connect failed: {e} "
                        f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                    )
                except (httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                    # Upstream stalled after the proxy connected — the proxy
                    # itself is likely fine. Short cooldown, NOT counted toward
                    # permanent death (a flaky upstream must not kill good
                    # proxies). Safe to retry: no bytes reached the client yet.
                    pool.record_transient(proxy_entry, message="upstream stall")
                    _inc_counter("errors")
                    if streaming_client is not None:
                        _release_client_in_use(proxy_entry.url)
                    last_error = JSONResponse(
                        status_code=502,
                        content={
                            "error": {
                                "message": "Upstream timed out.",
                                "type": "upstream_error",
                                "code": "upstream_timeout",
                            }
                        },
                    )
                    logger.warning(
                        f"Stream proxy {_mask_proxy_url(proxy_entry.url)} upstream "
                        f"stall: {type(e).__name__}: {e} "
                        f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                    )
                except Exception as e:
                    pool.record_transient(proxy_entry, message="pre-stream error")
                    _inc_counter("errors")
                    if streaming_client is not None:
                        _release_client_in_use(proxy_entry.url)
                    # ANY exception before _proxy_stream returns a response is
                    # retry-safe — no bytes reached the client yet. This covers
                    # protocol errors at header-wait, not just connect failures,
                    # matching the non-streaming path.
                    last_error = JSONResponse(
                        status_code=502,
                        content={
                            "error": {
                                "message": "Upstream error.",
                                "type": "upstream_error",
                                "code": "upstream_error",
                            }
                        },
                    )
                    logger.warning(
                        f"Stream proxy {_mask_proxy_url(proxy_entry.url)} error "
                        f"before response: {type(e).__name__}: {e} "
                        f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                    )
            finally:
                # Release the semaphore we ACQUIRED — a concurrent reload may
                # have swapped the module global; releasing that would over-
                # credit the new semaphore. Only release when _proxy_stream
                # did NOT take ownership (exception paths) — a handed-off
                # semaphore is released by the stream generator or the
                # error-return path inside _proxy_stream.
                if not semaphore_handed_off:
                    acquired_sem.release()

        # All retries exhausted
        if last_error:
            logger.error(
                f"Stream request failed after {attempt}/{MAX_REQUEST_RETRIES} attempts "
                f"across {len(tried_urls)} proxies"
            )
            return last_error

        if MAX_REQUEST_RETRIES <= 0:
            # Loop never ran — no attempt was made. Don't claim "all proxies
            # cooling" when the real issue is retries disabled.
            logger.error("MAX_REQUEST_RETRIES <= 0 — no upstream attempt made")
            return JSONResponse(
                status_code=503,
                content={
                    "error": {
                        "message": "No upstream attempts configured (MAX_REQUEST_RETRIES <= 0).",
                        "type": "configuration_error",
                        "code": "retries_disabled",
                    }
                },
            )

    # Non-streaming: retry with different proxies on transient failure
    last_error = None
    attempt = 0
    tried_urls: set[str] = set()
    dup_scan = 0  # consecutive already-tried returns (rotation stall guard)

    while attempt < MAX_REQUEST_RETRIES:
        # Exponential backoff before a RETRY (not the first attempt) —
        # kinder to the upstream during a failure cascade.
        if last_error is not None and RETRY_BACKOFF_BASE > 0:
            await asyncio.sleep(min(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)), RETRY_BACKOFF_MAX))
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
            # A full rotation of duplicates means every untried proxy is
            # currently cooling (next() only returns available proxies) —
            # stop rather than spinning on already-tried proxies.
            dup_scan += 1
            if dup_scan >= pool.total:
                logger.warning(
                    f"All untried proxies cooling, stopping retry loop "
                    f"({len(tried_urls)} tried, {pool.total} total)"
                )
                break
            continue
        tried_urls.add(proxy_entry.url)
        dup_scan = 0
        attempt += 1

        # First attempt may queue for capacity (full wait); retries fail
        # fast instead of stacking more long waits on a failing request.
        wait = SEMAPHORE_WAIT_SECONDS if last_error is None else RETRY_SEMAPHORE_WAIT_SECONDS
        acquired_sem = await _acquire_semaphore(wait)
        if acquired_sem is None:
            logger.warning(
                f"Semaphore busy for {wait}s — returning 503 "
                f"(concurrency={MAX_CONCURRENT_UPSTREAM})"
            )
            return JSONResponse(
                status_code=503,
                content={
                    "error": {
                        "message": "Relay at capacity — try again later.",
                        "type": "overloaded_error",
                        "code": "relay_at_capacity",
                    }
                },
                headers={"Retry-After": "10"},
            )
        try:
            try:
                async with _borrow_client(proxy_entry.url) as client:
                    resp = await _proxy_single(client, method, upstream_url,
                                              req_headers, body, proxy_entry)
                # Smart auth switching: a 401 means the request REACHED
                # upstream and was rejected for credentials. Observe it;
                # on the trigger threshold, probe alternate auth types and
                # switch if one verifies, then retry ONCE with the new auth.
                if AUTH_SWITCH_ENABLED:
                    auth_switcher.observe(resp.status_code)
                    if resp.status_code == 401 and auth_switcher.should_probe():
                        if await auth_switcher.probe_and_switch():
                            req_headers = _build_headers(dict(headers))
                            # Re-BORROW the pooled client for the retry: the
                            # first `async with _borrow_client` above has
                            # ALREADY exited, so `client` is no longer marked
                            # in-use. Reusing it unlocked would let _get_client's
                            # LRU eviction (pool at cap) or _prune_client_pool
                            # aclose() it mid-flight, aborting the retry and
                            # misattributing the failure to the proxy. The
                            # streaming path re-borrows (_make_streaming_client);
                            # this path must match.
                            async with _borrow_client(proxy_entry.url) as client2:
                                resp = await _proxy_single(
                                    client2, method, upstream_url, req_headers,
                                    body, proxy_entry)
                # Success or final error (4xx from upstream) — return immediately
                if resp.status_code < 500 or resp.status_code == 429:
                    return resp
                # 5xx upstream error — retryable
                last_error = resp
                logger.warning(
                    f"Upstream 5xx on {_mask_proxy_url(proxy_entry.url)} "
                    f"({resp.status_code}), retrying... "
                    f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                )
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                pool.record_timeout(proxy_entry)
                _inc_counter("errors")
                last_error = JSONResponse(
                    status_code=502,
                    content={
                        "error": {
                            "message": "Proxy connection failed.",
                            "type": "proxy_error",
                            "code": "proxy_connect_failed",
                        }
                    },
                )
                logger.warning(
                    f"Proxy {_mask_proxy_url(proxy_entry.url)} connect failed: {e} "
                    f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                )
            except (httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                # Upstream stalled after the proxy connected — the proxy is
                # likely fine. Short cooldown, NOT counted toward permanent
                # death (a flaky upstream must not kill good proxies).
                pool.record_transient(proxy_entry, message="upstream stall")
                _inc_counter("errors")
                last_error = JSONResponse(
                    status_code=502,
                    content={
                        "error": {
                            "message": "Upstream timed out.",
                            "type": "upstream_error",
                            "code": "upstream_timeout",
                        }
                    },
                )
                logger.warning(
                    f"Proxy {_mask_proxy_url(proxy_entry.url)} upstream stall: "
                    f"{type(e).__name__}: {e} "
                    f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                )
            except Exception as e:
                pool.record_transient(proxy_entry, message="upstream error")
                _inc_counter("errors")
                last_error = JSONResponse(
                    status_code=502,
                    content={
                        "error": {
                            "message": "Upstream error.",
                            "type": "upstream_error",
                            "code": "upstream_error",
                        }
                    },
                )
                logger.warning(
                    f"Proxy {_mask_proxy_url(proxy_entry.url)} error: "
                    f"{type(e).__name__}: {e} "
                    f"(attempt {attempt}/{MAX_REQUEST_RETRIES})"
                )
        finally:
            # Release the semaphore we ACQUIRED (see stream path note)
            acquired_sem.release()

    # All retries exhausted
    if last_error:
        logger.error(
            f"Request failed after {attempt}/{MAX_REQUEST_RETRIES} attempts "
            f"across {len(tried_urls)} proxies"
        )
        return last_error

    # If no retries happened and still no proxy (all cooling mid-loop)
    if MAX_REQUEST_RETRIES <= 0:
        # Loop never ran — no attempt was made. Don't claim "all proxies
        # cooling" when the real issue is retries disabled.
        logger.error("MAX_REQUEST_RETRIES <= 0 — no upstream attempt made")
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "message": "No upstream attempts configured (MAX_REQUEST_RETRIES <= 0).",
                    "type": "configuration_error",
                    "code": "retries_disabled",
                }
            },
        )

    # Unreachable in practice (every loop exit sets last_error or returns),
    # kept to satisfy the type checker.
    return JSONResponse(  # pragma: no cover
        status_code=503,
        content={
            "error": {
                "message": "No proxy available.",
                "type": "proxy_error",
                "code": "no_proxy_available",
            }
        },
    )


async def _proxy_single(client, method, url, headers, body, proxy_entry, probe: bool = False) -> Response:
    """Single-shot proxy: forward request, decompress response, relay headers.

    Strips Content-Encoding, Transfer-Encoding, and Content-Length from
    response headers because httpx auto-decompresses gzip/deflate/brotli
    and the response body length changes.

    The response body is read as a stream with a MAX_RESPONSE_SIZE cap —
    a runaway upstream must not be able to make the relay buffer an
    unbounded response (request bodies are capped via MAX_BODY_SIZE;
    responses previously were not).

    `probe=True` suppresses ALL pool/request-count side effects (429
    cooling, timeout cooling, success/latency recording) — a read-only
    health probe must not degrade production pool state just because the
    upstream answered 429 to a cheap /models call. The caller is
    responsible for classifying exceptions (ConnectError → cool proxy).
    """
    t0 = time.monotonic()
    req = client.build_request(method, url, headers=headers, content=body)
    resp = await client.send(req, stream=True)

    if not probe:
        if resp.status_code == 429:
            retry_after = _parse_retry_after(resp.headers)
            pool.record_429(proxy_entry, retry_after)
            _inc_counter("errors")
            logger.warning(f"429 on {_mask_proxy_url(proxy_entry.url)} — cooling for {retry_after}s")
        elif resp.status_code >= 400:
            _inc_counter("errors")
            # Only cool the proxy for proxy-related 4xx (407 proxy auth,
            # 408 request timeout, 425 too early). Client errors (400/401/
            # 403/404/422...) are NOT the proxy's fault — relay them without
            # degrading the pool, otherwise a single bad client request
            # rotates through and cools every proxy. 502/504 through a SOCKS
            # relay indicate the proxy's upstream connection failed — cool it
            # too so dead proxies leave rotation.
            if resp.status_code in (407, 408, 425, 502, 504):
                pool.record_timeout(proxy_entry)
        elif resp.status_code < 300:
            pool.record_success(proxy_entry)
            _inc_counter("ok")
        # 3xx is NEUTRAL: a redirect/captive-portal response proves nothing
        # about proxy health. Counting it as success would revive a
        # permanently-dead proxy and clear its error counters.

    # Read the body with the size cap.
    chunks = []
    total = 0
    try:
        async for chunk in resp.aiter_bytes():
            total += len(chunk)
            if MAX_RESPONSE_SIZE > 0 and total > MAX_RESPONSE_SIZE:
                if not probe:
                    pool.record_transient(proxy_entry, message="response too large")
                    _inc_counter("errors")
                logger.warning(
                    f"Response via {_mask_proxy_url(proxy_entry.url)} exceeded "
                    f"MAX_RESPONSE_SIZE ({MAX_RESPONSE_SIZE} bytes) — aborted"
                )
                return JSONResponse(
                    status_code=502,
                    content={
                        "error": {
                            "message": f"Upstream response exceeded MAX_RESPONSE_SIZE ({MAX_RESPONSE_SIZE} bytes).",
                            "type": "upstream_error",
                            "code": "response_too_large",
                        }
                    },
                )
            chunks.append(chunk)
    finally:
        await resp.aclose()

    latency_ms = (time.monotonic() - t0) * 1000
    # Record latency for non-429 success (matches the pre-stream-read
    # semantics: full-response time; probes record too).
    if resp.status_code < 300:
        pool.record_latency(proxy_entry, latency_ms)

    resp_headers = {}
    for key, val in resp.headers.items():
        lkey = key.lower()
        if lkey in ("transfer-encoding", "content-encoding", "content-length"):
            continue
        resp_headers[key] = val

    return Response(
        content=b"".join(chunks),
        status_code=resp.status_code,
        headers=resp_headers,
        media_type=resp.headers.get("content-type"),
    )


async def _proxy_stream(client, method, url, headers, body, proxy_entry,
                        acquired_sem=None) -> StreamingResponse:
    """Streaming proxy: forward chunked response, relaying upstream headers.

    Uses client.send(req, stream=True) instead of client.stream() so the
    upstream response headers are available before the StreamingResponse
    is constructed — this lets us forward x-request-id, openai-*,
    x-ratelimit-*, and other headers that clients rely on.

    `acquired_sem` is the concurrency semaphore held for this request.
    On error returns (429/4xx/5xx plain Response) it is released here
    before returning; on success the StreamingResponse's generator owns
    it and releases when the stream finishes — so the upstream
    concurrency slot is held for the WHOLE stream, not just connection
    setup. The caller must NOT release a handed-off semaphore.
    """
    req = client.build_request(method, url, headers=headers, content=body)
    t0 = time.monotonic()
    resp = await client.send(req, stream=True)
    # Time-to-first-byte: the response headers have arrived, which is the
    # meaningful latency metric for streams (the body streams afterwards).
    # Only recorded for success-ish statuses — fast error responses (429,
    # 4xx) would skew the pool's latency stats and make a rate-limited
    # proxy look fast. Matches _proxy_single's < 400 threshold.
    if resp.status_code < 400:
        latency_ms = (time.monotonic() - t0) * 1000
        pool.record_latency(proxy_entry, latency_ms)

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
        _inc_counter("errors")
        error_body = await resp.aread()
        await resp.aclose()
        if acquired_sem is not None:
            acquired_sem.release()
        # The client is POOLED — releasing the borrow (not aclose) keeps
        # the warm connection available for the next request. A broken
        # transport stays broken but is harmless: the next borrow that
        # fails to connect simply cools the proxy and retries another.
        _release_client_in_use(proxy_entry.url)
        return Response(
            content=error_body,
            status_code=429,
            headers=resp_headers,
            media_type="application/json",
        )

    if resp.status_code >= 400:
        # Only cool for proxy-related 4xx (see _proxy_single for rationale).
        # 502/504 through a SOCKS relay indicate the proxy's upstream
        # connection failed — cool it too so dead proxies leave rotation.
        if resp.status_code in (407, 408, 425, 502, 504):
            pool.record_timeout(proxy_entry)
        _inc_counter("errors")
        error_body = await resp.aread()
        await resp.aclose()
        if acquired_sem is not None:
            acquired_sem.release()
        _release_client_in_use(proxy_entry.url)
        return Response(
            content=error_body,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type="application/json",
        )

    # ── Success — stream the body ────────────────────────────────
    pool.record_success(proxy_entry)
    _inc_counter("ok")

    # If the upstream is sending SSE, error objects must be `data:`-framed
    # so OpenAI-style clients parse them instead of hitting a protocol error.
    is_sse = (resp.headers.get("content-type", "") or "").startswith("text/event-stream")

    def _error_chunk(payload: dict) -> bytes:
        if is_sse:
            return f"data: {json.dumps(payload)}\n\n".encode()
        return json.dumps(payload).encode()

    # Exactly-once semaphore release, shared by the generator's finally
    # and the GC finalizer below. The lock makes check-and-set atomic —
    # a double release would over-credit the semaphore and let requests
    # exceed MAX_CONCURRENT_UPSTREAM.
    _sem_release_lock = threading.Lock()
    _sem_released = False
    _sem = acquired_sem
    if _sem is not None and not HOLD_PERMIT_FOR_STREAM:
        # Opt-in escape hatch: the permit only gates CONNECTION SETUP, not
        # stream lifetime. Release it now that headers have arrived; the
        # generator must NOT release again. Trade-off: unbounded concurrent
        # streams can saturate the upstream request queue (observed 503s)
        # — HOLD_PERMIT_FOR_STREAM=true (default) is the safe setting.
        _sem.release()
        _sem = None

    def _release_sem():
        nonlocal _sem_released
        if _sem is None:
            return
        with _sem_release_lock:
            if not _sem_released:
                _sem_released = True
                _sem.release()

    # Exactly-once client borrow release (same rationale as the semaphore:
    # generator finally + GC finalizer both run for a stream; a double
    # release would un-borrow a SECOND stream's hold on the same pooled
    # client and let it be evicted mid-flight).
    _client_release_lock = threading.Lock()
    _client_released = False

    def _release_client_once():
        nonlocal _client_released
        with _client_release_lock:
            if not _client_released:
                _client_released = True
                _release_client_in_use(proxy_entry.url)

    async def _generate():
        try:
            async for chunk in resp.aiter_bytes():
                if _stream_shutdown_event.is_set():
                    yield _error_chunk({
                        "error": {"message": "Server shutting down", "type": "shutdown_error"}
                    })
                    return
                yield chunk
        except Exception as e:
            pool.record_transient(proxy_entry, message="mid-stream error")
            _inc_counter("errors")
            # Never emit the raw exception to the client — it may embed
            # socket/proxy/upstream internals. Log it server-side only.
            logger.error(f"Stream error on {_mask_proxy_url(proxy_entry.url)}: {type(e).__name__}: {e}")
            yield _error_chunk({
                "error": {"message": "Stream interrupted.", "type": "stream_error"}
            })
        finally:
            await resp.aclose()
            # Pooled client — release the borrow so it can serve the next
            # stream; the underlying connection is NOT torn down per stream.
            _release_client_once()
            # The concurrency slot is held for the stream's entire
            # lifetime — release it only when the generator finishes
            # (client disconnect, upstream EOF, or shutdown).
            _release_sem()

    gen = _generate()
    # If the client disconnects BEFORE the response starts, Starlette
    # never iterates the generator — its finally never runs and both the
    # semaphore permit AND the client borrow would leak forever. The
    # finalizer fires when the generator is GC'd (verified: also for a
    # NEVER-STARTED async generator) and releases through the same guarded
    # paths → exactly-once for both.
    weakref.finalize(gen, _release_client_once)
    if _sem is not None:
        weakref.finalize(gen, _release_sem)

    return StreamingResponse(
        gen,
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
        f"\u2192 {_mask_proxy_url(UPSTREAM_BASE)} "
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
    """Log structured request info with timing.

    /health is polled frequently by orchestrators and MCP tools — log it
    at DEBUG to keep INFO logs focused on real traffic. Query strings are
    redacted of credential-looking params (api_key, token, key, etc.)
    so secrets never reach the logs.
    """
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = (time.monotonic() - start) * 1000
    qs = request.url.query or ""
    if qs:
        qs = _redact_query(qs)
    log_line = (
        f"{request.method} {request.url.path}"
        f"{('?' + qs) if qs else ''} "
        f"\u2192 {response.status_code} ({duration_ms:.0f}ms)"
    )
    if RELAY_LOG_REQUESTS:
        # /health is polled frequently by orchestrators/MCP — DEBUG it so INFO
        # focuses on real traffic. Other requests log at INFO.
        if request.url.path == "/health":
            logger.debug(log_line)
        else:
            logger.info(log_line)
    return response


# ╔══════════════════════════════════════════════════════════════════╗
# ║  CORS middleware (allow web clients)                            ║
# ╚══════════════════════════════════════════════════════════════════╝

# allow_credentials=False is deliberate: the relay authenticates via
# Authorization / X-API-Key headers (non-credentialed CORS), never cookies.
# With allow_origins=["*"] + allow_credentials=True, Starlette reflects ANY
# Origin for credentialed requests — turning an open relay into a browser-
# usable proxy for any website. Headers still work without credentials mode.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    # Relay upstream headers (x-request-id, openai-*, x-ratelimit-*) are
    # invisible to browser JS without this — they'd be restricted to the
    # CORS-safelisted set, contradicting browser-client compatibility.
    expose_headers=["*"],
)


# ── Admin auth middleware (optional) ────────────────────────────
@app.middleware("http")
async def admin_auth(request: Request, call_next):
    """If ADMIN_API_KEY is set, require X-Admin-Key header on /admin/* routes."""
    if request.url.path.startswith("/admin/") and ADMIN_API_KEY:
        provided = request.headers.get("x-admin-key", "")
        # Constant-time compare — plain != would short-circuit on the
        # first mismatching byte, leaking key prefix length via timing.
        if not secrets.compare_digest(provided, ADMIN_API_KEY):
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
        # Masked — an upstream URL with embedded user:pass@ must not leak
        # credentials to unauthenticated health pollers. Identity for
        # credential-less URLs (the common case).
        "upstream_base": _mask_proxy_url(UPSTREAM_BASE),
        "models_available": len(MODELS_CACHE) if MODELS_CACHE else 0,
        "request_stats": dict(_request_count),
        "semaphore": {"max": MAX_CONCURRENT_UPSTREAM, "used": MAX_CONCURRENT_UPSTREAM - semaphore._value, "queued": _waiting_count},
        "uptime_seconds": int(time.monotonic() - _START_TIME),
        "version": VERSION,
        "shared_clients": len(_client_pool),
        "max_body_size": MAX_BODY_SIZE,
        "security": {
            "client_auth_enabled": bool(CLIENT_API_KEY),
            "admin_auth_enabled": bool(ADMIN_API_KEY),
        },
        "auth_switch": auth_switcher.status(),
    }


@app.get("/v1/models", response_model=None)
async def list_models(request: Request = None):
    # Gate with client auth when configured — model names are metadata but
    # should not be exposed to unauthenticated clients on an open relay.
    headers = dict(request.headers) if request is not None else {}
    if CLIENT_API_KEY and not _client_key_valid(headers):
        _inc_counter("auth_failed")
        return JSONResponse(
            status_code=401,
            content={
                "error": {
                    "message": "Invalid or missing client API key.",
                    "type": "authentication_error",
                    "code": "invalid_client_key",
                }
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not UPSTREAM_BASE:
        return {"object": "list", "data": []}

    # Check cache freshness
    now = time.monotonic()
    if MODELS_CACHE and (now - MODELS_CACHE_UPDATED) < MODELS_CACHE_TTL:
        return {"object": "list", "data": list(MODELS_CACHE)}

    try:
        # Route through the proxy pool — a direct client would leak the
        # relay's real IP to the upstream (defeats the proxy's purpose).
        headers = {}
        if UPSTREAM_AUTH_TYPE == "x-api-key":
            headers["x-api-key"] = UPSTREAM_API_KEY
        else:
            headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"

        # Retry across proxies on connect failure — one dead proxy must not
        # stall a cold-cache models refresh (the old code gave up after one).
        for attempt in range(MAX_REQUEST_RETRIES):
            proxy_entry = pool.next()
            if proxy_entry is None:
                logger.warning("All proxies cooling — cannot refresh models, serving cache")
                return {"object": "list", "data": list(MODELS_CACHE)}

            # Respect the concurrency limit — the models refresh is an upstream
            # call too; bypassing the semaphore could exceed
            # MAX_CONCURRENT_UPSTREAM when a flood of /v1/models requests hits
            # a cold cache.
            acquired_sem = await _acquire_semaphore(SEMAPHORE_WAIT_SECONDS)
            if acquired_sem is None:
                logger.warning("Semaphore busy — serving cached models")
                return {"object": "list", "data": list(MODELS_CACHE)}
            try:
                async with _borrow_client(proxy_entry.url) as client:
                    resp = await _proxy_single(
                        client,
                        "GET", f"{UPSTREAM_BASE}/models", headers, None, proxy_entry,
                    )
                if resp.status_code == 200:
                    data = json.loads(resp.body.decode()).get("data", [])
                    filtered = [m for m in data if _model_allowed(m.get("id", ""))]
                    _update_models_cache(filtered)
                    return {"object": "list", "data": filtered}
                # Non-200 (401/429/5xx): _proxy_single already recorded pool
                # effects; serve the cache — retrying won't change the status.
                break
            except (httpx.ConnectError, httpx.ConnectTimeout):
                # Dead proxy — cool it so the next real request doesn't pay the
                # connect timeout too (the generic handler below would swallow
                # the exception without touching the pool, leaving the dead
                # proxy in rotation indefinitely).
                pool.record_timeout(proxy_entry)
                logger.warning(
                    f"Models refresh connect failure via {_mask_proxy_url(proxy_entry.url)} "
                    f"({attempt + 1}/{MAX_REQUEST_RETRIES}) — cooled, trying next proxy"
                )
            finally:
                acquired_sem.release()
    except Exception as e:
        logger.warning(f"Failed to refresh models: {e}")

    return {"object": "list", "data": list(MODELS_CACHE)}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    # Auth BEFORE reading the body — an unauthenticated attacker must not
    # be able to make us buffer up to MAX_BODY_SIZE bytes per request.
    if not _client_key_valid(dict(request.headers)):
        _inc_counter("auth_failed")
        return _client_auth_error()
    body = await _read_body_capped(request)
    if body is None:
        logger.warning(f"Request body exceeds MAX_BODY_SIZE ({MAX_BODY_SIZE} bytes)")
        return JSONResponse(
            status_code=413,
            content={
                "error": {
                    "message": f"Request body too large (max {MAX_BODY_SIZE} bytes).",
                    "type": "payload_too_large",
                    "code": "body_too_large",
                }
            },
        )
    headers = dict(request.headers)
    return await _proxy_request(
        "POST", "/chat/completions", body, headers, request.url.query or "",
    )


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy_all(path: str, request: Request):
    # Auth BEFORE reading the body (see chat_completions).
    if not _client_key_valid(dict(request.headers)):
        _inc_counter("auth_failed")
        return _client_auth_error()
    body = None
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        # DELETE is included: some APIs (e.g. file/fine-tune cleanup
        # endpoints) send a JSON body with DELETE. Dropping it would
        # silently mutate the upstream request semantics.
        body = await _read_body_capped(request)
        if body is None:
            logger.warning(f"Request body exceeds MAX_BODY_SIZE ({MAX_BODY_SIZE} bytes)")
            return JSONResponse(
                status_code=413,
                content={
                    "error": {
                        "message": f"Request body too large (max {MAX_BODY_SIZE} bytes).",
                        "type": "payload_too_large",
                        "code": "body_too_large",
                    }
                },
            )
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
    proxy_entry = None
    try:
        # Route through the proxy pool — never hit the upstream directly
        # (the admin health check must reflect the real proxied path).
        proxy_entry = pool.next()
        if proxy_entry is None:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "upstream": _mask_proxy_url(UPSTREAM_BASE),
                    "error": "All proxies cooling — cannot reach upstream",
                    "latency_ms": 0,
                },
            )

        headers = {}
        if UPSTREAM_AUTH_TYPE == "x-api-key":
            headers["x-api-key"] = UPSTREAM_API_KEY
        else:
            headers["Authorization"] = f"Bearer {UPSTREAM_API_KEY}"

        # Respect the concurrency limit — the health check is an upstream
        # call and must not bypass MAX_CONCURRENT_UPSTREAM.
        acquired_sem = await _acquire_semaphore(SEMAPHORE_WAIT_SECONDS)
        if acquired_sem is None:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "upstream": _mask_proxy_url(UPSTREAM_BASE),
                    "error": "Relay at capacity — cannot run health check",
                    "latency_ms": round((time.monotonic() - t0) * 1000, 1),
                },
            )
        try:
            async with _borrow_client(proxy_entry.url) as client:
                resp = await _proxy_single(
                    client,
                    "GET", f"{UPSTREAM_BASE}/models", headers, None, proxy_entry,
                    probe=True,  # read-only probe — must not cool the pool
                )
        finally:
            acquired_sem.release()
        latency_ms = (time.monotonic() - t0) * 1000
        models_count = 0
        if resp.status_code == 200:
            try:
                models_count = len(json.loads(resp.body.decode()).get("data", []))
            except Exception:
                models_count = 0
        # Only a 200 proves the upstream is healthy — a 401 (wrong key),
        # 404 (bad path) or 5xx must report degraded, not "ok".
        return {
            "status": "ok" if resp.status_code == 200 else "degraded",
            "upstream": _mask_proxy_url(UPSTREAM_BASE),
            "upstream_status": resp.status_code,
            "latency_ms": round(latency_ms, 1),
            "models_count": models_count,
        }
    except (httpx.ConnectError, httpx.ConnectTimeout) as e:
        # Same failure class as the request path (proxy_connect_failed →
        # 502) — a dead proxy must not look like a relay outage (503).
        # Also cool the proxy: probe=True skipped response-based cooling,
        # but a connect failure IS proxy-attributable — without this the
        # dead proxy would stay in rotation indefinitely.
        if proxy_entry is not None:
            pool.record_timeout(proxy_entry)
        _proxy_for_log = _mask_proxy_url(proxy_entry.url) if proxy_entry else "?"
        logger.warning(f"upstream-health connect failure via {_proxy_for_log}: {type(e).__name__}: {e}")
        latency_ms = (time.monotonic() - t0) * 1000
        return JSONResponse(
            status_code=502,
            content={
                "status": "error",
                "upstream": _mask_proxy_url(UPSTREAM_BASE),
                "error": "proxy_connect_failed",
                "latency_ms": round(latency_ms, 1),
            },
        )
    except (httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
        _proxy_for_log = _mask_proxy_url(proxy_entry.url) if proxy_entry else "?"
        logger.warning(f"upstream-health timeout via {_proxy_for_log}: {type(e).__name__}: {e}")
        latency_ms = (time.monotonic() - t0) * 1000
        return JSONResponse(
            status_code=502,
            content={
                "status": "error",
                "upstream": _mask_proxy_url(UPSTREAM_BASE),
                "error": "upstream_timeout",
                "latency_ms": round(latency_ms, 1),
            },
        )
    except Exception as e:
        # Never emit the raw exception — it may embed socket/proxy details.
        logger.error(f"upstream-health failed: {type(e).__name__}: {e}")
        latency_ms = (time.monotonic() - t0) * 1000
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "upstream": _mask_proxy_url(UPSTREAM_BASE),
                "error": "Health check failed",
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
        logger.info(f"Proxy reset (admin): {_mask_proxy_url(url)}")
        return {"status": "ok", "message": f"Proxy reset: {_mask_proxy_url(url)}"}
    return JSONResponse(
        status_code=404,
        content=({"error": f"Proxy not found in pool: {_mask_proxy_url(url)}"}),
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
    # Unvalidated input (string/bool/None from the JSON body) would raise
    # TypeError inside reset_by_errors → unhandled 500. Coerce defensively.
    try:
        min_errs = int(min_errs)
    except (TypeError, ValueError):
        min_errs = CONSECUTIVE_ERROR_THRESHOLD
    reset_count = pool.reset_by_errors(min_errs)
    logger.info(f"Reset {reset_count} permanently-failed proxies (admin)")
    return {"status": "ok", "message": f"Reset {reset_count} proxies"}


def _reload_upstream_config():
    """Re-read config.json/env and update upstream settings in place.

    Updates UPSTREAM_BASE, UPSTREAM_API_KEY, UPSTREAM_AUTH_TYPE,
    ADMIN_API_KEY, CLIENT_API_KEY and proxy list without a process
    restart. Env vars still win.
    """
    global UPSTREAM_BASE, UPSTREAM_API_KEY, UPSTREAM_AUTH_TYPE
    global MAX_CONCURRENT_UPSTREAM, MODEL_FILTER_PATTERN, SEMAPHORE_WAIT_SECONDS
    global PROXY_LIST_FILE, PROXY_LIST_ENV, _model_filter_re, PROXY_HEALTH_CHECK_URL
    global MODELS_CACHE, MODELS_CACHE_UPDATED, CLIENT_API_KEY, MAX_BODY_SIZE
    global HEALTH_FAIL_THRESHOLD, ADMIN_API_KEY, PROXY_HEALTH_CHECK_INTERVAL
    global CONSECUTIVE_ERROR_THRESHOLD, PERMANENT_COOLDOWN_SECONDS, MAX_RETRY_AFTER_SECONDS
    global AUTH_SWITCH_ENABLED, AUTH_SWITCH_CANDIDATES, AUTH_STATE_PATH
    global AUTH_SWITCH_TRIGGER_THRESHOLD, AUTH_SWITCH_PROBE_SUCCESSES
    global AUTH_SWITCH_COOLDOWN_S, AUTH_SWITCH_MAX_PER_WINDOW, AUTH_SWITCH_WINDOW_S
    # Runtime-reloadable concurrency knobs. RELAY_WORKERS, RELAY_MAX_CONNECTIONS
    # and RELAY_BACKLOG are deliberately NOT here — uvicorn's worker count and
    # inbound limits are fixed at launch and cannot change live.
    global MAX_QUEUED_REQUESTS, HOLD_PERMIT_FOR_STREAM, HEALTH_CHECK_CONCURRENCY
    global RETRY_SEMAPHORE_WAIT_SECONDS, RETRY_BACKOFF_BASE, RETRY_BACKOFF_MAX
    global LATENCY_SKIP_THRESHOLD_MS, CLIENT_IDLE_TTL, MAX_RESPONSE_SIZE
    global UPSTREAM_CONNECT_TIMEOUT, UPSTREAM_READ_TIMEOUT, RELAY_LOG_REQUESTS
    file_cfg = _load_config_file(_CONFIG_PATH) if _CONFIG_PATH else {}
    merged = _merge_config(file_cfg)

    UPSTREAM_BASE = str(merged["UPSTREAM_BASE"]).rstrip("/")
    UPSTREAM_API_KEY = str(merged["UPSTREAM_API_KEY"])
    UPSTREAM_AUTH_TYPE = str(merged["UPSTREAM_AUTH_TYPE"]).lower()
    ADMIN_API_KEY = str(os.environ.get("ADMIN_API_KEY") or merged.get("ADMIN_API_KEY", ""))
    CLIENT_API_KEY = str(os.environ.get("CLIENT_API_KEY") or merged.get("CLIENT_API_KEY", ""))
    MAX_CONCURRENT_UPSTREAM = int(merged["MAX_CONCURRENT_UPSTREAM"])
    MAX_QUEUED_REQUESTS = int(merged["MAX_QUEUED_REQUESTS"])
    HOLD_PERMIT_FOR_STREAM = str(merged["HOLD_PERMIT_FOR_STREAM"]).lower() in ("1", "true", "yes", "on")
    HEALTH_CHECK_CONCURRENCY = int(merged["HEALTH_CHECK_CONCURRENCY"])
    RETRY_SEMAPHORE_WAIT_SECONDS = float(os.environ.get("RETRY_SEMAPHORE_WAIT_SECONDS") or
        str(merged.get("RETRY_SEMAPHORE_WAIT_SECONDS", 2.0)))
    RETRY_BACKOFF_BASE = float(os.environ.get("RETRY_BACKOFF_BASE") or
        str(merged.get("RETRY_BACKOFF_BASE", 0.1)))
    RETRY_BACKOFF_MAX = float(os.environ.get("RETRY_BACKOFF_MAX") or
        str(merged.get("RETRY_BACKOFF_MAX", 1.0)))
    LATENCY_SKIP_THRESHOLD_MS = float(os.environ.get("LATENCY_SKIP_THRESHOLD_MS") or
        str(merged.get("LATENCY_SKIP_THRESHOLD_MS", 0)))
    CLIENT_IDLE_TTL = float(os.environ.get("CLIENT_IDLE_TTL") or
        str(merged.get("CLIENT_IDLE_TTL", 120)))
    MAX_RESPONSE_SIZE = int(os.environ.get("MAX_RESPONSE_SIZE") or
        str(merged.get("MAX_RESPONSE_SIZE", 200 * 1024 * 1024)))
    UPSTREAM_CONNECT_TIMEOUT = float(os.environ.get("UPSTREAM_CONNECT_TIMEOUT") or
        str(merged.get("UPSTREAM_CONNECT_TIMEOUT", 15)))
    UPSTREAM_READ_TIMEOUT = float(os.environ.get("UPSTREAM_READ_TIMEOUT") or
        str(merged.get("UPSTREAM_READ_TIMEOUT", 120)))
    RELAY_LOG_REQUESTS = str(os.environ.get("RELAY_LOG_REQUESTS") or
        str(merged.get("RELAY_LOG_REQUESTS", "true"))).lower() in ("1", "true", "yes", "on")
    SEMAPHORE_WAIT_SECONDS = float(os.environ.get("SEMAPHORE_WAIT_SECONDS") or
        str(merged.get("SEMAPHORE_WAIT_SECONDS", 30.0)))
    MODEL_FILTER_PATTERN = str(merged["MODEL_FILTER_PATTERN"])
    _model_filter_re = re.compile(MODEL_FILTER_PATTERN)
    PROXY_LIST_FILE = os.environ.get("PROXY_LIST") or str(merged.get("PROXY_LIST", ""))
    PROXY_LIST_ENV = os.environ.get("PROXY_LIST_ENV") or str(merged.get("PROXY_LIST_ENV", ""))
    PROXY_HEALTH_CHECK_URL = str(os.environ.get("PROXY_HEALTH_CHECK_URL") or
        str(merged.get("PROXY_HEALTH_CHECK_URL", "http://httpbin.org/ip")))
    PROXY_HEALTH_CHECK_INTERVAL = int(os.environ.get("PROXY_HEALTH_CHECK_INTERVAL") or
        str(merged.get("PROXY_HEALTH_CHECK_INTERVAL", 60)))
    MAX_BODY_SIZE = int(os.environ.get("MAX_BODY_SIZE") or
        str(merged.get("MAX_BODY_SIZE", 100 * 1024 * 1024)))
    HEALTH_FAIL_THRESHOLD = int(os.environ.get("HEALTH_FAIL_THRESHOLD") or
        str(merged.get("HEALTH_FAIL_THRESHOLD", 3)))
    CONSECUTIVE_ERROR_THRESHOLD = int(os.environ.get("CONSECUTIVE_ERROR_THRESHOLD") or
        str(merged.get("CONSECUTIVE_ERROR_THRESHOLD", 3)))
    PERMANENT_COOLDOWN_SECONDS = int(os.environ.get("PERMANENT_COOLDOWN_SECONDS") or
        str(merged.get("PERMANENT_COOLDOWN_SECONDS", 86400)))
    MAX_RETRY_AFTER_SECONDS = int(os.environ.get("MAX_RETRY_AFTER_SECONDS") or
        str(merged.get("MAX_RETRY_AFTER_SECONDS", 3600)))
    AUTH_SWITCH_ENABLED = str(os.environ.get("AUTH_SWITCH_ENABLED") or
        str(merged.get("AUTH_SWITCH_ENABLED", "true"))).lower() in ("1", "true", "yes", "on")
    AUTH_SWITCH_CANDIDATES = [c.strip().lower() for c in str(
        os.environ.get("AUTH_SWITCH_CANDIDATES") or
        str(merged.get("AUTH_SWITCH_CANDIDATES", "bearer,x-api-key"))
    ).split(",") if c.strip()]
    AUTH_SWITCH_TRIGGER_THRESHOLD = int(os.environ.get("AUTH_SWITCH_TRIGGER_THRESHOLD") or
        str(merged.get("AUTH_SWITCH_TRIGGER_THRESHOLD", 3)))
    AUTH_SWITCH_PROBE_SUCCESSES = int(os.environ.get("AUTH_SWITCH_PROBE_SUCCESSES") or
        str(merged.get("AUTH_SWITCH_PROBE_SUCCESSES", 2)))
    AUTH_SWITCH_COOLDOWN_S = int(os.environ.get("AUTH_SWITCH_COOLDOWN_S") or
        str(merged.get("AUTH_SWITCH_COOLDOWN_S", 300)))
    AUTH_SWITCH_MAX_PER_WINDOW = int(os.environ.get("AUTH_SWITCH_MAX_PER_WINDOW") or
        str(merged.get("AUTH_SWITCH_MAX_PER_WINDOW", 3)))
    AUTH_SWITCH_WINDOW_S = int(os.environ.get("AUTH_SWITCH_WINDOW_S") or
        str(merged.get("AUTH_SWITCH_WINDOW_S", 3600)))
    AUTH_STATE_PATH = os.path.expanduser(str(os.environ.get("AUTH_STATE_PATH") or
        str(merged.get("AUTH_STATE_PATH", "~/.hermes/proxy-relay/auth_state.json"))))
    # Propagate reloaded knobs into the live switcher. The state path
    # change is honored too — a reload after an operator edited config
    # should point persistence at the new location.
    auth_switcher.reconfigure(
        candidates=AUTH_SWITCH_CANDIDATES,
        trigger_threshold=AUTH_SWITCH_TRIGGER_THRESHOLD,
        probe_successes=AUTH_SWITCH_PROBE_SUCCESSES,
        cooldown_s=AUTH_SWITCH_COOLDOWN_S,
        max_per_window=AUTH_SWITCH_MAX_PER_WINDOW,
        window_s=AUTH_SWITCH_WINDOW_S,
        state_path=AUTH_STATE_PATH,
        enabled=AUTH_SWITCH_ENABLED,
    )
    _init_pool()
    _resize_semaphore()
    # The upstream changed — cached models belong to the old endpoint.
    # Invalidate so the next /v1/models fetch pulls from the new upstream
    # instead of serving stale models for up to MODELS_CACHE_TTL seconds.
    MODELS_CACHE.clear()
    MODELS_CACHE_UPDATED = 0.0

    return {
        "status": "ok",
        "message": "Configuration reloaded",
        "upstream_base": _mask_proxy_url(UPSTREAM_BASE),
        "proxies_total": pool.total,
    }


@app.post("/admin/reload-config")
async def admin_reload_config(request: Request):
    """Hot-reload upstream config + proxy list from config.json/env.

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _check_admin_rate_limit(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    try:
        result = _reload_upstream_config()
    except (ValueError, TypeError, re.error) as e:
        # Malformed config.json (e.g. "MAX_CONCURRENT_UPSTREAM": "abc" or a
        # bad MODEL_FILTER_PATTERN) would otherwise bubble up as a raw 500
        # with no error body. Report the bad setting instead — the relay
        # keeps serving with the PREVIOUS good config (the reload failed
        # before mutating globals... note: settings before the failure ARE
        # applied; the user must fix and re-reload).
        logger.error(f"Config reload rejected (bad value): {type(e).__name__}: {e}")
        return JSONResponse(
            status_code=400,
            content={"error": f"Config reload rejected: {type(e).__name__}: {e}"},
        )
    # The proxy list may have changed — close pooled clients for proxies
    # that were removed, so they don't keep connections alive pointlessly.
    # (Matches /admin/reload-proxies behavior — this path was missing it.)
    await _prune_client_pool({p.url for p in pool._proxies})
    logger.info(f"Config reloaded (admin): upstream={_mask_proxy_url(UPSTREAM_BASE)}, {pool.total} proxies")
    return result


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

    # Masked — an upstream URL with embedded user:pass@ must not leak
    # credentials to stdout (piped logs, CI, shared terminals).
    if not UPSTREAM_BASE:
        report("ERROR", "UPSTREAM_BASE is empty — relay cannot proxy requests")
    else:
        print(f"  ✓ UPSTREAM_BASE: {_mask_proxy_url(UPSTREAM_BASE)}")

    if not UPSTREAM_API_KEY:
        report("WARNING", "UPSTREAM_API_KEY is empty — upstream auth will fail")

    if UPSTREAM_AUTH_TYPE not in ("bearer", "x-api-key"):
        report("ERROR", f"Invalid UPSTREAM_AUTH_TYPE: {UPSTREAM_AUTH_TYPE!r} (expected bearer or x-api-key)")

    if not (1 <= int(RELAY_PORT) <= 65535):
        report("ERROR", f"Invalid RELAY_PORT: {RELAY_PORT!r} (expected 1–65535)")
    else:
        print(f"  ✓ RELAY_PORT: {RELAY_PORT}")

    if int(MAX_CONCURRENT_UPSTREAM) < 1:
        report("ERROR", f"Invalid MAX_CONCURRENT_UPSTREAM: {MAX_CONCURRENT_UPSTREAM!r} (expected >= 1)")
    else:
        print(f"  ✓ MAX_CONCURRENT_UPSTREAM: {MAX_CONCURRENT_UPSTREAM}")

    if int(MAX_QUEUED_REQUESTS) < 0:
        report("ERROR", f"Invalid MAX_QUEUED_REQUESTS: {MAX_QUEUED_REQUESTS!r} (expected >= 0; 0 = unlimited backlog)")
    else:
        print(f"  ✓ MAX_QUEUED_REQUESTS: {MAX_QUEUED_REQUESTS} ({'unlimited backlog' if MAX_QUEUED_REQUESTS == 0 else 'bounded backlog'})")

    if int(HEALTH_CHECK_CONCURRENCY) < 1:
        report("ERROR", f"Invalid HEALTH_CHECK_CONCURRENCY: {HEALTH_CHECK_CONCURRENCY!r} (expected >= 1)")
    else:
        print(f"  ✓ HEALTH_CHECK_CONCURRENCY: {HEALTH_CHECK_CONCURRENCY}")

    if int(RELAY_WORKERS) < 1:
        report("ERROR", f"Invalid RELAY_WORKERS: {RELAY_WORKERS!r} (expected >= 1)")
    else:
        print(f"  ✓ RELAY_WORKERS: {RELAY_WORKERS} ({'single process' if RELAY_WORKERS == 1 else 'NOTE: cooldown state is NOT shared across workers'})")

    print(f"  ✓ HOLD_PERMIT_FOR_STREAM: {HOLD_PERMIT_FOR_STREAM} ({'permit held for whole stream (upstream-queue safe)' if HOLD_PERMIT_FOR_STREAM else 'permit released after connection setup (max throughput)'})")

    for name, req in (
        ("UPSTREAM_CONNECT_TIMEOUT", "gt0"),
        ("UPSTREAM_READ_TIMEOUT", "gt0"),
        ("MAX_RESPONSE_SIZE", "ge0"),
        ("CLIENT_IDLE_TTL", "ge0"),
        ("RETRY_BACKOFF_BASE", "ge0"),
        ("RETRY_BACKOFF_MAX", "ge0"),
        ("LATENCY_SKIP_THRESHOLD_MS", "ge0"),
        ("RELAY_MAX_CONNECTIONS", "ge0"),
        ("RELAY_BACKLOG", "ge0"),
    ):
        val = globals()[name]
        ok = (val > 0) if req == "gt0" else (val >= 0)
        if not ok:
            report("ERROR", f"Invalid {name}: {val!r} (expected {'> 0' if req == 'gt0' else '>= 0'})")
        else:
            print(f"  ✓ {name}: {val}")

    if int(MAX_BODY_SIZE) < 0:
        report("WARNING", f"MAX_BODY_SIZE {MAX_BODY_SIZE} < 0 — use 0 to disable the cap")

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
    else:
        # socks5:// resolves the UPSTREAM hostname at the RELAY (leaking DNS +
        # possibly a different CDN IP than the proxy would resolve);
        # socks5h:// resolves at the proxy (anonymity-preserving). Recommend it.
        local_resolve = sum(1 for p in proxies if p.startswith("socks5://"))
        if local_resolve:
            report(
                "WARNING",
                f"{local_resolve} proxy URL(s) use 'socks5://' — DNS for the upstream "
                "hostname is resolved at the relay, not the proxy. Prefer 'socks5h://' "
                "(remote DNS) for privacy and to avoid CDN geo-divergence.",
            )

    if CLIENT_API_KEY:
        print("  ✓ CLIENT_API_KEY: set (client auth required for /v1/*)")
    else:
        print("  ⚠ CLIENT_API_KEY: not set — relay is an open proxy for anyone who can reach it")

    print(f"  ✓ MAX_BODY_SIZE: {MAX_BODY_SIZE} bytes ({'disabled' if MAX_BODY_SIZE <= 0 else 'enabled'})")

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
        global SEMAPHORE_WAIT_SECONDS, CLIENT_API_KEY
        global PROXY_LIST_FILE, PROXY_LIST_ENV, _CONFIG_PATH, PROXY_HEALTH_CHECK_URL
        global CONSECUTIVE_ERROR_THRESHOLD, PERMANENT_COOLDOWN_SECONDS, MAX_BODY_SIZE, HEALTH_FAIL_THRESHOLD
        global MAX_RETRY_AFTER_SECONDS, _model_filter_re, PROXY_HEALTH_CHECK_INTERVAL
        global MAX_QUEUED_REQUESTS, HOLD_PERMIT_FOR_STREAM, HEALTH_CHECK_CONCURRENCY, RELAY_WORKERS
        global RELAY_MAX_CONNECTIONS, RELAY_BACKLOG, UPSTREAM_CONNECT_TIMEOUT, UPSTREAM_READ_TIMEOUT
        global CLIENT_IDLE_TTL, MAX_RESPONSE_SIZE, RETRY_SEMAPHORE_WAIT_SECONDS
        global RETRY_BACKOFF_BASE, RETRY_BACKOFF_MAX, LATENCY_SKIP_THRESHOLD_MS, RELAY_LOG_REQUESTS
        _CONFIG_PATH = os.path.expanduser(args.config)
        _file_cfg = _load_config_file(_CONFIG_PATH)
        _merged = _merge_config(_file_cfg)
        UPSTREAM_BASE = str(_merged["UPSTREAM_BASE"]).rstrip("/")
        UPSTREAM_API_KEY = str(_merged["UPSTREAM_API_KEY"])
        UPSTREAM_AUTH_TYPE = str(_merged["UPSTREAM_AUTH_TYPE"]).lower()
        CLIENT_API_KEY = str(os.environ.get("CLIENT_API_KEY") or _merged.get("CLIENT_API_KEY", ""))
        PROXY_HEALTH_CHECK_URL = str(os.environ.get("PROXY_HEALTH_CHECK_URL") or
            str(_merged.get("PROXY_HEALTH_CHECK_URL", "http://httpbin.org/ip")))
        RELAY_PORT = int(_merged["RELAY_PORT"])
        MAX_CONCURRENT_UPSTREAM = int(_merged["MAX_CONCURRENT_UPSTREAM"])
        MAX_QUEUED_REQUESTS = int(_merged["MAX_QUEUED_REQUESTS"])
        HOLD_PERMIT_FOR_STREAM = str(_merged["HOLD_PERMIT_FOR_STREAM"]).lower() in ("1", "true", "yes", "on")
        HEALTH_CHECK_CONCURRENCY = int(_merged["HEALTH_CHECK_CONCURRENCY"])
        RELAY_WORKERS = int(_merged["RELAY_WORKERS"])
        RELAY_MAX_CONNECTIONS = int(_merged["RELAY_MAX_CONNECTIONS"])
        RELAY_BACKLOG = int(_merged["RELAY_BACKLOG"])
        UPSTREAM_CONNECT_TIMEOUT = float(_merged["UPSTREAM_CONNECT_TIMEOUT"])
        UPSTREAM_READ_TIMEOUT = float(_merged["UPSTREAM_READ_TIMEOUT"])
        CLIENT_IDLE_TTL = float(_merged["CLIENT_IDLE_TTL"])
        MAX_RESPONSE_SIZE = int(_merged["MAX_RESPONSE_SIZE"])
        RETRY_SEMAPHORE_WAIT_SECONDS = float(os.environ.get("RETRY_SEMAPHORE_WAIT_SECONDS") or
            str(_merged.get("RETRY_SEMAPHORE_WAIT_SECONDS", 2.0)))
        RETRY_BACKOFF_BASE = float(os.environ.get("RETRY_BACKOFF_BASE") or
            str(_merged.get("RETRY_BACKOFF_BASE", 0.1)))
        RETRY_BACKOFF_MAX = float(os.environ.get("RETRY_BACKOFF_MAX") or
            str(_merged.get("RETRY_BACKOFF_MAX", 1.0)))
        LATENCY_SKIP_THRESHOLD_MS = float(os.environ.get("LATENCY_SKIP_THRESHOLD_MS") or
            str(_merged.get("LATENCY_SKIP_THRESHOLD_MS", 0)))
        RELAY_LOG_REQUESTS = str(os.environ.get("RELAY_LOG_REQUESTS") or
            str(_merged.get("RELAY_LOG_REQUESTS", "true"))).lower() in ("1", "true", "yes", "on")
        SEMAPHORE_WAIT_SECONDS = float(os.environ.get("SEMAPHORE_WAIT_SECONDS") or
            str(_merged.get("SEMAPHORE_WAIT_SECONDS", 30.0)))
        MODEL_FILTER_PATTERN = str(_merged["MODEL_FILTER_PATTERN"])
        # The filter regex must be recompiled — _model_allowed reads the
        # compiled pattern, not MODEL_FILTER_PATTERN. Without this, the
        # --config filter silently never applied at startup.
        _model_filter_re = re.compile(MODEL_FILTER_PATTERN)
        LOG_LEVEL = str(_merged["LOG_LEVEL"]).upper()
        PROXY_LIST_FILE = os.environ.get("PROXY_LIST") or str(_merged.get("PROXY_LIST", ""))
        PROXY_LIST_ENV = os.environ.get("PROXY_LIST_ENV") or str(_merged.get("PROXY_LIST_ENV", ""))
        CONSECUTIVE_ERROR_THRESHOLD = int(os.environ.get("CONSECUTIVE_ERROR_THRESHOLD") or
            str(_merged.get("CONSECUTIVE_ERROR_THRESHOLD", 3)))
        PERMANENT_COOLDOWN_SECONDS = int(os.environ.get("PERMANENT_COOLDOWN_SECONDS") or
            str(_merged.get("PERMANENT_COOLDOWN_SECONDS", 86400)))
        HEALTH_FAIL_THRESHOLD = int(os.environ.get("HEALTH_FAIL_THRESHOLD") or
            str(_merged.get("HEALTH_FAIL_THRESHOLD", 3)))
        MAX_BODY_SIZE = int(os.environ.get("MAX_BODY_SIZE") or
            str(_merged.get("MAX_BODY_SIZE", 100 * 1024 * 1024)))
        MAX_RETRY_AFTER_SECONDS = int(os.environ.get("MAX_RETRY_AFTER_SECONDS") or
            str(_merged.get("MAX_RETRY_AFTER_SECONDS", 3600)))
        PROXY_HEALTH_CHECK_INTERVAL = int(os.environ.get("PROXY_HEALTH_CHECK_INTERVAL") or
            str(_merged.get("PROXY_HEALTH_CHECK_INTERVAL", 60)))
        _resize_semaphore()
        ADMIN_API_KEY = str(os.environ.get("ADMIN_API_KEY") or _merged.get("ADMIN_API_KEY", ""))  # noqa: F841

    if args.check:
        _run_config_check()
        sys.exit(0)

    import uvicorn

    workers = max(1, RELAY_WORKERS)
    if workers > 1:
        # Multi-process (opt-in): each worker imports the relay fresh and
        # carries its OWN pool/cooldowns/health state/client pool. Cooldowns
        # are NOT shared across workers — a proxy cooled in one worker can
        # still be hit by another. Use for raw throughput on independent
        # proxy sets; prefer a single worker when cooldown coherence matters.
        logger.warning(
            f"RELAY_WORKERS={workers}: each worker has its own in-memory pool "
            "(cooldowns/health state/clients are NOT shared across workers)"
        )

    # Custom SIGTERM/SIGINT handlers only in single-process mode. With
    # workers>1, uvicorn's master process manages worker lifecycle; our
    # sys.exit(0) handler would preempt its graceful shutdown.
    if workers == 1:
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
        workers=workers,
        # Inbound connection caps (0 = uvicorn defaults). Guards against FD
        # exhaustion / slow-loris BEFORE the semaphore backlog logic runs.
        limit_concurrency=RELAY_MAX_CONNECTIONS if RELAY_MAX_CONNECTIONS > 0 else None,
        backlog=RELAY_BACKLOG if RELAY_BACKLOG > 0 else 2048,
    )


if __name__ == "__main__":
    main()
