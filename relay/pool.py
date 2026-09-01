"""Thread-safe proxy rotation pool with cooldown, model-exhaust, and breaker state.

Extracted from ``relay/relay.py`` (2026-08-31). The class body is byte-identical
to the original; config values that were once read as relay module globals
(``LATENCY_SKIP_THRESHOLD_MS``, ``CONSECUTIVE_ERROR_THRESHOLD``,
``PERMANENT_COOLDOWN_SECONDS``, ``MAX_RETRY_AFTER_SECONDS``,
``MODEL_EXHAUST_CAP``) are now looked up live in the relay module's globals via
``_relay_globals``, installed by ``relay.relay`` through
``set_relay_globals({...})``. That keeps the test contract intact: tests
monkeypatch e.g. ``relay_mod.LATENCY_SKIP_THRESHOLD_MS`` and the next pool call
sees it, exactly as before the extraction.

The pool has no other coupling to the relay module — it is a self-contained,
directly-testable deep module.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Optional


logger = logging.getLogger("proxy-relay.pool")

# Live reference to relay.relay's module globals, installed at import by
# relay.py. Looked up at CALL time so ``monkeypatch.setattr(relay_mod, "X", v)``
# (the test contract for LATENCY_SKIP_THRESHOLD_MS and the cooldown constants)
# is honored by the next pool call.
_relay_globals: dict = {}


def set_relay_globals(g: dict) -> None:
    """Install the relay module's globals dict for live config lookups."""
    global _relay_globals
    _relay_globals = g


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
        # Per-proxy per-model upstream budget exhaustion: (url, model) → expiry
        # (monotonic). Each proxy egress IP has its OWN upstream free-tier
        # quota per model (proven 2026-08-02: same key on a different IP still
        # 429s → quota follows the IP). A proxy spent for model M must NOT be
        # cooled (it stays healthy for other models) — skip it for M until the
        # Retry-After and keep sweeping the pool. Bounded by _exhaust_cap.
        self._model_exhaust: dict[tuple[str, str], float] = {}
        # Global model circuit breaker: (model) -> expiry (monotonic).
        # Upstream returns 400 "Model is unavailable" for GLOBALLY gated free
        # models (deepseek-v4-flash-free) — a capacity gate, not a per-IP budget.
        # Every proxy fails identically, so parking per-proxy is pointless. Instead
        # we trip a model-level breaker: for `cap` seconds no proxy is selected for
        # that model, and the request loop returns the same FreeUsageLimitError
        # shape the fallback bridge listens for — so Hermes skips straight to the
        # next model in the chain with ZERO wasted upstream round-trips.
        self._model_breaker: dict[str, float] = {}
        # Honor the merged config (config.json + env), not a hardcoded default.
        # The module-level MODEL_EXHAUST_CAP merges config.json + env (line 628);
        # use the same effective value here so config.json's cap is actually
        # honored. Fall back to env / 21600 only if the module value is unset.
        self._exhaust_cap = float(
            os.environ.get("MODEL_EXHAUST_CAP")
            or _relay_globals.get("MODEL_EXHAUST_CAP", 21600)
        )
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

    def next(self, model: str | None = None) -> Optional[ProxyEntry]:
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
                    if model and self._is_model_exhausted_locked(candidate.url, model, now):
                        continue  # proxy healthy but out of budget for THIS model
                    return self._maybe_skip_slow(now, n, model)
            return None

    def mark_model_exhaust(self, url: str, model: str, secs: float) -> None:
        """Park `url` for `model` only — the proxy stays ACTIVE for other
        models (its egress IP spent its per-model budget; cooling it would
        needlessly remove bandwidth for models that still work)."""
        secs = min(max(float(secs), 1.0), self._exhaust_cap)
        with self._lock:
            self._model_exhaust[(url, model)] = time.monotonic() + secs

    def trip_model_breaker(self, model: str, secs: float) -> None:
        """Trip the global model circuit breaker (upstream capacity gate).

        Used when EVERY proxy would fail for this model (e.g. 400 "Model is
        unavailable" — a global gate, not a per-IP budget). Parks the model for
        `secs` so the request loop skips it and returns the fallback-bridge error
        shape, instead of burning a round-trip on every proxy."""
        secs = min(max(float(secs), 5.0), self._exhaust_cap)
        with self._lock:
            self._model_breaker[model] = time.monotonic() + secs

    def model_breaker_open(self, model: str, now: float | None = None) -> bool:
        """True if the global model breaker for `model` is currently tripped."""
        if now is None:
            now = time.monotonic()
        with self._lock:
            exp = self._model_breaker.get(model)
            if exp is None:
                return False
            if now >= exp:
                del self._model_breaker[model]
                return False
            return True

    def breaker_models(self) -> dict[str, int]:
        """{model: seconds_remaining} for observability/health endpoint."""
        now = time.monotonic()
        out: dict[str, int] = {}
        with self._lock:
            for m, exp in list(self._model_breaker.items()):
                if exp > now:
                    out[m] = int(exp - now)
                else:
                    del self._model_breaker[m]
        return out

    def _is_model_exhausted_locked(self, url: str, model: str, now: float) -> bool:
        exp = self._model_exhaust.get((url, model))
        if exp is None:
            return False
        if now >= exp:
            del self._model_exhaust[(url, model)]
            return False
        return True

    def exhausted_count_for(self, model: str) -> int:
        """Number of proxies currently parked (model-exhausted) for `model`."""
        now = time.monotonic()
        with self._lock:
            return sum(1 for (u, m), exp in self._model_exhaust.items()
                       if m == model and exp > now)

    def exhausted_models(self) -> dict[str, int]:
        """{model: count_of_exhausted_proxies} for observability."""
        now = time.monotonic()
        with self._lock:
            out: dict[str, int] = {}
            for (u, m), exp in self._model_exhaust.items():
                if exp > now:
                    out[m] = out.get(m, 0) + 1
            return out

    def _maybe_skip_slow(self, now: float, n: int, model: str | None = None) -> ProxyEntry:
        """Latency-aware selection: prefer a faster proxy over a slow one.

        When LATENCY_SKIP_THRESHOLD_MS > 0 and the round-robin candidate is
        measurably slower than the threshold, scan for a faster available
        proxy (unknown-latency proxies count as fast — no data, no bias).
        Falls back to the candidate when nothing faster exists. The scan is
        bounded to the pool size and only runs when the knob is enabled
        (default 0 = pure round-robin, zero overhead).
        """
        candidate = self._proxies[self._index]
        if (_relay_globals.get('LATENCY_SKIP_THRESHOLD_MS') or 0) <= 0 or candidate.latency_samples == 0:
            return candidate
        if candidate.avg_latency_ms <= (_relay_globals.get('LATENCY_SKIP_THRESHOLD_MS') or 0):
            return candidate
        start = self._index
        for _ in range(n - 1):
            self._index = (self._index + 1) % n
            alt = self._proxies[self._index]
            if alt.cooldown_until <= now and not alt.permanently_dead:
                if model and self._is_model_exhausted_locked(alt.url, model, now):
                    continue
                if alt.latency_samples == 0 or alt.avg_latency_ms <= (_relay_globals.get('LATENCY_SKIP_THRESHOLD_MS') or 0):
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
        # INTENTIONAL ASYMMETRY (do not "unify" without a reason):
        #  * record_429 / _parse_retry_after clamp a proxy TRANSIENT cooldown
        #    to MAX_RETRY_AFTER_SECONDS (3600) with a hard +10s floor — the
        #    proxy may recover sooner, so the cap is tight.
        #  * MODEL_EXHAUST_CAP (21600) bounds a MODEL's per-budget park, which
        #    legitimately lasts up to the daily reset; mark_model_exhaust has a
        #    1s floor, trip_model_breaker a 5s floor. Different concerns =
        #    different bounds.
        try:
            cooldown = max(int(retry_after), 10)
        except (ValueError, TypeError):
            cooldown = 60
        cooldown = min(cooldown, _relay_globals.get('MAX_RETRY_AFTER_SECONDS') or 3600)
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
            if proxy.consecutive_errors >= (_relay_globals.get('CONSECUTIVE_ERROR_THRESHOLD') or 3):
                proxy.cooldown_until = now + (_relay_globals.get('PERMANENT_COOLDOWN_SECONDS') or 86400)
                proxy.permanently_dead = True
                proxy.last_error = (
                    f"Permanent failure after {proxy.consecutive_errors} "
                    f"consecutive errors (cooling {_relay_globals.get('PERMANENT_COOLDOWN_SECONDS') or 86400}s)"
                )
                logger.warning(
                    f"Proxy {_mask_proxy_url(proxy.url)} MARKED PERMANENTLY UNAVAILABLE "
                    f"({proxy.consecutive_errors} consecutive errors, "
                    f"cooling {_relay_globals.get('PERMANENT_COOLDOWN_SECONDS') or 86400}s)"
                )
            else:
                proxy.cooldown_until = now + 30
                proxy.last_error = (
                    f"Temporary failure ({proxy.consecutive_errors}/"
                    f"{_relay_globals.get('CONSECUTIVE_ERROR_THRESHOLD') or 3} consecutive)"
                )

    def record_permanent_failure(self, proxy: ProxyEntry, reason: str = ""):
        """Explicitly mark a proxy as permanently failed (e.g., API-reported exhaustion)."""
        now = time.monotonic()
        with self._lock:
            proxy.cooldown_until = now + (_relay_globals.get('PERMANENT_COOLDOWN_SECONDS') or 86400)
            proxy.permanently_dead = True
            proxy.consecutive_errors += 1
            proxy.last_error = reason or f"Permanent failure (cooling {_relay_globals.get('PERMANENT_COOLDOWN_SECONDS') or 86400}s)"
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
        """Record a latency sample for the proxy (EWMA — exponential decay).

        The old arithmetic mean `(avg*(n-1)+x)/n` never decays: after ~100
        samples a proxy that degraded hours ago stayed \"fast\" forever, so
        LATENCY_SKIP_THRESHOLD_MS stopped working. An EWMA (α = 0.2) weights
        recent samples heavily, so a proxy's average tracks its CURRENT
        performance and latency-aware selection stays responsive. The first
        sample seeds the average outright (no history to decay).
        """
        with self._lock:
            proxy.last_latency_ms = latency_ms
            if proxy.latency_samples == 0:
                proxy.avg_latency_ms = latency_ms
            else:
                proxy.avg_latency_ms = (
                    0.2 * latency_ms + 0.8 * proxy.avg_latency_ms
                )
            proxy.latency_samples += 1

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
                    if p.permanently_dead or remaining >= (_relay_globals.get('PERMANENT_COOLDOWN_SECONDS') or 86400) // 2:
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

    def set_exhaust_cap(self, seconds: float) -> None:
        """Update the model-exhaust cooldown cap in place.

        Called on config reload so a live cap change takes effect without a
        restart. Clamped to a sane floor (a cap below the 1s model-exhaust
        floor would make model parking unusable).
        """
        with self._lock:
            self._exhaust_cap = max(float(seconds), 1.0)

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
