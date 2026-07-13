"""Proxy pool with dynamic 429 cooldown.

Manages a pool of SOCKS5 proxies, tracks rate-limit cooldowns
using the upstream's Retry-After header, and provides round-robin
selection of available proxies.
"""

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ProxyEntry:
    """A single proxy with cooldown state."""
    url: str
    cooldown_until: float = 0.0  # time.monotonic() when cooling; 0 = ready
    last_error: str = ""
    consecutive_errors: int = 0
    total_ok: int = 0
    total_429: int = 0


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
        self._index = 0
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
        """True if every proxy is currently in cooldown."""
        now = time.monotonic()
        with self._lock:
            return all(p.cooldown_until > now for p in self._proxies)

    def next(self) -> Optional[ProxyEntry]:
        """Get the next available proxy via round-robin.

        Returns None when ALL proxies are cooling (fail-fast —
        no upstream calls attempted).
        """
        now = time.monotonic()

        with self._lock:
            if not self._proxies:
                return None

            # If all cooling, return None immediately
            if all(p.cooldown_until > now for p in self._proxies):
                return None

            # Round-robin: start from last index + 1
            n = len(self._proxies)
            for _ in range(n):
                self._index = (self._index + 1) % n
                candidate = self._proxies[self._index]
                if candidate.cooldown_until <= now:
                    return candidate

            return None

    def record_429(self, proxy: ProxyEntry, retry_after: int = 60):
        """Mark a proxy as cooling after a 429.

        Args:
            proxy: The ProxyEntry that got 429'd.
            retry_after: Seconds to cool (from Retry-After header).
                         Default 60s if header missing.
        """
        now = time.monotonic()
        with self._lock:
            proxy.cooldown_until = now + max(retry_after, 10)  # minimum 10s
            proxy.consecutive_errors += 1
            proxy.total_429 += 1
            self._all_time_429 += 1

    def record_timeout(self, proxy: ProxyEntry):
        """Mark a proxy as temporarily failing on timeout."""
        now = time.monotonic()
        with self._lock:
            # Timeout = short cooldown (30s), not a full rate-limit
            proxy.cooldown_until = now + 30
            proxy.consecutive_errors += 1

    def record_success(self, proxy: ProxyEntry):
        """Reset error state on success."""
        with self._lock:
            proxy.consecutive_errors = 0
            proxy.total_ok += 1
            self._all_time_ok += 1

    def stats(self) -> dict:
        """Return current pool statistics."""
        now = time.monotonic()
        with self._lock:
            available = sum(1 for p in self._proxies if p.cooldown_until <= now)
            cooling_list = []
            for p in self._proxies:
                remaining = max(0, p.cooldown_until - now)
                if remaining > 0:
                    cooling_list.append({
                        "proxy": p.url,
                        "remaining_s": int(remaining),
                        "total_429": p.total_429,
                    })
            return {
                "total": len(self._proxies),
                "available": available,
                "cooling": len(cooling_list),
                "cooling_details": sorted(cooling_list, key=lambda x: x["remaining_s"]),
                "all_time_ok": self._all_time_ok,
                "all_time_429": self._all_time_429,
            }

    def reload(self, proxies: list[str]):
        """Replace the entire proxy list (e.g. on SIGHUP or API call)."""
        now = time.monotonic()
        with self._lock:
            # Preserve cooldown state for proxies that are still in the new list
            old_map = {p.url: p for p in self._proxies}
            new_list = []
            for url in proxies:
                existing = old_map.get(url)
                if existing:
                    new_list.append(existing)
                else:
                    new_list.append(ProxyEntry(url=url, cooldown_until=now))
            self._proxies = new_list
            self._index = 0

    def clear_cooldowns(self):
        """Clear all cooldowns (force all proxies available)."""
        now = time.monotonic()
        with self._lock:
            for p in self._proxies:
                p.cooldown_until = now
                p.consecutive_errors = 0
