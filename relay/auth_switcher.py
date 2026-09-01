"""Upstream auth-method failure detection and auto-switch.

Extracted from ``relay/relay.py`` (2026-08-31). The switcher class is
self-contained EXCEPT for a small set of live reads from the relay module:

* ``UPSTREAM_AUTH_TYPE`` / ``UPSTREAM_BASE`` (read at call time, and
  ``UPSTREAM_AUTH_TYPE`` also written on a switch),
* ``RETRY_SEMAPHORE_WAIT_SECONDS``,
* the relay helpers ``_acquire_semaphore`` / ``_build_headers`` /
  ``_borrow_client`` and the ``pool`` instance.

These are resolved through a live reference to ``relay.relay``'s module
dict installed by ``set_relay_globals`` — the same seam ``relay/pool.py``
uses — so tests that monkeypatch ``relay_mod._borrow_client`` /
``relay_mod.UPSTREAM_AUTH_TYPE`` etc. keep working exactly as before.
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

import httpx

logger = logging.getLogger("proxy-relay")

# Live reference to relay.relay's module globals, installed at import time by
# relay.py (see set_relay_globals). AuthSwitcher reads/writes config globals
# and calls relay helper functions through this dict so that monkeypatching
# relay module attributes (the test contract) and /admin/reload-config
# re-binds both keep working.
_relay_globals: dict = {}


def set_relay_globals(globals_dict: dict) -> None:
    """Point AuthSwitcher's live config/helper reads at relay.relay's globals.

    Stores the reference (not a copy) so later bindings and monkeypatches on
    relay.relay's globals are visible at call time — mirroring relay/pool.py.
    """
    global _relay_globals
    _relay_globals = globals_dict


def _get_relay_auth_type() -> str:
    """Current upstream auth type from relay.relay's live globals (never None)."""
    return str(_relay_globals.get("UPSTREAM_AUTH_TYPE") or "bearer")


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
        # Cooldown applies only AFTER a probe has actually been attempted —
        # _last_probe_ts starts at 0.0, so `now - 0 < cooldown_s` would wrongly
        # block the FIRST ever probe on a long-lived process (monotonic clock
        # already past cooldown_s), which is what made should_probe() flaky on
        # reused CI runners (Python 3.12 matrix).
        if self._last_probe_ts > 0 and now - self._last_probe_ts < self.cooldown_s:
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
        current = _get_relay_auth_type()
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
        if not _relay_globals.get("UPSTREAM_BASE"):
            return "inconclusive"
        url = f"{_relay_globals.get('UPSTREAM_BASE')}/models"
        # Gate the probe with the concurrency semaphore — it IS an upstream
        # call and must not bypass MAX_CONCURRENT_UPSTREAM (design rule: all
        # upstream-touching routes honor the gate). A short bounded wait; if
        # the relay is at capacity, defer the probe (inconclusive is never an
        # auth signal, so a skipped probe cannot cause a false switch).
        gate = await _relay_globals["_acquire_semaphore"](_relay_globals.get("RETRY_SEMAPHORE_WAIT_SECONDS"))
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
            proxy_entry = _relay_globals["pool"].next()
            if proxy_entry is None or proxy_entry.url in tried:
                break
            tried.add(proxy_entry.url)
            headers = _relay_globals["_build_headers"]({}, auth_type=auth_type)
            try:
                async with _relay_globals["_borrow_client"](proxy_entry.url) as client:
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
        _relay_globals["UPSTREAM_AUTH_TYPE"] = new
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
                    "auth_type": _relay_globals.get("UPSTREAM_AUTH_TYPE"),
                    "switched_at": self._switch_history[-1]["ts"] if self._switch_history else None,
                    "switch_count": self._switches_done,
                }, f, indent=2)
        except Exception as e:
            logger.warning(f"AUTH SWITCH: failed to persist auth state: {e}")

    def load_state(self) -> str | None:
        """Read the persisted auth type (last verified at runtime), if any."""
        if not self.enabled:
            return None
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
            "current_auth_type": _relay_globals.get("UPSTREAM_AUTH_TYPE"),
            "consecutive_401s": self._consecutive_401,
            "total_401s": self._total_401,
            "probes_run": self._probes_run,
            "switches": self._switches_done,
            "alert": self._alert,
            "candidates": list(self.candidates),
            "switch_history": list(self._switch_history[-5:]),
        }
