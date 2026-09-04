"""Profile-aware proxy pool registry for Hermes Proxy Relay.

A relay historically served ONE CooldownPool built from a single proxy source.
This module adds runtime-switchable PROFILES: each profile owns its OWN
CooldownPool (isolated cooldown/breaker state) and its own proxy source, but
they share the relay's single upstream path. Only ONE profile is ACTIVE at a
time; switching the active profile rebinds the relay's ``pool`` global with zero
restart (avoids the ~90s graceful-shutdown dead window).

Design (2026-09-04, user-confirmed):
  * ONE active profile at a time (switched on the fly, "prompt me to switch").
  * In-memory pool state, NOT persisted across restart (confirmed acceptable).
  * ``DEFAULT_PROFILE`` selects the boot profile; the active pool is the relay's
    ``pool`` global, so the ENTIRE existing code path (routing, health checker,
    auth switcher, fallback bridge) works unchanged — switching just rebinds
    which CooldownPool ``pool`` points at.
  * Non-active profiles are built LAZILY on first switch (boot cost unchanged
    for single-profile deployments; no file reads at import for the test suite,
    which runs with ``RELAY_CONFIG=""`` -> empty PROFILE_DEFS).

This module is a PURE cache: it holds no file-reading or URL validation. The
relay supplies the source-resolution (``build_proxies``) and pool construction
(``init_fn``) so that profile pools are validated EXACTLY like the legacy
loaders (same per-URL validation, same dedup). The relay also supplies
``specs_provider`` (live {name: spec}) so a hot ``/admin/reload-config``
re-reads new profile definitions without rebuilding the registry.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from .pool import CooldownPool

logger = logging.getLogger("proxy-relay.profiles")


class ProfileRegistry:
    """Named cache of isolated CooldownPools, for runtime switching.

    Callables (all owned by the relay so config/IO/validation stay in ONE
    module — this class is pure state + a cache):
      specs_provider() -> {name: spec}      live profile definitions
      build_proxies(name, spec) -> [str]    resolve spec -> validated, dedup'd list
      init_fn([str]) -> CooldownPool        build a pool from a resolved list

    Pools are cached per name: re-switching to a profile RESUMES its in-memory
    cooldown/breaker state (we do not rebuild a pool that is already built).
    """

    def __init__(
        self,
        *,
        specs_provider: Callable[[], dict[str, Any]],
        build_proxies: Callable[[str, Any], list[str]],
        init_fn: Callable[[list[str]], CooldownPool],
        profiles_dir: str,
    ) -> None:
        self._specs_provider = specs_provider
        self._build_proxies = build_proxies
        self._init_fn = init_fn
        self._profiles_dir = profiles_dir
        self._pools: dict[str, CooldownPool] = {}

    # ── introspection (never leaks proxy URLs / credentials) ─────────
    def specs(self) -> dict[str, Any]:
        """Live {name: spec} map.

        The provider may return a list of {"name":..., "proxies":...} dicts
        (config.json shape) or an already-mapped dict; both normalize here.
        """
        try:
            raw = self._specs_provider()
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("specs_provider failed: %s", e)
            return {}
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return dict(raw)
        if isinstance(raw, (list, tuple)):
            out: dict[str, Any] = {}
            for d in raw:
                if isinstance(d, dict) and d.get("name"):
                    out[str(d["name"])] = d
            return out
        logger.warning("specs_provider returned unexpected type: %r", type(raw))
        return {}

    def names(self) -> list[str]:
        return list(self.specs())

    def defined(self, name: str) -> bool:
        return name in self.specs()

    # ── pool lifecycle ────────────────────────────────────────────────
    def build(self, name: str) -> CooldownPool:
        """Return the pool for ``name``, building (and caching) it on first use."""
        spec = self.specs().get(name)
        if spec is None:
            raise KeyError(name)
        if name not in self._pools:
            # The spec is the full profile dict {"name":.., "proxies":<source>};
            # the source itself is the "proxies" field (path | {"file"} | {"env"}).
            source = spec.get("proxies") if isinstance(spec, dict) else spec
            proxies = self._build_proxies(source)
            self._pools[name] = self._init_fn(list(proxies))
            logger.info(
                "Built pool for profile %r: %d proxies", name, self._pools[name].total
            )
        return self._pools[name]

    def get(self, name: str, *, build: bool = True) -> Optional[CooldownPool]:
        if not self.defined(name):
            return None
        if build and name not in self._pools:
            return self.build(name)
        return self._pools.get(name)

    def drop(self, name: str) -> None:
        """Forget a built pool so the next switch rebuilds it from fresh sources."""
        self._pools.pop(name, None)

    def rebuild(self) -> None:
        """Rebuild every cached pool from fresh specs (config reload path).

        Only profiles already built are rebuilt; unbuilt profiles are built on
        first switch. This refreshes the active pool after a hot config reload
        so changed proxy lists take effect without a restart.
        """
        for name in list(self._pools):
            spec = self.specs().get(name)
            if spec is None:
                self._pools.pop(name, None)
                continue
            source = spec.get("proxies") if isinstance(spec, dict) else spec
            self._pools[name] = self._init_fn(list(self._build_proxies(source)))
            logger.info(
                "Rebuilt pool for profile %r (config reload): %d proxies",
                name,
                self._pools[name].total,
            )

    def refresh(self, name: str) -> CooldownPool:
        """Force-re-read a profile's sources and rebuild its pool (no restart).

        Bypasses the cache: even if the profile's spec (filename) is unchanged,
        the underlying file may have been edited by an operator — a config
        reload must pick that up (parity with the legacy single-pool reload,
        which re-reads the proxy file on every reload). Rebinding the returned
        pool to the relay's ``pool`` global makes the fresh list live.
        """
        spec = self.specs().get(name)
        if spec is None:
            self._pools.pop(name, None)
            raise KeyError(name)
        source = spec.get("proxies") if isinstance(spec, dict) else spec
        self._pools[name] = self._init_fn(list(self._build_proxies(source)))
        logger.info(
            "Refreshed pool for profile %r (reload): %d proxies",
            name,
            self._pools[name].total,
        )
        return self._pools[name]

    def stats(self) -> dict[str, dict[str, int]]:
        """Per-built-profile counters. Never surfaces proxy URLs/credentials."""
        out: dict[str, dict[str, int]] = {}
        for name, p in self._pools.items():
            total = int(p.total)
            available = int(p.available_count)
            out[name] = {
                "total": total,
                "available": available,
                "cooling": max(0, total - available),
            }
        return out