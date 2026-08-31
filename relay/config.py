"""Configuration subsystem for Hermes Proxy Relay.

SINGLE source of truth for all runtime configuration. Prior to this module,
config truth was parsed in FOUR places in ``relay.relay``:

1. the import-time block (``_load_config_file -> _merge_config -> ~60 globals``)
2. ``main()``'s ``--config`` re-merge
3. ``_reload_upstream_config`` (hot reload)
4. ``_apply_dynamic_cap_config`` (dynamic-cap subset)

Each site re-derived the same knobs with its own fallback defaults, so a fresh
config key had to be added in all four places with *matching* defaults or it
drifted (the STREAM_IDLE_TIMEOUT / MODEL_EXHAUST_CAP reload-drift bug class).

This module owns THE parse: environment wins over file config wins over
defaults, exactly once, behind a small interface::

    config.load()      # build the snapshot (idempotent, import-safe)
    config.reload()    # re-read file + env, swap the snapshot
    config.snapshot()  # frozen mapping: the single truth

The relay module exposes the historic UPPER_CASE names as thin delegations into
the snapshot so all existing call sites and tests keep working unchanged.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

logger = logging.getLogger("proxy-relay")


# ── Defaults (single home for every knob's fallback) ──────────────────────
# These carry a LOT of hard-won operational context. Preserve the comments.
_DEFAULT_CONFIG: dict[str, Any] = {
    "UPSTREAM_BASE": "",
    "UPSTREAM_API_KEY": "",
    "UPSTREAM_AUTH_TYPE": "bearer",
    # ── Secondary "go" upstream (ported from the production relay) ──
    # The production relay exposes /go/v1/* routes to a second upstream
    # (GO_UPSTREAM_BASE) with its own key. Kept for behavioral parity; the
    # go routes 503 when GO_UPSTREAM_BASE is empty.
    "GO_UPSTREAM_BASE": "",
    "GO_UPSTREAM_API_KEY": "",
    "GO_UPSTREAM_AUTH_TYPE": "bearer",
    # When true, /v1/models returns ONLY ids containing "-free" (matches the
    # production relay's free-tier filter — clients pick from what they see).
    "MODELS_FREE_ONLY": "false",
    # Cap (seconds) for per-proxy per-model budget-exhaust skip time
    # (FreeUsageLimitError Retry-After is ~6h; never park a proxy longer).
    "MODEL_EXHAUST_CAP": 21600,
    "RELAY_PORT": 4002,
    # Cap on concurrent upstream spans. See the original inline commentary.
    "MAX_CONCURRENT_UPSTREAM": 24,
    # ── Dynamic cap (auto-tuned concurrency, v1.8) ─────────────────────
    "DYNAMIC_CAP_ENABLED": "false",
    "DYNAMIC_CAP_CPU_TARGET_PCT": 90,
    "DYNAMIC_CAP_CPU_MAX_PCT": 96,
    "DYNAMIC_CAP_MIN": 10,
    "DYNAMIC_CAP_MAX": 500,
    "DYNAMIC_CAP_INTERVAL_S": 5,
    "DYNAMIC_CAP_STEP": 0.10,
    "DYNAMIC_CAP_SMOOTHING": 0.3,
    "DYNAMIC_CAP_DISK_TARGET_PCT": 70,
    "DYNAMIC_CAP_DISK_MAX_PCT": 85,
    "MAX_QUEUED_REQUESTS": 100,
    "HOLD_PERMIT_FOR_STREAM": "true",
    "HEALTH_CHECK_CONCURRENCY": 20,
    "RELAY_WORKERS": 1,
    "RELAY_MAX_CONNECTIONS": 0,
    "RELAY_BACKLOG": 0,
    "UPSTREAM_CONNECT_TIMEOUT": 15,
    "UPSTREAM_READ_TIMEOUT": 120,
    "STREAM_IDLE_TIMEOUT": 60,
    "CLIENT_IDLE_TTL": 120,
    "MAX_RESPONSE_SIZE": 200 * 1024 * 1024,
    "MODEL_FILTER_PATTERN": ".*",
    "LOG_LEVEL": "INFO",
    "PROXY_LIST": "",
    "PROXY_LIST_ENV": "",
    "CONSECUTIVE_ERROR_THRESHOLD": 3,
    "PERMANENT_COOLDOWN_SECONDS": 86400,
    "MAX_RETRY_AFTER_SECONDS": 3600,
    "ADMIN_API_KEY": "",
    "CLIENT_API_KEY": "",
    "MAX_REQUEST_RETRIES": 3,
    "RETRY_SEMAPHORE_WAIT_SECONDS": 2.0,
    "RETRY_BACKOFF_BASE": 0.1,
    "RETRY_BACKOFF_MAX": 1.0,
    "FALLBACK_MODEL": "",
    "LATENCY_SKIP_THRESHOLD_MS": 0,
    "RELAY_LOG_REQUESTS": "true",
    "SEMAPHORE_WAIT_SECONDS": 30.0,
    "PROXY_HEALTH_CHECK_INTERVAL": 60,
    "PROXY_HEALTH_CHECK_URL": "http://httpbin.org/ip",
    "HEALTH_FAIL_THRESHOLD": 3,
    "MAX_BODY_SIZE": 100 * 1024 * 1024,
    # ── Smart auth switching ─────────────────────────────────────────
    "AUTH_SWITCH_ENABLED": "true",
    "AUTH_SWITCH_CANDIDATES": "bearer,x-api-key",
    "AUTH_SWITCH_TRIGGER_THRESHOLD": 3,
    "AUTH_SWITCH_PROBE_SUCCESSES": 2,
    "AUTH_SWITCH_COOLDOWN_S": 300,
    "AUTH_SWITCH_MAX_PER_WINDOW": 3,
    "AUTH_SWITCH_WINDOW_S": 3600,
    "AUTH_STATE_PATH": "~/.hermes/proxy-relay/auth_state.json",
    # ── Client pool (not in the original _DEFAULT_CONFIG but read as globals) ──
    "CLIENT_POOL_MAX": 100,
}

# Keys with derived post-processing (documented at each line below).
_DERIVED: dict[str, str] = {
    # DYNAMIC_CAP_MAX must be at least DYNAMIC_CAP_MIN
    "DYNAMIC_CAP_MAX": ">=DYNAMIC_CAP_MIN",
}


def _load_config_file(path: str) -> dict:
    """Load config from a JSON file (written by the Hermes plugin)."""
    try:
        p = os.path.expanduser(path)
        if os.path.exists(p):
            with open(p) as f:
                return json.load(f)
    except Exception as e:  # pragma: no cover - defensive: read errors are rare
        logger.warning(f"Failed to load config file {path}: {e}")
    return {}


def _merge_config(file_config: dict) -> dict:
    """Env vars take precedence over file config over defaults."""
    cfg = dict(_DEFAULT_CONFIG)
    cfg.update(file_config)
    for key in cfg:
        env_val = os.environ.get(key)
        if env_val is not None and env_val != "":
            cfg[key] = env_val
    return cfg


def _as_bool(v: Any) -> bool:
    return str(v).lower() in ("1", "true", "yes", "on")


def _env_or(key: str, merged: dict, *, default: Any = None, default_key: str | None = None) -> Any:
    """env-wins helper with `or` semantics (empty string = unset).

    Mirrors the historic `os.environ.get(K) or _merged.get(K, DEFAULT)` pattern
    EXACTLY: an env var set to "" behaves as UNSET, and the fallback comes from
    the merged dict (file config or default).
    """
    dflt = default if default_key is None else _DEFAULT_CONFIG.get(default_key, default)
    return os.environ.get(key) or str(merged.get(key, dflt))


def build(merged: dict[str, Any]) -> dict[str, Any]:
    """Compute the full typed snapshot from a merged config.

    Faithfully reproduces every historic derivation, including the
    ``os.environ.get(K) or _merged.get(K, D)`` env-wins semantics, the bool
    parsing set, and the ``DYNAMIC_CAP_MAX >= DYNAMIC_CAP_MIN`` clamp. This is
    the ONE place the raw merged strings become typed runtime values.
    """
    S = {}

    S["UPSTREAM_BASE"] = str(merged["UPSTREAM_BASE"]).rstrip("/")
    S["UPSTREAM_API_KEY"] = str(merged["UPSTREAM_API_KEY"])
    S["UPSTREAM_AUTH_TYPE"] = str(merged["UPSTREAM_AUTH_TYPE"]).lower()
    S["GO_UPSTREAM_BASE"] = str(merged["GO_UPSTREAM_BASE"]).rstrip("/")

    _go_key = str(os.environ.get("GO_UPSTREAM_API_KEY") or merged.get("GO_UPSTREAM_API_KEY", ""))
    if not _go_key:
        logger.warning(
            "GO_UPSTREAM_API_KEY not set — falling back to UPSTREAM_API_KEY. "
            "If GO_UPSTREAM_BASE is configured for a DIFFERENT upstream, set a "
            "distinct GO_UPSTREAM_API_KEY (silent fallback would leak the "
            "primary key to the secondary upstream)."
        )
    S["GO_UPSTREAM_API_KEY"] = _go_key or S["UPSTREAM_API_KEY"]
    S["GO_UPSTREAM_AUTH_TYPE"] = str(merged["GO_UPSTREAM_AUTH_TYPE"]).lower()

    S["MODELS_FREE_ONLY"] = _as_bool(merged["MODELS_FREE_ONLY"])
    S["MODEL_EXHAUST_CAP"] = float(_env_or("MODEL_EXHAUST_CAP", merged, default=21600))
    S["RELAY_PORT"] = int(merged["RELAY_PORT"])
    S["MAX_CONCURRENT_UPSTREAM"] = int(merged["MAX_CONCURRENT_UPSTREAM"])

    # ── Dynamic cap knobs ────────────────────────────────────────────
    S["DYNAMIC_CAP_ENABLED"] = _as_bool(merged["DYNAMIC_CAP_ENABLED"])
    S["DYNAMIC_CAP_CPU_TARGET_PCT"] = float(merged["DYNAMIC_CAP_CPU_TARGET_PCT"])
    S["DYNAMIC_CAP_CPU_MAX_PCT"] = float(merged["DYNAMIC_CAP_CPU_MAX_PCT"])
    S["DYNAMIC_CAP_DISK_TARGET_PCT"] = float(merged["DYNAMIC_CAP_DISK_TARGET_PCT"])
    S["DYNAMIC_CAP_DISK_MAX_PCT"] = float(merged["DYNAMIC_CAP_DISK_MAX_PCT"])
    S["DYNAMIC_CAP_MIN"] = max(1, int(merged["DYNAMIC_CAP_MIN"]))
    S["DYNAMIC_CAP_MAX"] = max(S["DYNAMIC_CAP_MIN"], int(merged["DYNAMIC_CAP_MAX"]))
    S["DYNAMIC_CAP_INTERVAL_S"] = float(merged["DYNAMIC_CAP_INTERVAL_S"])
    S["DYNAMIC_CAP_STEP"] = float(merged["DYNAMIC_CAP_STEP"])
    S["DYNAMIC_CAP_SMOOTHING"] = float(merged["DYNAMIC_CAP_SMOOTHING"])

    S["MAX_QUEUED_REQUESTS"] = int(merged["MAX_QUEUED_REQUESTS"])
    S["HOLD_PERMIT_FOR_STREAM"] = _as_bool(merged["HOLD_PERMIT_FOR_STREAM"])
    S["HEALTH_CHECK_CONCURRENCY"] = int(merged["HEALTH_CHECK_CONCURRENCY"])
    S["RELAY_WORKERS"] = int(merged["RELAY_WORKERS"])
    S["RELAY_MAX_CONNECTIONS"] = int(merged["RELAY_MAX_CONNECTIONS"])
    S["RELAY_BACKLOG"] = int(merged["RELAY_BACKLOG"])

    S["UPSTREAM_CONNECT_TIMEOUT"] = float(merged["UPSTREAM_CONNECT_TIMEOUT"])
    S["UPSTREAM_READ_TIMEOUT"] = float(merged["UPSTREAM_READ_TIMEOUT"])
    S["STREAM_IDLE_TIMEOUT"] = float(_env_or("STREAM_IDLE_TIMEOUT", merged, default=0))
    S["CLIENT_IDLE_TTL"] = float(merged["CLIENT_IDLE_TTL"])
    S["MAX_RESPONSE_SIZE"] = int(merged["MAX_RESPONSE_SIZE"])

    S["MODEL_FILTER_PATTERN"] = str(merged["MODEL_FILTER_PATTERN"])
    S["LOG_LEVEL"] = str(merged["LOG_LEVEL"]).upper()

    S["PROXY_LIST_FILE"] = os.environ.get("PROXY_LIST") or str(merged.get("PROXY_LIST", ""))
    S["PROXY_LIST_ENV"] = os.environ.get("PROXY_LIST_ENV") or str(merged.get("PROXY_LIST_ENV", ""))

    S["CONSECUTIVE_ERROR_THRESHOLD"] = int(_env_or("CONSECUTIVE_ERROR_THRESHOLD", merged, default=3))
    S["PERMANENT_COOLDOWN_SECONDS"] = int(_env_or("PERMANENT_COOLDOWN_SECONDS", merged, default=86400))
    S["MAX_RETRY_AFTER_SECONDS"] = int(_env_or("MAX_RETRY_AFTER_SECONDS", merged, default=3600))
    S["ADMIN_API_KEY"] = str(os.environ.get("ADMIN_API_KEY") or merged.get("ADMIN_API_KEY", ""))
    S["CLIENT_API_KEY"] = str(os.environ.get("CLIENT_API_KEY") or merged.get("CLIENT_API_KEY", ""))

    S["MAX_REQUEST_RETRIES"] = int(_env_or("MAX_REQUEST_RETRIES", merged, default=3))
    S["RETRY_SEMAPHORE_WAIT_SECONDS"] = float(_env_or("RETRY_SEMAPHORE_WAIT_SECONDS", merged, default=2.0))
    S["RETRY_BACKOFF_BASE"] = float(_env_or("RETRY_BACKOFF_BASE", merged, default=0.1))
    S["RETRY_BACKOFF_MAX"] = float(_env_or("RETRY_BACKOFF_MAX", merged, default=1.0))
    S["FALLBACK_MODEL"] = str(_env_or("FALLBACK_MODEL", merged, default=""))
    S["LATENCY_SKIP_THRESHOLD_MS"] = float(_env_or("LATENCY_SKIP_THRESHOLD_MS", merged, default=0))
    S["RELAY_LOG_REQUESTS"] = _as_bool(_env_or("RELAY_LOG_REQUESTS", merged, default="true"))
    S["SEMAPHORE_WAIT_SECONDS"] = float(_env_or("SEMAPHORE_WAIT_SECONDS", merged, default=30.0))
    S["PROXY_HEALTH_CHECK_INTERVAL"] = int(_env_or("PROXY_HEALTH_CHECK_INTERVAL", merged, default=60))
    S["PROXY_HEALTH_CHECK_URL"] = str(_env_or("PROXY_HEALTH_CHECK_URL", merged, default="http://httpbin.org/ip"))
    S["HEALTH_FAIL_THRESHOLD"] = int(_env_or("HEALTH_FAIL_THRESHOLD", merged, default=3))
    S["MAX_BODY_SIZE"] = int(_env_or("MAX_BODY_SIZE", merged, default=100 * 1024 * 1024))

    # ── Auth switching ───────────────────────────────────────────────
    S["AUTH_SWITCH_ENABLED"] = _as_bool(_env_or("AUTH_SWITCH_ENABLED", merged, default="true"))
    S["AUTH_SWITCH_CANDIDATES"] = [c.strip().lower() for c in str(
        _env_or("AUTH_SWITCH_CANDIDATES", merged, default="bearer,x-api-key")
    ).split(",") if c.strip()]
    S["AUTH_SWITCH_TRIGGER_THRESHOLD"] = int(_env_or("AUTH_SWITCH_TRIGGER_THRESHOLD", merged, default=3))
    S["AUTH_SWITCH_PROBE_SUCCESSES"] = int(_env_or("AUTH_SWITCH_PROBE_SUCCESSES", merged, default=2))
    S["AUTH_SWITCH_COOLDOWN_S"] = int(_env_or("AUTH_SWITCH_COOLDOWN_S", merged, default=300))
    S["AUTH_SWITCH_MAX_PER_WINDOW"] = int(_env_or("AUTH_SWITCH_MAX_PER_WINDOW", merged, default=3))
    S["AUTH_SWITCH_WINDOW_S"] = int(_env_or("AUTH_SWITCH_WINDOW_S", merged, default=3600))
    S["AUTH_STATE_PATH"] = os.path.expanduser(str(
        _env_or("AUTH_STATE_PATH", merged, default="~/.hermes/proxy-relay/auth_state.json")))

    # ── Client pool (read by _client_pool_cap) ───────────────────────
    S["CLIENT_POOL_MAX"] = int(_env_or("CLIENT_POOL_MAX", merged, default=100))
    S["CLIENT_POOL_ENABLED"] = S["CLIENT_POOL_MAX"] > 0

    # Derived regex compiled once here (single home) — pattern consumers read
    # the compiled object via snapshot.
    S["_model_filter_re"] = re.compile(S["MODEL_FILTER_PATTERN"])

    return S


class _Config:
    """Module-level state holder: the snapshot + the merged raw dict."""

    def __init__(self) -> None:
        self._merged: dict[str, Any] = dict(_DEFAULT_CONFIG)
        self._snapshot: dict[str, Any] = {}
        self._path: str = ""

    def _build_from(self, path: str) -> dict[str, Any]:
        """Shared file read + merge + build used by load/reload."""
        self._path = path
        file_cfg = _load_config_file(self._path) if self._path else {}
        self._merged = _merge_config(file_cfg)
        self._snapshot = build(self._merged)
        return self._snapshot

    def load(self, path: str | None = None) -> dict[str, Any]:
        """(Re)build the snapshot from env + config file. Import-safe, idempotent.

        Called by the relay module at every module (re)import with the
        module's _CONFIG_PATH so a monkeypatched path is honored; falls back
        to the RELAY_CONFIG env var.
        """
        if path is None:
            path = os.environ.get("RELAY_CONFIG", "") or ""
        return self._build_from(path)

    def reload(self, path: str | None = None) -> dict[str, Any]:
        """Re-read file + env and swap the snapshot. Returns new snapshot."""
        if path is None:
            path = os.environ.get("RELAY_CONFIG", "") or ""
        return self._build_from(path)

    def snapshot(self) -> dict[str, Any]:
        return self._snapshot

    def merged(self) -> dict[str, Any]:
        return self._merged


# Module-level singleton. Import is lazy-after-env in the relay module, so
# we bind the initial snapshot at construction from the ambient env/file —
# exactly mirroring the relay module's historic import-time config binding.
# Hot-reload paths call config.reload() explicitly.
config = _Config()
config.load()
