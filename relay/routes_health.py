"""Router module: routes_health.

The /health endpoint — a live snapshot of relay state.

Handlers relocated verbatim from relay.relay (2026-09-01, G3 router split).
Relay module-globals are dereferenced through the live ``_relay_globals`` seam
(installed by relay.relay via ``set_relay_globals``) — the same pattern as
relay/pool.py and relay/auth_switcher.py — so monkeypatching ``relay_mod.X``
at call time is honored.
"""

from fastapi import APIRouter
import time

# Live relay globals seam (same contract as relay/pool.py, relay/auth_switcher.py).
_relay_globals: dict = {}


def set_relay_globals(globals_dict: dict) -> None:
    """Install the LIVE relay module globals dict (not a copy)."""
    global _relay_globals
    _relay_globals = globals_dict


def _G(name: str):
    """Dereference a relay module-global by name at call time."""
    return _relay_globals[name]


router = APIRouter()


@router.get("/health")
async def health():
    stats = _G('pool').stats()
    return {
        "status": "ok" if stats["available"] > 0 else "degraded",
        "pool_stats": stats,
        # Masked — an upstream URL with embedded user:pass@ must not leak
        # credentials to unauthenticated health pollers. Identity for
        # credential-less URLs (the common case).
        "upstream_base": _G('_mask_proxy_url')(_G('UPSTREAM_BASE')),
        "models_available": len(_G('MODELS_CACHE')) if _G('MODELS_CACHE') else 0,
        "request_stats": dict(_G('_request_count')),
        "model_breakers": _G('pool').breaker_models(),
        "proxy_breakers": _G('pool').breaker_proxies(),
        "semaphore": {"max": _G('_semaphore_max'), "used": _G('_semaphore_max') - _G('semaphore')._value, "queued": _G('_waiting_count')},
        "dynamic_cap": {
            "enabled": bool(_G('DYNAMIC_CAP_ENABLED')),
            "effective_max": int(_G('_EFFECTIVE_CAP')) if _G('DYNAMIC_CAP_ENABLED') else int(_G('MAX_CONCURRENT_UPSTREAM')),
            "cpu_pct": round(_G('_dyn_last_cpu_pct'), 1),
            "target_pct": _G('DYNAMIC_CAP_CPU_TARGET_PCT'),
            "hard_max_pct": _G('DYNAMIC_CAP_CPU_MAX_PCT'),
            "disk_pct": round(_G('_dyn_last_disk_pct'), 1),
            "disk_target_pct": _G('DYNAMIC_CAP_DISK_TARGET_PCT'),
            "disk_hard_max_pct": _G('DYNAMIC_CAP_DISK_MAX_PCT'),
            "range": [_G('DYNAMIC_CAP_MIN'), _G('DYNAMIC_CAP_MAX')],
            "interval_s": _G('DYNAMIC_CAP_INTERVAL_S'),
            "adjustments": _G('_dyn_adjustments'),
            "last_cap": _G('_dyn_last_cap'),
        },
        "uptime_seconds": int(time.monotonic() - _G('_START_TIME')),
        "version": _G('VERSION'),
        "shared_clients": len(_G('_client_pool')),
        "max_body_size": _G('MAX_BODY_SIZE'),
        "security": {
            "client_auth_enabled": bool(_G('CLIENT_API_KEY')),
            "admin_auth_enabled": bool(_G('ADMIN_API_KEY')),
        },
        "auth_switch": _G('auth_switcher').status(),
    }

