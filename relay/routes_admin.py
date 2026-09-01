"""Router module: routes_admin.

The /admin family: upstream health, cooldown/reset/reload operations.

Handlers relocated verbatim from relay.relay (2026-09-01, G3 router split).
Relay module-globals are dereferenced through the live ``_relay_globals`` seam
(installed by relay.relay via ``set_relay_globals``) — the same pattern as
relay/pool.py and relay/auth_switcher.py — so monkeypatching ``relay_mod.X``
at call time is honored.
"""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import logging
import time
import httpx
import json
import re

logger = logging.getLogger("proxy-relay")


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


@router.get("/admin/upstream-health")
async def admin_upstream_health(request: Request):
    """Check if the upstream API is reachable through the relay.

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _G('_check_admin_rate_limit')(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    if not _G('UPSTREAM_BASE'):
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": "No upstream configured"},
        )

    t0 = time.monotonic()
    proxy_entry = None
    try:
        # Route through the proxy pool — never hit the upstream directly
        # (the admin health check must reflect the real proxied path).
        proxy_entry = _G('pool').next()
        if proxy_entry is None:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "upstream": _G('_mask_proxy_url')(_G('UPSTREAM_BASE')),
                    "error": "All proxies cooling — cannot reach upstream",
                    "latency_ms": 0,
                },
            )

        headers = {}
        if _G('UPSTREAM_AUTH_TYPE') == "x-api-key":
            headers["x-api-key"] = _G('UPSTREAM_API_KEY')
        else:
            headers["Authorization"] = f"Bearer {_G('UPSTREAM_API_KEY')}"

        # Respect the concurrency limit — the health check is an upstream
        # call and must not bypass MAX_CONCURRENT_UPSTREAM.
        acquired_sem = await _G('_acquire_semaphore')(_G('SEMAPHORE_WAIT_SECONDS'))
        if acquired_sem is None:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "upstream": _G('_mask_proxy_url')(_G('UPSTREAM_BASE')),
                    "error": "Relay at capacity — cannot run health check",
                    "latency_ms": round((time.monotonic() - t0) * 1000, 1),
                },
            )
        try:
            async with _G('_borrow_client')(proxy_entry.url) as client:
                resp = await _G('_proxy_single')(
                    client,
                    "GET", f"{_G('UPSTREAM_BASE')}/models", headers, None, proxy_entry,
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
            "upstream": _G('_mask_proxy_url')(_G('UPSTREAM_BASE')),
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
            _G('pool').record_timeout(proxy_entry)
        _proxy_for_log = _G('_mask_proxy_url')(proxy_entry.url) if proxy_entry else "?"
        logger.warning(f"upstream-health connect failure via {_proxy_for_log}: {type(e).__name__}: {e}")
        latency_ms = (time.monotonic() - t0) * 1000
        return JSONResponse(
            status_code=502,
            content={
                "status": "error",
                "upstream": _G('_mask_proxy_url')(_G('UPSTREAM_BASE')),
                "error": "proxy_connect_failed",
                "latency_ms": round(latency_ms, 1),
            },
        )
    except (httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
        _proxy_for_log = _G('_mask_proxy_url')(proxy_entry.url) if proxy_entry else "?"
        logger.warning(f"upstream-health timeout via {_proxy_for_log}: {type(e).__name__}: {e}")
        latency_ms = (time.monotonic() - t0) * 1000
        return JSONResponse(
            status_code=502,
            content={
                "status": "error",
                "upstream": _G('_mask_proxy_url')(_G('UPSTREAM_BASE')),
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
                "upstream": _G('_mask_proxy_url')(_G('UPSTREAM_BASE')),
                "error": "Health check failed",
                "latency_ms": round(latency_ms, 1),
            },
        )



@router.post("/admin/clear-cooldowns")
async def admin_clear_cooldowns(request: Request):
    """Reset ALL proxies to available (clears temporary AND permanent cooldowns).

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _G('_check_admin_rate_limit')(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    _G('pool').clear_cooldowns()
    logger.info("All proxy cooldowns cleared (admin)")
    return {
        "status": "ok",
        "message": "All cooldowns cleared",
        "proxies_total": _G('pool').total,
        "available": _G('pool').available_count,
    }



@router.post("/admin/reset-proxy")
async def admin_reset_proxy(request: Request):
    """Reset a single proxy by URL. Body: {\"url\": \"socks5://...\"}

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _G('_check_admin_rate_limit')(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})
    url = data.get("url", "")
    if not url:
        return JSONResponse(status_code=400, content={"error": "Body must include 'url' field"})
    if _G('pool').reset_proxy(url):
        logger.info(f"Proxy reset (admin): {_G('_mask_proxy_url')(url)}")
        return {"status": "ok", "message": f"Proxy reset: {_G('_mask_proxy_url')(url)}"}
    return JSONResponse(
        status_code=404,
        content=({"error": f"Proxy not found in pool: {_G('_mask_proxy_url')(url)}"}),
    )



@router.post("/admin/reload-proxies")
async def admin_reload_proxies(request: Request):
    """Reload the proxy list from the configured file/env.

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _G('_check_admin_rate_limit')(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    _G('_init_pool')()
    await _G('_prune_client_pool')({p.url for p in _G('pool')._proxies})
    logger.info(f"Proxy list reloaded (admin): {_G('pool').total} proxies")
    return {
        "status": "ok",
        "message": "Proxy list reloaded",
        "proxies_total": _G('pool').total,
        "available": _G('pool').available_count,
    }



@router.post("/admin/reset-by-errors")
async def admin_reset_by_errors(request: Request):
    """Reset all proxies that have been permanently failed.
    Body: {\"min_consecutive\": 3} (optional, defaults to CONSECUTIVE_ERROR_THRESHOLD)

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _G('_check_admin_rate_limit')(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    try:
        data = await request.json() if request.headers.get("content-length") != "0" else {}
    except Exception:
        data = {}
    min_errs = data.get("min_consecutive", _G('CONSECUTIVE_ERROR_THRESHOLD'))
    # Unvalidated input (string/bool/None from the JSON body) would raise
    # TypeError inside reset_by_errors → unhandled 500. Coerce defensively.
    try:
        min_errs = int(min_errs)
    except (TypeError, ValueError):
        min_errs = _G('CONSECUTIVE_ERROR_THRESHOLD')
    reset_count = _G('pool').reset_by_errors(min_errs)
    logger.info(f"Reset {reset_count} permanently-failed proxies (admin)")
    return {"status": "ok", "message": f"Reset {reset_count} proxies"}



@router.post("/admin/reload-config")
async def admin_reload_config(request: Request):
    """Hot-reload upstream config + proxy list from config.json/env.

    Auth is enforced by the admin middleware (X-Admin-Key header).
    """
    if not await _G('_check_admin_rate_limit')(request.client.host if request.client else "unknown"):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    try:
        result = _G('_reload_upstream_config')()
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
    await _G('_prune_client_pool')({p.url for p in _G('pool')._proxies})
    logger.info(f"Config reloaded (admin): upstream={_G('_mask_proxy_url')(_G('UPSTREAM_BASE'))}, {_G('pool').total} proxies")
    return result

