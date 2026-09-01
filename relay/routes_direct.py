"""Direct-egress router: free-filtered OpenCode models WITHOUT the SOCKS5 pool.

Why this exists
---------------
The main /v1 and /go/v1 families route through the SOCKS5 pool (they exist to
spread requests across 250 residential proxies). For an "opencode-zen-normal"
provider — direct egress from the relay's own IP — the pool would defeat the
purpose (and leak the host IP only if misused). This router serves the SAME
free-filtered model directory + shared cache, but talks to the upstream via a
plain (pool-less) httpx client.

It is mounted ONLY when DIRECT_EGRESS=true (see relay.relay), so the existing
/v1 + /go/v1 proxied paths are untouched. The two providers (proxied + normal)
share ONE free-set filter and ONE model cache, so there is zero drift between
them and they both auto-track the upstream as OpenCode updates its catalog.

Route prefix: /v1n (e.g. /v1n/models, /v1n/chat/completions). The distinct
prefix is REQUIRED — mounting a second /v1 on the same app would collide with
routes_v1's /v1 (FastAPI resolves by mount order, so the wrong handler could
win). /v1n is unambiguous.

Reuses relay.relay's live-globals seam via set_relay_globals (same contract as
routes_v1.py / pool.py / auth_switcher.py), so no new global state is needed:
MODELS_FREE_ONLY, MODEL_FILTER_PATTERN, MODELS_CACHE, the semaphore, the auth
gate, and error mapping are all the shared relay singletons.
"""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import logging
import time
import httpx
import json

logger = logging.getLogger("proxy-relay")

# Live relay globals seam (same contract as routes_v1.py).
_relay_globals: dict = {}


def set_relay_globals(globals_dict: dict) -> None:
    """Install the LIVE relay module globals dict (not a copy)."""
    global _relay_globals
    _relay_globals = globals_dict


def _G(name: str):
    """Dereference a relay module-global by name at call time."""
    return _relay_globals[name]


router = APIRouter()


@router.get("/v1n/models", response_model=None)
async def list_models_direct(request: Request = None):  # noqa: ANN001 — FastAPI optional handler param
    # Same auth gate as routes_v1 — model names are metadata but shouldn't be
    # exposed to unauthenticated clients on an open relay.
    headers = dict(request.headers) if request is not None else {}
    if _G("CLIENT_API_KEY") and not _G("_client_key_valid")(headers):
        _G("_inc_counter")("auth_failed")
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

    base = _G("UPSTREAM_BASE")
    if not base:
        return {"object": "list", "data": []}

    def _free_filter(models: list[dict]) -> list[dict]:
        if not _G("MODELS_FREE_ONLY"):
            return models
        return [m for m in models
                if "-free" in m.get("id", "")
                or m.get("id") == "big-pickle"]

    # Check cache freshness (shared with the proxied path — one source of truth).
    now = time.monotonic()
    if _G("MODELS_CACHE") and (now - _G("MODELS_CACHE_UPDATED")) < _G("MODELS_CACHE_TTL"):
        return {"object": "list", "data": _free_filter(list(_G("MODELS_CACHE")))}

    # Browser UA — Cloudflare-challenged upstreams 403 non-browser UAs.
    headers = {}
    if _G("UPSTREAM_AUTH_TYPE") == "x-api-key":
        headers["x-api-key"] = _G("UPSTREAM_API_KEY")
    else:
        headers["Authorization"] = f"Bearer {_G('UPSTREAM_API_KEY')}"
    headers["User-Agent"] = "opencode/1.18.25"
    headers.setdefault("HTTP-Referer", "https://opencode.ai/")
    headers.setdefault("X-Title", "opencode")

    acquired_sem = await _G("_acquire_semaphore")(_G("SEMAPHORE_WAIT_SECONDS"))
    if acquired_sem is None:
        logger.warning("Semaphore busy — serving cached models (direct)")
        return {"object": "list", "data": _free_filter(list(_G("MODELS_CACHE")))}
    try:
        # Direct egress: plain httpx client, NO proxy (mirror relay's non-proxied
        # pattern at health.py/relay.py:509 — fresh client per request).
        async with httpx.AsyncClient(timeout=httpx.Timeout(
            _G("UPSTREAM_READ_TIMEOUT"), connect=_G("UPSTREAM_CONNECT_TIMEOUT"))
        ) as client:
            resp = await client.get(f"{base}/models", headers=headers)
        if resp.status_code == 200:
            data = json.loads(resp.content.decode()).get("data", [])
            filtered = [m for m in data]
            _G("_update_models_cache")(filtered)
            return {"object": "list", "data": _free_filter(filtered)}
        logger.warning(f"Direct models fetch non-200: {resp.status_code} — serving cache")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Direct models refresh failed: {e}")
    finally:
        acquired_sem.release()

    return {"object": "list", "data": _free_filter(list(_G("MODELS_CACHE")))}


@router.post("/v1n/chat/completions")
async def chat_completions_direct(request: Request):
    # Auth BEFORE reading the body (same ordering as routes_v1).
    if not _G("_client_key_valid")(dict(request.headers)):
        _G("_inc_counter")("auth_failed")
        return _G("_client_auth_error")()
    body = await _G("_read_body_capped")(request)
    if body is None:
        return JSONResponse(
            status_code=413,
            content={
                "error": {
                    "message": f"Request body too large (max {_G('MAX_BODY_SIZE')} bytes).",
                    "type": "payload_too_large",
                    "code": "body_too_large",
                }
            },
        )

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return JSONResponse(
            status_code=400,
            content={"error": {"message": "Invalid JSON body.", "type": "invalid_request_error"}},
        )

    base = _G("UPSTREAM_BASE")
    if not base:
        return JSONResponse(
            status_code=503,
            content={"error": {"message": "Upstream base URL is not configured.", "type": "configuration_error"}},
        )

    headers = {"Content-Type": "application/json", "User-Agent": "opencode/1.18.25",
               "HTTP-Referer": "https://opencode.ai/", "X-Title": "opencode"}
    if _G("UPSTREAM_AUTH_TYPE") == "x-api-key":
        headers["x-api-key"] = _G("UPSTREAM_API_KEY")
    else:
        headers["Authorization"] = f"Bearer {_G('UPSTREAM_API_KEY')}"

    acquired_sem = await _G("_acquire_semaphore")(_G("SEMAPHORE_WAIT_SECONDS"))
    if acquired_sem is None:
        return JSONResponse(
            status_code=503,
            content={"error": {"message": "Semaphore busy — retry later.", "type": "overloaded"}},
        )
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(
            _G("UPSTREAM_READ_TIMEOUT"), connect=_G("UPSTREAM_CONNECT_TIMEOUT"))
        ) as client:
            resp = await client.post(f"{base}/chat/completions", headers=headers, content=body)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Direct chat upstream error: {e}")
        return JSONResponse(
            status_code=502,
            content={"error": {"message": "Upstream error.", "type": "upstream_error"}},
        )
    finally:
        acquired_sem.release()

    # Pass through the upstream status + body (streaming is a superset: httpx
    # returns the full body; for SSE we'd need a streaming response, but direct
    # mode uses non-stream by default — mirror the proxied error mapping).
    content_type = resp.headers.get("content-type", "")
    if resp.status_code >= 400:
        return JSONResponse(
            status_code=502 if resp.status_code >= 500 else resp.status_code,
            content={"error": {"message": f"Upstream responded {resp.status_code}.", "type": "upstream_error"}},
        )
    return JSONResponse(
        status_code=resp.status_code,
        content=json.loads(resp.content.decode()),
        media_type=content_type,
    )
