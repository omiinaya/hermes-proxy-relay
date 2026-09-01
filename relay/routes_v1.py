"""Router module: routes_v1.

The /v1 and /go proxy families: models listing, chat completions, and pass-through.

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


@router.get("/v1/models", response_model=None)
@router.get("/go/v1/models", response_model=None)
async def list_models(request: Request = None):
    # Gate with client auth when configured — model names are metadata but
    # should not be exposed to unauthenticated clients on an open relay.
    headers = dict(request.headers) if request is not None else {}
    if _G('CLIENT_API_KEY') and not _G('_client_key_valid')(headers):
        _G('_inc_counter')("auth_failed")
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

    if not _G('UPSTREAM_BASE'):
        return {"object": "list", "data": []}

    is_go = request is not None and request.url.path.startswith("/go/")
    base = _G('GO_UPSTREAM_BASE') if is_go else _G('UPSTREAM_BASE')
    api_key = _G('GO_UPSTREAM_API_KEY') if is_go else _G('UPSTREAM_API_KEY')
    if not base:
        return {"object": "list", "data": []}

    def _free_filter(models: list[dict]) -> list[dict]:
        if not _G('MODELS_FREE_ONLY'):
            return models
        return [m for m in models
                if "-free" in m.get("id", "") or m.get("id") == "big-pickle"]

    # Check cache freshness
    now = time.monotonic()
    if _G('MODELS_CACHE') and (now - _G('MODELS_CACHE_UPDATED')) < _G('MODELS_CACHE_TTL'):
        return {"object": "list", "data": _free_filter(list(_G('MODELS_CACHE')))}

    try:
        # Route through the proxy pool — a direct client would leak the
        # relay's real IP to the upstream (defeats the proxy's purpose).
        headers = {}
        if _G('UPSTREAM_AUTH_TYPE') == "x-api-key":
            headers["x-api-key"] = api_key
        else:
            headers["Authorization"] = f"Bearer {api_key}"
        # Browser UA (production parity): Cloudflare-challenged upstreams 403
        # non-browser UAs — the models fetch must look like a browser too.
        headers["User-Agent"] = ("opencode/1.18.25")
        headers.setdefault("HTTP-Referer", "https://opencode.ai/")
        headers.setdefault("X-Title", "opencode")

        # Retry across proxies on connect failure — one dead proxy must not
        # stall a cold-cache models refresh (the old code gave up after one).
        for attempt in range(_G('MAX_REQUEST_RETRIES')):
            proxy_entry = _G('pool').next()
            if proxy_entry is None:
                logger.warning("All proxies cooling — cannot refresh models, serving cache")
                return {"object": "list", "data": _free_filter(list(_G('MODELS_CACHE')))}

            # Respect the concurrency limit — the models refresh is an upstream
            # call too; bypassing the semaphore could exceed
            # MAX_CONCURRENT_UPSTREAM when a flood of /v1/models requests hits
            # a cold cache.
            acquired_sem = await _G('_acquire_semaphore')(_G('SEMAPHORE_WAIT_SECONDS'))
            if acquired_sem is None:
                logger.warning("Semaphore busy — serving cached models")
                return {"object": "list", "data": _free_filter(list(_G('MODELS_CACHE')))}
            try:
                async with _G('_borrow_client')(proxy_entry.url) as client:
                    resp = await _G('_proxy_single')(
                        client,
                        "GET", f"{base}/models", headers, None, proxy_entry,
                    )
                if resp.status_code == 200:
                    data = json.loads(resp.body.decode()).get("data", [])
                    filtered = [m for m in data if _G('_model_allowed')(m.get("id", ""))]
                    _G('_update_models_cache')(filtered)
                    return {"object": "list", "data": _free_filter(filtered)}
                # Non-200 (401/429/5xx): _proxy_single already recorded pool
                # effects; serve the cache — retrying won't change the status.
                break
            except (httpx.ConnectError, httpx.ConnectTimeout):
                # Dead proxy — cool it so the next real request doesn't pay the
                # connect timeout too (the generic handler below would swallow
                # the exception without touching the pool, leaving the dead
                # proxy in rotation indefinitely).
                _G('pool').record_timeout(proxy_entry)
                logger.warning(
                    f"Models refresh connect failure via {_G('_mask_proxy_url')(proxy_entry.url)} "
                    f"({attempt + 1}/{_G('MAX_REQUEST_RETRIES')}) — cooled, trying next proxy"
                )
            finally:
                acquired_sem.release()
    except Exception as e:
        logger.warning(f"Failed to refresh models: {e}")

    return {"object": "list", "data": _free_filter(list(_G('MODELS_CACHE')))}



async def _chat_handler(request: Request, go: bool = False):
    # Auth BEFORE reading the body — an unauthenticated attacker must not
    # be able to make us buffer up to MAX_BODY_SIZE bytes per request.
    if not _G('_client_key_valid')(dict(request.headers)):
        _G('_inc_counter')("auth_failed")
        return _G('_client_auth_error')()
    body = await _G('_read_body_capped')(request)
    if body is None:
        logger.warning(f"Request body exceeds MAX_BODY_SIZE ({_G('MAX_BODY_SIZE')} bytes)")
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
    headers = dict(request.headers)
    return await _G('_proxy_request')(
        "POST", "/v1/chat/completions" if go else "/chat/completions",
        body, headers, request.url.query or "", go=go,
    )


@router.post("/v1/chat/completions")
async def chat_completions(request: Request):
    return await _chat_handler(request, go=False)



@router.post("/go/v1/chat/completions")
async def go_chat_completions(request: Request):
    return await _chat_handler(request, go=True)



async def _proxy_all_impl(path: str, request: Request, go: bool = False):
    # Auth BEFORE reading the body (see chat_completions).
    if not _G('_client_key_valid')(dict(request.headers)):
        _G('_inc_counter')("auth_failed")
        return _G('_client_auth_error')()
    body = None
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        # DELETE is included: some APIs (e.g. file/fine-tune cleanup
        # endpoints) send a JSON body with DELETE. Dropping it would
        # silently mutate the upstream request semantics.
        body = await _G('_read_body_capped')(request)
        if body is None:
            logger.warning(f"Request body exceeds MAX_BODY_SIZE ({_G('MAX_BODY_SIZE')} bytes)")
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
    headers = dict(request.headers)
    return await _G('_proxy_request')(
        request.method, f"/{path}", body, headers, request.url.query or "", go=go,
    )


@router.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy_all(path: str, request: Request):
    return await _proxy_all_impl(path, request, go=False)



@router.api_route("/go/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def go_proxy_all(path: str, request: Request):
    return await _proxy_all_impl(path, request, go=True)

