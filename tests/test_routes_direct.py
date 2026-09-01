"""Direct-egress router tests (DIRECT_EGRESS mode for opencode-zen-normal).

Exercises relay/routes_direct.py standalone: the free-filtered, pool-less
/v1n models + chat completions router mounted only when DIRECT_EGRESS=true.

Mirrors test_routes_modules.py conventions:
- relay.relay is NEVER imported at module top-level (collection time) — the
  production env exports CLIENT_API_KEY/UPSTREAM_API_KEY which would capture
  real keys and break later open-relay tests. All relay imports are lazy.
- The `wired` fixture points the router seam at relay.relay's live globals
  and saves/restores the globals the tests mutate, for hermetic isolation.
"""

import importlib

import json

import pytest


@pytest.fixture(scope="module")
def lc():
    """Lazily import the relay + direct router modules (post-env-patch)."""
    relay_mod = importlib.import_module("relay.relay")
    rd = importlib.import_module("relay.routes_direct")
    return relay_mod, rd


def _route_paths(router):
    return {getattr(r, "path", "") for r in router.routes}


def _req(path="/", headers=None, method="GET", client_host="10.0.0.1"):
    """Build a lightweight stand-in Request via starlette."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        "query_string": b"",
        "client": (client_host, 1234),
    }
    return Request(scope)


# Globals these tests mutate — save/restore for hermetic isolation.
_SET_GLOBALS = [
    "UPSTREAM_BASE",
    "CLIENT_API_KEY",
    "MODELS_CACHE",
    "MODELS_CACHE_UPDATED",
    "MODELS_CACHE_TTL",
    "MODELS_FREE_ONLY",
    "UPSTREAM_READ_TIMEOUT",
    "UPSTREAM_CONNECT_TIMEOUT",
    "SEMAPHORE_WAIT_SECONDS",
]


@pytest.fixture
def wired(lc):
    """Point the direct-router seam at relay.relay's live globals; restore on exit."""
    relay_mod, rd = lc
    d = relay_mod.__dict__
    rd._relay_globals = d
    saved = {k: d.get(k) for k in _SET_GLOBALS}
    # Hermetic: no client auth, a fake upstream, sane timeouts.
    d["CLIENT_API_KEY"] = ""
    d["UPSTREAM_BASE"] = "https://example.com/v1"
    d["UPSTREAM_API_KEY"] = "public"
    d["UPSTREAM_AUTH_TYPE"] = "bearer"
    d["MODELS_FREE_ONLY"] = True
    d["UPSTREAM_READ_TIMEOUT"] = 30.0
    d["UPSTREAM_CONNECT_TIMEOUT"] = 10.0
    d["SEMAPHORE_WAIT_SECONDS"] = 5.0
    d["MODELS_CACHE"] = []
    d["MODELS_CACHE_UPDATED"] = 0.0
    d["MODELS_CACHE_TTL"] = 600
    d["MAX_BODY_SIZE"] = 1024 * 1024
    try:
        yield relay_mod, rd
    finally:
        for k, v in saved.items():
            d[k] = v
        rd._relay_globals = d


class TestDirectModule:
    def test_router_is_apirouter(self, lc):
        _, rd = lc
        paths = _route_paths(rd.router)
        assert "/v1n/models" in paths
        assert "/v1n/chat/completions" in paths
        # Must NOT collide with the proxied /v1 routes.
        assert "/v1/models" not in paths

    async def test_list_models_no_upstream_empty(self, wired):
        relay_mod, rd = wired
        relay_mod.UPSTREAM_BASE = ""
        result = await rd.list_models_direct()
        assert result == {"object": "list", "data": []}

    async def test_list_models_serves_cache_fresh(self, wired):
        relay_mod, rd = wired
        relay_mod.MODELS_CACHE_TTL = 600
        relay_mod.MODELS_CACHE_UPDATED = 9999999999.0
        relay_mod.MODELS_CACHE = [{"id": "cached-free", "object": "model"}]
        result = await rd.list_models_direct()
        ids = [m["id"] for m in result["data"]]
        # free filter applies to cache too
        assert ids == ["cached-free"]

    async def test_list_models_free_filter(self, wired):
        relay_mod, rd = wired
        relay_mod.MODELS_CACHE_TTL = 0  # force fetch (cold)
        relay_mod.MODELS_CACHE_UPDATED = 0.0
        relay_mod._update_models_cache = lambda lst: None

        class FakeResp:
            status_code = 200

            def __init__(self, payload):
                self._p = payload

            @property
            def content(self):
                return json.dumps(self._p).encode()

        calls = {}

        async def fake_get(url, headers=None):
            calls["url"] = url
            return FakeResp({"data": [
                {"id": "big-pickle"},
                {"id": "paid-model-x"},
                {"id": "deepseek-v4-flash-free"},
                {"id": "mimo-v2.5-free"},
            ]})

        import httpx

        original_AsyncClient = httpx.AsyncClient
        original_sem = relay_mod._acquire_semaphore

        class FakeClient:
            def __init__(self, *a, **k):
                self.get = fake_get

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        async def no_sem(timeout=None):
            class _S:
                def __init__(self):
                    pass

                def release(self):
                    pass

            return _S()

        relay_mod._acquire_semaphore = no_sem
        httpx.AsyncClient = lambda *a, **k: FakeClient()
        try:
            result = await rd.list_models_direct()
        finally:
            relay_mod._acquire_semaphore = original_sem
            httpx.AsyncClient = original_AsyncClient

        ids = [m["id"] for m in result["data"]]
        assert "big-pickle" in ids
        assert "deepseek-v4-flash-free" in ids
        assert "paid-model-x" not in ids
        # went direct (no proxy): the URL is the upstream, not a proxy
        assert calls["url"] == "https://example.com/v1/models"


class TestDirectChat:
    async def test_chat_auth_gate(self, wired):
        relay_mod, rd = wired
        relay_mod.CLIENT_API_KEY = "sekrit"
        relay_mod._client_auth_error = lambda: "AUTHERR"
        resp = await rd.chat_completions_direct(_req(method="POST"))
        assert resp == "AUTHERR"

    async def test_chat_body_too_large(self, wired):
        relay_mod, rd = wired
        relay_mod.MAX_BODY_SIZE = 10

        async def _read_body_capped(req):
            return None

        relay_mod._read_body_capped = _read_body_capped
        resp = await rd.chat_completions_direct(_req(method="POST"))
        assert resp.status_code == 413

    async def test_chat_succeeds_direct(self, wired):
        relay_mod, rd = wired
        relay_mod.MAX_BODY_SIZE = 1024 * 1024

        async def _read_body_capped(req):
            return b'{"model": "big-pickle", "messages": []}'

        relay_mod._read_body_capped = _read_body_capped
        relay_mod._client_auth_error = lambda: "AUTHERR"

        class FakeResp:
            status_code = 200

            @property
            def content(self):
                return b'{"choices": [{"message": {"role": "assistant", "content": "hi"}}]}'

            @property
            def headers(self):
                return {"content-type": "application/json"}

        import httpx

        original_AsyncClient = httpx.AsyncClient

        async def fake_post(url, headers=None, content=None):
            return FakeResp()

        class FakeClient:
            def __init__(self, *a, **k):
                self.post = fake_post

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        httpx.AsyncClient = lambda *a, **k: FakeClient()
        try:
            resp = await rd.chat_completions_direct(_req(method="POST"))
        finally:
            httpx.AsyncClient = original_AsyncClient

        assert resp.status_code == 200

    async def test_chat_upstream_error_maps_502(self, wired):
        relay_mod, rd = wired
        relay_mod.MAX_BODY_SIZE = 1024 * 1024

        async def _read_body_capped(req):
            return b"{}"

        relay_mod._read_body_capped = _read_body_capped
        relay_mod._client_auth_error = lambda: "AUTHERR"

        class Boom:
            async def __aenter__(self):
                raise RuntimeError("upstream down")

            async def __aexit__(self, *a):
                return False

        import httpx

        original_AsyncClient = httpx.AsyncClient
        httpx.AsyncClient = lambda *a, **k: Boom()
        try:
            resp = await rd.chat_completions_direct(_req(method="POST"))
        finally:
            httpx.AsyncClient = original_AsyncClient

        assert resp.status_code == 502
        assert resp.headers["content-type"].startswith("application/json")
