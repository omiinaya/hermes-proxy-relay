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
    "MAX_BODY_SIZE",
    # The chat tests below assign relay._client_auth_error / relay._read_body_capped
    # directly (not via monkeypatch) — those must be restored or they leak into the
    # shared relay dict and break later test files (e.g. test_routes_modules) that
    # resolve the same names through the seam. Same for _acquire_semaphore and
    # _update_models_cache, which the fetch tests swap out.
    "_client_auth_error",
    "_read_body_capped",
    "_acquire_semaphore",
    "_update_models_cache",
    "UPSTREAM_AUTH_TYPE",
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

    def test_mounts_only_when_flag_enabled(self, monkeypatch):
        """The `if DIRECT_EGRESS:` mount in relay.py (module-top) gates /v1n.

        With the flag OFF (default) the direct router is not on `app`. Flipping
        the flag and re-importing relay re-runs the module top, which must mount
        the direct router; flipping it back must unmount it. (importlib.reload is
        proven safe here by test_routes_modules.test_reload_survives.)
        """
        import sys

        relay_mod = importlib.import_module("relay.relay")

        def _all_paths(a):
            """All route paths on the app, expanding included routers.

            FastAPI mounts include_router entries as _IncludedRouter placeholders
            (path=None) that never flatten into app.router.routes — the real
            routes live on the placeholder's .original_router. Walk into those.
            """

            def walk(routes):
                for r in routes:
                    p = getattr(r, "path", None)
                    if isinstance(p, str):
                        out.add(p)
                    sub = getattr(r, "original_router", None)
                    if sub is not None and hasattr(sub, "routes"):
                        walk(sub.routes)

            out = set()
            walk(a.router.routes)
            return out

        # Default: flag is False (conftest RELAY_CONFIG="" -> pure defaults).
        assert relay_mod.DIRECT_EGRESS is False
        paths = _all_paths(relay_mod.app)
        assert "/v1n/models" not in paths
        assert "/v1n/chat/completions" not in paths
        # The proxied router IS mounted (proves the walker expands inclusions).
        assert "/v1/models" in paths

        # Flag ON -> re-import re-runs the module-top mount block.
        monkeypatch.setenv("DIRECT_EGRESS", "1")
        importlib.reload(relay_mod)
        try:
            assert relay_mod.DIRECT_EGRESS is True
            paths = _all_paths(relay_mod.app)
            assert "/v1n/models" in paths
            assert "/v1n/chat/completions" in paths
        finally:
            # Restore: clear the flag + re-import back to the default (flag OFF)
            # module so the rest of the suite sees the unmounted state.
            monkeypatch.delenv("DIRECT_EGRESS", raising=False)
            importlib.reload(relay_mod)

        assert relay_mod.DIRECT_EGRESS is False
        paths = _all_paths(relay_mod.app)
        assert "/v1n/models" not in paths
        # Router module identity is stable across reload (sys.modules persists).
        assert sys.modules.get("relay.routes_direct") is not None


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


# ---------------------------------------------------------------------------
# Edge-case coverage for the free-filter / auth / upstream branches in
# routes_direct.py. Each test isolates ONE uncovered branch and restores every
# mutated global (the `wired` fixture save/restores _SET_GLOBALS, which now
# includes _acquire_semaphore, _update_models_cache, _client_auth_error,
# _read_body_capped, MAX_BODY_SIZE, UPSTREAM_AUTH_TYPE).
# ---------------------------------------------------------------------------


async def _no_sem(timeout=None):
    """_acquire_semaphore stand-in that always succeeds (release is a no-op)."""

    class _S:
        def release(self):
            pass

    return _S()


class _FakeResp:
    def __init__(self, status, payload=b"{}"):
        self.status_code = status
        self._c = payload

    @property
    def content(self):
        return self._c

    @property
    def headers(self):
        return {"content-type": "application/json"}


class _FakeClient:
    """Minimal httpx.AsyncClient stand-in: get/post return canned responses."""

    def __init__(self, get=None, post=None, seen=None):
        self._get = get
        self._post = post
        self._seen = seen

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url, headers=None):
        if self._seen is not None:
            self._seen["url"] = url
            self._seen["headers"] = dict(headers)
        return self._get(url, headers)

    async def post(self, url, headers=None, content=None):
        if self._seen is not None:
            self._seen["url"] = url
            self._seen["headers"] = dict(headers)
        return self._post(url, headers, content)


def _swap_asyncclient(rd_mod, client_factory):
    """Swap httpx.AsyncClient for the duration of one test (restores on exit)."""
    import httpx

    original = httpx.AsyncClient
    httpx.AsyncClient = client_factory
    return original


class TestDirectModelsEdge:
    async def test_list_models_requires_client_key(self, wired):
        # L61-62: auth gate on the /v1n models route.
        relay_mod, rd = wired
        relay_mod.CLIENT_API_KEY = "sekrit"
        resp = await rd.list_models_direct(_req())  # no auth header
        assert resp.status_code == 401
        assert resp.headers["www-authenticate"] == "Bearer"

    async def test_list_models_free_filter_disabled(self, wired):
        # L80: MODELS_FREE_ONLY=False -> cache passes through unfiltered.
        relay_mod, rd = wired
        relay_mod.MODELS_FREE_ONLY = False
        relay_mod.MODELS_CACHE_TTL = 600
        relay_mod.MODELS_CACHE_UPDATED = 9999999999.0
        relay_mod.MODELS_CACHE = [
            {"id": "paid-model-x"},
            {"id": "deepseek-v4-flash-free"},
        ]
        result = await rd.list_models_direct()
        ids = [m["id"] for m in result["data"]]
        assert ids == ["paid-model-x", "deepseek-v4-flash-free"]

    async def test_list_models_xapikey_header(self, wired):
        # L93: x-api-key upstream auth path builds the x-api-key header.
        relay_mod, rd = wired
        relay_mod.MODELS_CACHE_TTL = 0  # force a cold fetch
        relay_mod.MODELS_CACHE_UPDATED = 0.0
        relay_mod._update_models_cache = lambda lst: None
        relay_mod.UPSTREAM_AUTH_TYPE = "x-api-key"
        relay_mod.UPSTREAM_API_KEY = "up-key"
        seen = {}

        def _get(url, headers=None):
            return _FakeResp(200, json.dumps({"data": [{"id": "deepseek-v4-flash-free"}]}).encode())

        relay_mod._acquire_semaphore = _no_sem
        original = _swap_asyncclient(rd, lambda *a, **k: _FakeClient(get=_get, seen=seen))
        try:
            await rd.list_models_direct()
        finally:
            import httpx

            httpx.AsyncClient = original

        assert seen["headers"].get("x-api-key") == "up-key"
        assert "Authorization" not in seen["headers"]
        assert "User-Agent" in seen["headers"]

    async def test_list_models_semaphore_busy_serves_cache(self, wired):
        # L102-103: semaphore unavailable -> serve the (filtered) cache.
        relay_mod, rd = wired
        relay_mod.MODELS_CACHE = [{"id": "deepseek-v4-flash-free"}, {"id": "paid-x"}]
        relay_mod.MODELS_CACHE_TTL = 0  # stale -> would fetch
        relay_mod.MODELS_CACHE_UPDATED = 0.0

        async def _busy(timeout=None):
            return None

        relay_mod._acquire_semaphore = _busy
        result = await rd.list_models_direct()
        # served from cache, filtered (free-only default)
        ids = [m["id"] for m in result["data"]]
        assert ids == ["deepseek-v4-flash-free"]

    async def test_list_models_non200_serves_cache(self, wired):
        # L116-122: upstream non-200 -> warning, fall through to cache.
        relay_mod, rd = wired
        relay_mod._update_models_cache = lambda lst: None
        relay_mod.MODELS_CACHE = [{"id": "deepseek-v4-flash-free"}]
        relay_mod.MODELS_CACHE_TTL = 0
        relay_mod.MODELS_CACHE_UPDATED = 0.0
        relay_mod._acquire_semaphore = _no_sem

        def _get(url, headers=None):
            return _FakeResp(404)

        original = _swap_asyncclient(rd, lambda *a, **k: _FakeClient(get=_get))
        try:
            result = await rd.list_models_direct()
        finally:
            import httpx

            httpx.AsyncClient = original

        ids = [m["id"] for m in result["data"]]
        assert ids == ["deepseek-v4-flash-free"]  # served from cache

    async def test_list_models_exception_serves_cache(self, wired):
        # L117-118-122: client.get raises -> warning, fall through to cache.
        relay_mod, rd = wired
        relay_mod._update_models_cache = lambda lst: None
        relay_mod.MODELS_CACHE = [{"id": "deepseek-v4-flash-free"}]
        relay_mod.MODELS_CACHE_TTL = 0
        relay_mod.MODELS_CACHE_UPDATED = 0.0
        relay_mod._acquire_semaphore = _no_sem

        def _boom(url, headers=None):
            raise RuntimeError("connection refused")

        original = _swap_asyncclient(rd, lambda *a, **k: _FakeClient(get=_boom))
        try:
            result = await rd.list_models_direct()
        finally:
            import httpx

            httpx.AsyncClient = original

        ids = [m["id"] for m in result["data"]]
        assert ids == ["deepseek-v4-flash-free"]  # served from cache


class TestDirectChatEdge:
    async def test_chat_invalid_json_400(self, wired):
        # L146-147: malformed JSON body -> 400.
        relay_mod, rd = wired

        async def _read(req):
            return b"not-json"

        relay_mod._read_body_capped = _read
        resp = await rd.chat_completions_direct(_req(method="POST"))
        assert resp.status_code == 400

    async def test_chat_no_upstream_503(self, wired):
        # L154: UPSTREAM_BASE empty -> 503 configuration error.
        relay_mod, rd = wired

        async def _read(req):
            return b"{}"

        relay_mod._read_body_capped = _read
        relay_mod.UPSTREAM_BASE = ""
        resp = await rd.chat_completions_direct(_req(method="POST"))
        assert resp.status_code == 503

    async def test_chat_semaphore_busy_503(self, wired):
        # L168: semaphore unavailable -> 503 overloaded.
        relay_mod, rd = wired

        async def _read(req):
            return b"{}"

        relay_mod._read_body_capped = _read

        async def _busy(timeout=None):
            return None

        relay_mod._acquire_semaphore = _busy
        resp = await rd.chat_completions_direct(_req(method="POST"))
        assert resp.status_code == 503

    async def test_chat_xapikey_header(self, wired):
        # L162: x-api-key upstream auth path for the chat route.
        relay_mod, rd = wired

        async def _read(req):
            return b"{}"

        relay_mod._read_body_capped = _read
        relay_mod.UPSTREAM_AUTH_TYPE = "x-api-key"
        relay_mod.UPSTREAM_API_KEY = "up-key"
        seen = {}

        def _post(url, headers=None, content=None):
            return _FakeResp(200, json.dumps({"choices": []}).encode())

        original = _swap_asyncclient(rd, lambda *a, **k: _FakeClient(post=_post, seen=seen))
        try:
            await rd.chat_completions_direct(_req(method="POST"))
        finally:
            import httpx

            httpx.AsyncClient = original

        assert seen["headers"].get("x-api-key") == "up-key"
        assert "Authorization" not in seen["headers"]

    async def test_chat_upstream_4xx_maps(self, wired):
        # L190-194: upstream 4xx (non-5xx) -> pass the 4xx status through.
        relay_mod, rd = wired

        async def _read(req):
            return b"{}"

        relay_mod._read_body_capped = _read

        def _post(url, headers=None, content=None):
            return _FakeResp(404)

        original = _swap_asyncclient(rd, lambda *a, **k: _FakeClient(post=_post))
        try:
            resp = await rd.chat_completions_direct(_req(method="POST"))
        finally:
            import httpx

            httpx.AsyncClient = original

        # 4xx (not 5xx) is passed through, not mapped to 502
        assert resp.status_code == 404

    async def test_chat_upstream_5xx_maps_502(self, wired):
        # L190-194 (5xx branch): upstream 5xx -> 502.
        relay_mod, rd = wired

        async def _read(req):
            return b"{}"

        relay_mod._read_body_capped = _read

        def _post(url, headers=None, content=None):
            return _FakeResp(503)

        original = _swap_asyncclient(rd, lambda *a, **k: _FakeClient(post=_post))
        try:
            resp = await rd.chat_completions_direct(_req(method="POST"))
        finally:
            import httpx

            httpx.AsyncClient = original

        assert resp.status_code == 502
        assert resp.headers["content-type"].startswith("application/json")
