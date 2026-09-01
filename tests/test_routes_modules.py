"""G3 router-split tests: exercise the extracted router modules directly.

The /v1, /admin, and /health handlers moved out of relay.relay into
relay/routes_v1.py, relay/routes_admin.py, and relay/routes_health.py (2026-09-01).
relay.relay re-exports the names for backward compatibility; these tests exercise
the NEW modules directly so the split surface has its own coverage and the modules
are provably standalone (their own router, seam, and callables).

IMPORTANT: relay.relay must NOT be imported at module top-level (collection time).
The relay binds config from os.environ at import (env wins over defaults), and the
production service env exports CLIENT_API_KEY/UPSTREAM_API_KEY. If relay is imported
during collection, it captures those real keys and later tests that expect an open
relay (no client auth) break. All relay/router imports are therefore lazy — inside
fixtures and test bodies.
"""

import importlib

import pytest


@pytest.fixture(scope="module")
def lc():
    """Lazily import the relay + router modules (post-env-patch, at first test run)."""
    relay_mod = importlib.import_module("relay.relay")
    rh = importlib.import_module("relay.routes_health")
    rv = importlib.import_module("relay.routes_v1")
    ra = importlib.import_module("relay.routes_admin")
    return relay_mod, rh, rv, ra


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


# Globals these tests mutate — must be saved/restored for hermetic isolation.
_SET_GLOBALS = [
    "UPSTREAM_BASE",
    "CLIENT_API_KEY",
    "MODELS_CACHE",
    "MODELS_CACHE_UPDATED",
    "MODELS_CACHE_TTL",
    "MODELS_FREE_ONLY",
    "_ADMIN_RATE_LIMIT",
    "_ADMIN_RATE_WINDOW",
    "_admin_rate_hits",
    "pool",
]


@pytest.fixture
def wired(lc):
    """Point all three router seams at relay.relay's live globals; restore on exit."""
    relay_mod, rh, rv, ra = lc
    d = relay_mod.__dict__
    # Point the router modules' seams at the live relay dict (same as relay.py wires
    # at import; re-do here so tests are standalone even if a prior test re-wired).
    rh._relay_globals = d
    rv._relay_globals = d
    ra._relay_globals = d
    saved = {k: d.get(k) for k in _SET_GLOBALS}
    # Hermetic: no client auth, no upstream
    d["CLIENT_API_KEY"] = ""
    d["UPSTREAM_BASE"] = "https://example.com/v1"
    d["_ADMIN_RATE_LIMIT"] = 20
    d["_ADMIN_RATE_WINDOW"] = 60
    d["_admin_rate_hits"].clear()
    try:
        yield relay_mod, rh, rv, ra
    finally:
        models_cache_saved = saved.get("MODELS_CACHE")
        for k, v in saved.items():
            if k == "MODELS_CACHE":
                continue
            d[k] = v
        if models_cache_saved is not None:
            d["MODELS_CACHE"].clear()
            d["MODELS_CACHE"].extend(models_cache_saved)
        rh._relay_globals = d
        rv._relay_globals = d
        ra._relay_globals = d


class TestHealthModule:
    def test_router_is_apirouter(self, lc):
        _, rh, _, _ = lc
        assert "/health" in _route_paths(rh.router)

    async def test_health_direct(self, wired):
        relay_mod, rh, _, _ = wired
        relay_mod.pool = relay_mod.CooldownPool(["socks5://u1:p1@p1:1080"])
        result = await rh.health()
        assert result["status"] in ("ok", "degraded")
        assert "pool_stats" in result
        assert "models_available" in result
        assert "security" in result
        assert "auth_switch" in result


class TestV1Module:
    def test_router_is_apirouter(self, lc):
        _, _, rv, _ = lc
        paths = _route_paths(rv.router)
        assert "/v1/models" in paths
        assert "/go/v1/models" in paths
        assert "/v1/chat/completions" in paths
        assert "/go/v1/chat/completions" in paths
        assert "/v1/{path:path}" in paths
        assert "/go/{path:path}" in paths

    async def test_list_models_no_upstream_empty(self, wired):
        relay_mod, _, rv, _ = wired
        relay_mod.UPSTREAM_BASE = ""
        relay_mod.MODELS_CACHE.clear()
        result = await rv.list_models()
        assert result == {"object": "list", "data": []}

    async def test_list_models_serves_cache(self, wired):
        relay_mod, _, rv, _ = wired
        relay_mod.UPSTREAM_BASE = "https://example.com/v1"
        relay_mod.MODELS_CACHE.clear()
        relay_mod.MODELS_CACHE.extend([{"id": "cached-model"}])
        relay_mod.MODELS_CACHE_UPDATED = 9999999999.0
        relay_mod.MODELS_CACHE_TTL = 600
        relay_mod.MODELS_FREE_ONLY = False
        result = await rv.list_models()
        assert [m["id"] for m in result["data"]] == ["cached-model"]

    async def test_proxy_all_rejects_bad_client(self, wired):
        relay_mod, _, rv, _ = wired
        relay_mod.CLIENT_API_KEY = "sekrit"
        resp = await rv._proxy_all_impl("chat/completions", _req(method="POST"), go=False)
        assert resp.status_code == 401


class TestAdminModule:
    def test_router_is_apirouter(self, lc):
        _, _, _, ra = lc
        paths = _route_paths(ra.router)
        assert "/admin/upstream-health" in paths
        assert "/admin/clear-cooldowns" in paths
        assert "/admin/reset-proxy" in paths
        assert "/admin/reload-proxies" in paths
        assert "/admin/reset-by-errors" in paths
        assert "/admin/reload-config" in paths

    async def test_admin_clear_cooldowns(self, wired):
        relay_mod, _, _, ra = wired
        relay_mod.CLIENT_API_KEY = ""
        result = await ra.admin_clear_cooldowns(_req())
        assert isinstance(result, dict)
        assert result.get("status") == "ok"

    async def test_admin_reset_by_errors(self, wired):
        relay_mod, _, _, ra = wired
        relay_mod.CLIENT_API_KEY = ""
        result = await ra.admin_reset_by_errors(_req())
        assert isinstance(result, dict)
        assert result.get("status") == "ok"
        assert "Reset" in result.get("message", "")


class TestRelayIdentity:
    """The split preserved the relay.relay contract: re-exports are the SAME objects."""

    def test_re_exports_same_object(self, lc):
        relay_mod, rh, rv, ra = lc
        assert relay_mod.health is rh.health
        assert relay_mod.list_models is rv.list_models
        assert relay_mod.chat_completions is rv.chat_completions
        assert relay_mod.go_chat_completions is rv.go_chat_completions
        assert relay_mod.proxy_all is rv.proxy_all
        assert relay_mod.go_proxy_all is rv.go_proxy_all
        assert relay_mod.admin_upstream_health is ra.admin_upstream_health
        assert relay_mod.admin_clear_cooldowns is ra.admin_clear_cooldowns
        assert relay_mod.admin_reset_proxy is ra.admin_reset_proxy
        assert relay_mod.admin_reload_proxies is ra.admin_reload_proxies
        assert relay_mod.admin_reset_by_errors is ra.admin_reset_by_errors
        assert relay_mod.admin_reload_config is ra.admin_reload_config

    def test_handlers_live_in_router_modules(self, lc):
        _, rh, rv, ra = lc
        assert rh.health.__module__ == "relay.routes_health"
        assert rv.list_models.__module__ == "relay.routes_v1"
        assert ra.admin_clear_cooldowns.__module__ == "relay.routes_admin"

    def test_seam_is_live_reference(self, lc):
        """The router seam stores the LIVE relay dict (not a copy) — the monkeypatch contract."""
        relay_mod, rh, rv, ra = lc
        sentinel = {}
        rh._relay_globals = sentinel
        try:
            assert rh._relay_globals is sentinel
        finally:
            rh._relay_globals = relay_mod.__dict__

    def test_reload_survives(self):
        """importlib.reload(relay) must not break the router seams (module identity persists).

        After reload, relay.py re-wires the seams to the (new) module dict of the
        reloaded relay. The router modules themselves keep identity in sys.modules.
        """
        import sys

        relay_mod = importlib.import_module("relay.relay")
        before_router = sys.modules.get("relay.routes_v1")
        importlib.reload(relay_mod)
        assert sys.modules.get("relay.routes_v1") is before_router
        assert sys.modules["relay.routes_v1"]._relay_globals is relay_mod.__dict__
        assert sys.modules["relay.routes_admin"]._relay_globals is relay_mod.__dict__
        assert sys.modules["relay.routes_health"]._relay_globals is relay_mod.__dict__
        assert relay_mod.list_models is sys.modules["relay.routes_v1"].list_models
        assert relay_mod.admin_clear_cooldowns is sys.modules["relay.routes_admin"].admin_clear_cooldowns
