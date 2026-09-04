"""Runtime-switchable proxy profile tests (2026-09-04).

Covers the profile registry, the hot-swap (switch_profile), profile-mode
_init_pool / _reload_active_profile, the proxy-source builder, and the
/admin/profile + /admin/reload-proxies admin surface.

Hermetic: these run under the suite's RELAY_CONFIG="" (pure defaults,
PROFILE_DEFS empty -> the legacy single-pool path is byte-identical, which
the other 730 tests prove). Profile-mode behavior is exercised by
monkeypatching the relay module's PROFILE_DEFS / PROFILES_DIR /
DEFAULT_PROFILE / registry / active_profile / pool and restoring them after
each test.
"""

import importlib

import pytest


@pytest.fixture(scope="module")
def lc():
    """Lazily import relay + routes_admin (post-env-patch)."""
    relay_mod = importlib.import_module("relay.relay")
    ra = importlib.import_module("relay.routes_admin")
    return relay_mod, ra


@pytest.fixture
def profile_env(lc, tmp_path, monkeypatch):
    """A hermetic profile environment with two profiles (a, b) on temp files.

    Creates <tmp>/profiles/{a.txt,b.txt} with distinct proxy URLs, points the
    relay's PROFILES_DIR / PROFILE_DEFS / DEFAULT_PROFILE there, builds the
    registry, and saves/restores every module global it mutates.
    """
    relay_mod, _ = lc
    pdir = tmp_path / "profiles"
    pdir.mkdir()
    (pdir / "a.txt").write_text(
        "socks5h://au:ap@a:1080\nsocks5h://au2:ap2@a:1081\n"
    )
    (pdir / "b.txt").write_text("socks5h://bu:bp@b:1080\n")
    # A relative path must resolve under PROFILES_DIR (CWD-independent).
    (pdir / "b_env.txt").write_text("socks5h://be:be2@be:1080\n")

    saved = {
        "PROFILE_DEFS": relay_mod.PROFILE_DEFS,
        "PROFILES_DIR": relay_mod.PROFILES_DIR,
        "DEFAULT_PROFILE": relay_mod.DEFAULT_PROFILE,
        "active_profile": relay_mod.active_profile,
        "registry": relay_mod.registry,
        "pool": relay_mod.pool,
    }
    monkeypatch.setattr(relay_mod, "PROFILES_DIR", str(pdir))
    monkeypatch.setattr(
        relay_mod,
        "PROFILE_DEFS",
        [
            {"name": "a", "proxies": "a.txt"},
            {"name": "b", "proxies": "b.txt"},
        ],
    )
    monkeypatch.setattr(relay_mod, "DEFAULT_PROFILE", "a")
    relay_mod.registry = relay_mod._build_profile_registry()
    relay_mod.active_profile = "a"
    # Point routes_admin's seam at the live relay dict (so _G resolves).
    ra = importlib.import_module("relay.routes_admin")
    ra._relay_globals = relay_mod.__dict__
    saved["ra_seam"] = ra._relay_globals

    yield relay_mod, ra, pdir

    for k, v in saved.items():
        if k == "ra_seam":
            continue
        setattr(relay_mod, k, v)
    ra._relay_globals = saved["ra_seam"]


# ── helpers ─────────────────────────────────────────────────────────────


def _req(method="GET", path="/admin/profile", body=None, client_host="10.0.0.1"):
    from starlette.requests import Request

    if body is None:
        body = b""
    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
        "query_string": b"",
        "client": (client_host, 1234),
        "receive": receive,
    }
    return Request(scope)


# ── ProfileRegistry unit tests ──────────────────────────────────────────


class TestRegistry:
    def test_names_and_defined(self, profile_env):
        relay_mod, _, _ = profile_env
        reg = relay_mod._build_profile_registry()
        assert set(reg.names()) == {"a", "b"}
        assert reg.defined("a")
        assert reg.defined("b")
        assert not reg.defined("z")

    def test_build_isolated_and_cached(self, profile_env):
        relay_mod, _, _ = profile_env
        reg = relay_mod._build_profile_registry()
        a1 = reg.build("a")
        a2 = reg.build("a")  # cached — same object
        b1 = reg.build("b")
        assert a1 is a2, "build must cache (resume state, not rebuild)"
        assert a1 is not b1, "distinct profiles must be distinct pools"
        assert a1.total == 2  # a.txt has 2 proxies
        assert b1.total == 1  # b.txt has 1 proxy

    def test_build_unknown_raises(self, profile_env):
        relay_mod, _, _ = profile_env
        reg = relay_mod._build_profile_registry()
        with pytest.raises(KeyError):
            reg.build("z")

    def test_stats_never_leaks_urls(self, profile_env):
        relay_mod, _, _ = profile_env
        reg = relay_mod._build_profile_registry()
        reg.build("a")
        s = reg.stats()
        assert set(s) == {"a"}
        assert s["a"]["total"] == 2
        # No proxy URL may appear anywhere in the stats payload.
        flat = str(s)
        assert "@a:1080" not in flat and "au:" not in flat

    def test_refresh_rereads_source(self, profile_env):
        relay_mod, _, pdir = profile_env
        reg = relay_mod._build_profile_registry()
        before = reg.build("b").total
        # Append a proxy to the active (b) source file, then refresh — the
        # fresh count must reflect the edit (no restart, no cache).
        (pdir / "b.txt").write_text(
            "socks5h://bu:bp@b:1080\nsocks5h://bu2:bp2@b:1081\n"
        )
        new = reg.refresh("b")
        assert before == 1
        assert new.total == 2, "refresh must re-read the edited file"

    def test_refresh_drops_removed_profile(self, profile_env, monkeypatch):
        relay_mod, _, _ = profile_env
        reg = relay_mod._build_profile_registry()
        reg.build("b")
        # Remove 'b' from the live specs; refresh must drop the cached pool.
        relay_mod.PROFILE_DEFS = [{"name": "a", "proxies": "a.txt"}]
        with pytest.raises(KeyError):
            reg.refresh("b")
        assert "b" not in reg.stats()

    def test_drop_forgets_pool(self, profile_env):
        relay_mod, _, _ = profile_env
        reg = relay_mod._build_profile_registry()
        reg.build("b")
        assert "b" in reg.stats()
        reg.drop("b")
        assert "b" not in reg.stats()


# ── source builder + path resolution ────────────────────────────────────


class TestSourceBuilder:
    def test_relative_resolves_under_profiles_dir(self, profile_env):
        relay_mod, _, _ = profile_env
        urls = relay_mod._build_profile_proxies("a.txt")
        assert len(urls) == 2
        assert all(u.startswith("socks5h://") for u in urls)

    def test_absolute_path(self, profile_env, tmp_path):
        relay_mod, _, pdir = profile_env
        abspath = pdir / "a.txt"
        urls = relay_mod._build_profile_proxies(str(abspath))
        assert len(urls) == 2

    def test_dict_file_form(self, profile_env):
        relay_mod, _, _ = profile_env
        urls = relay_mod._build_profile_proxies({"file": "b.txt"})
        assert len(urls) == 1

    def test_env_form(self):
        import relay.relay as r
        urls = r._build_profile_proxies(
            {"env": "socks5h://eu:ep@e:1080, socks5h://eu:ep@e:1081"}
        )
        assert len(urls) == 2

    def test_dedup(self):
        import relay.relay as r
        urls = r._build_profile_proxies(
            {"env": "socks5h://eu:ep@e:1080,socks5h://eu:ep@e:1080"}
        )
        assert len(urls) == 1

    def test_invalid_dropped(self):
        import relay.relay as r
        urls = r._build_profile_proxies(
            {"env": "socks5h://eu:ep@e:1080,not-a-proxy,bad://x"}
        )
        assert len(urls) == 1

    def test_bad_spec_returns_empty(self):
        import relay.relay as r
        assert r._build_profile_proxies(12345) == []

    def test_resolve_relative_vs_absolute(self, profile_env, monkeypatch):
        relay_mod, _, pdir = profile_env
        assert relay_mod._resolve_profile_file_path("a.txt") == str(pdir / "a.txt")
        assert relay_mod._resolve_profile_file_path("/abs/a.txt") == "/abs/a.txt"


# ── switch_profile (the hot-swap) ───────────────────────────────────────


class TestSwitch:
    def test_switch_rebinds_pool(self, profile_env):
        relay_mod, _, _ = profile_env
        before_pool = relay_mod.pool
        before_active = relay_mod.active_profile
        result = relay_mod.switch_profile("b")
        assert result["status"] == "ok"
        assert result["active"] == "b"
        assert result["proxies_total"] == 1
        assert relay_mod.pool is not before_pool, "switch must rebind to a new pool"
        assert relay_mod.active_profile == "b"

    def test_switch_resumes_state(self, profile_env):
        relay_mod, _, _ = profile_env
        # Build b, cool one of its proxies, switch away, then back — the
        # cooled proxy must STILL be cooling (state resumed, not rebuilt).
        relay_mod.switch_profile("b")
        b_pool = relay_mod.pool
        target = b_pool._proxies[0]
        b_pool.record_429(target, retry_after=3600)
        relay_mod.switch_profile("a")
        assert relay_mod.active_profile == "a"
        relay_mod.switch_profile("b")
        # Back on b: it must be the SAME cached pool (state preserved).
        assert relay_mod.pool is b_pool
        assert relay_mod.pool.available_count == 0  # still cooling

    def test_switch_unknown_404(self, profile_env):
        relay_mod, _, _ = profile_env
        before_pool = relay_mod.pool
        result = relay_mod.switch_profile("z")
        assert result["status"] == "error"
        assert "z" in result["error"]
        assert relay_mod.pool is before_pool, "failed switch must not rebind"
        assert relay_mod.active_profile == "a"

    def test_switch_no_profiles(self, lc, monkeypatch):
        relay_mod, _ = lc
        monkeypatch.setattr(relay_mod, "PROFILE_DEFS", [])
        monkeypatch.setattr(relay_mod, "registry", None)
        result = relay_mod.switch_profile("a")
        assert result["status"] == "error"
        assert "No proxy profiles" in result["error"]


# ── profile-mode _init_pool / _reload_active_profile ────────────────────


class TestLifecycle:
    def test_init_pool_profile_mode_activates_default(self, profile_env):
        relay_mod, _, _ = profile_env
        # Simulate the app-lifespan call. PROFILE_DEFS is set, so it must
        # build the registry + activate DEFAULT_PROFILE ("a").
        relay_mod.registry = None
        relay_mod._init_pool()
        assert relay_mod.registry is not None
        assert relay_mod.active_profile == "a"
        assert relay_mod.pool.total == 2  # a.txt = 2 proxies

    def test_reload_does_not_snap_to_default(self, profile_env):
        relay_mod, _, pdir = profile_env
        # Switch to b, then reload — the ACTIVE profile must stay b (not
        # revert to DEFAULT "a"), and b's freshly-edited file is picked up.
        relay_mod.switch_profile("b")
        (pdir / "b.txt").write_text(
            "socks5h://bu:bp@b:1080\nsocks5h://bu2:bp2@b:1081\n"
        )
        relay_mod._reload_active_profile()
        assert relay_mod.active_profile == "b", "reload must not snap to DEFAULT"
        assert relay_mod.pool.total == 2  # edited file re-read

    def test_reload_removal_yields_empty_pool(self, profile_env, monkeypatch):
        relay_mod, _, _ = profile_env
        relay_mod.switch_profile("b")
        # Operator removed 'b' from config after the switch.
        relay_mod.PROFILE_DEFS = [{"name": "a", "proxies": "a.txt"}]
        relay_mod._reload_active_profile()
        assert relay_mod.active_profile == "b"  # unchanged
        assert relay_mod.pool.total == 0, "removed active profile -> empty pool"
        # Operator can now switch to a valid profile.
        assert relay_mod.switch_profile("a")["status"] == "ok"
        assert relay_mod.pool.total == 2


# ── admin surface (drives the real app via httpx.ASGITransport) ─────
#
# httpx.ASGITransport runs the FULL app (all middleware + routers) in-process
# WITHOUT executing the ASGI lifespan (no TestClient context manager is used),
# so _init_pool() is NOT re-run — the tests exercise the module globals +
# registry that the profile_env / lc fixtures have already seeded, through the
# real middleware chain (admin auth, rate limit, CORS) and the real route
# handlers. This is a faithful end-to-end test of the admin surface.

import httpx


def _http_client_ctx(relay_mod):
    """Async context manager yielding an httpx client bound to the live app."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _client():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=relay_mod.app),
            base_url="http://testrelay",
            timeout=10.0,
        ) as c:
            yield c

    return _client


class TestAdmin:
    async def test_list_profiles_enabled(self, profile_env):
        relay_mod, ra, pdir = profile_env
        async with _http_client_ctx(relay_mod)() as c:
            resp = await c.get("/admin/profile")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] == "ok"
        assert data["profiles_enabled"] is True
        assert data["active"] == "a"
        names = {p["name"] for p in data["profiles"]}
        assert names == {"a", "b"}
        for p in data["profiles"]:
            assert p["source"] == "file"

    async def test_list_profiles_legacy(self, lc, monkeypatch):
        relay_mod, ra = lc
        monkeypatch.setattr(relay_mod, "PROFILE_DEFS", [])
        ra._relay_globals = relay_mod.__dict__
        async with _http_client_ctx(relay_mod)() as c:
            resp = await c.get("/admin/profile")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["profiles_enabled"] is False

    async def test_switch_ok(self, profile_env):
        relay_mod, ra, pdir = profile_env
        async with _http_client_ctx(relay_mod)() as c:
            resp = await c.post("/admin/profile", json={"name": "b"})
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] == "ok"
        assert data["active"] == "b"
        assert relay_mod.active_profile == "b"

    async def test_switch_unknown_404(self, profile_env):
        relay_mod, ra, pdir = profile_env
        async with _http_client_ctx(relay_mod)() as c:
            resp = await c.post("/admin/profile", json={"name": "z"})
        assert resp.status_code == 404, resp.text
        assert "z" in resp.text

    async def test_switch_not_configured_503(self, lc, monkeypatch):
        relay_mod, ra = lc
        monkeypatch.setattr(relay_mod, "PROFILE_DEFS", [])
        ra._relay_globals = relay_mod.__dict__
        async with _http_client_ctx(relay_mod)() as c:
            resp = await c.post("/admin/profile", json={"name": "a"})
        assert resp.status_code == 503, resp.text

    async def test_switch_bad_body_400(self, profile_env):
        relay_mod, ra, pdir = profile_env
        async with _http_client_ctx(relay_mod)() as c:
            resp1 = await c.post("/admin/profile", content=b"not-json",
                                 headers={"Content-Type": "application/json"})
            resp2 = await c.post("/admin/profile", json={"name": ""})
        assert resp1.status_code == 400, resp1.text
        assert resp2.status_code == 400, resp2.text

    async def test_reload_proxies_profile_mode_keeps_active(self, profile_env):
        relay_mod, ra, pdir = profile_env
        # Pre-switch to b; the reload must keep active=b (not snap to DEFAULT).
        relay_mod.switch_profile("b")
        (pdir / "b.txt").write_text(
            "socks5h://bu:bp@b:1080\nsocks5h://bu2:bp2@b:1081\n"
        )
        async with _http_client_ctx(relay_mod)() as c:
            resp = await c.post("/admin/reload-proxies")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] == "ok"
        assert data["active"] == "b", "reload must not snap to DEFAULT"
        assert data["proxies_total"] == 2

    async def test_reload_proxies_legacy(self, lc, monkeypatch):
        relay_mod, ra = lc
        monkeypatch.setattr(relay_mod, "PROFILE_DEFS", [])
        ra._relay_globals = relay_mod.__dict__
        async with _http_client_ctx(relay_mod)() as c:
            resp = await c.post("/admin/reload-proxies")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] == "ok"
        assert "active" not in data, "legacy path must not emit a profile field"
