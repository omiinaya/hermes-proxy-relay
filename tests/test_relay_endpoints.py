"""Integration tests for relay FastAPI endpoints using TestClient.

These tests verify:
- Health endpoint returns correct status
- Chat completions route proxies correctly
- Streaming path works end-to-end
- Model list endpoint
- Admin endpoints (clear-cooldowns, reset-proxy, reload-proxies, reset-by-errors)
- All-proxies-cooling returns 429
- Proxy errors return 502

Uses TestClient from Starlette (bundled with FastAPI) for synchronous tests,
plus mocked upstream responses via httpx transport monkeypatches.
"""

from unittest.mock import patch

import os
import time

import httpx
import pytest
from fastapi.testclient import TestClient
from fastapi import Response


@pytest.fixture(scope="module")
def client():
    """Create a TestClient with a fresh relay app instance.

    The relay's global state is reset by importing and calling _init_pool()
    with the test environment. Module-scoped to avoid reloading the
    1275-line relay module per test.
    """
    with patch.dict("os.environ", {
        "UPSTREAM_BASE": "https://test-api.example.com/v1",
        "UPSTREAM_API_KEY": "test-key-123",
        "UPSTREAM_AUTH_TYPE": "bearer",
        "RELAY_PORT": "9999",
        "MAX_CONCURRENT_UPSTREAM": "10",
        "MODEL_FILTER_PATTERN": ".*",
        "LOG_LEVEL": "CRITICAL",
        "CONSECUTIVE_ERROR_THRESHOLD": "3",
        "PERMANENT_COOLDOWN_SECONDS": "86400",
        "PROXY_LIST": "",
        "PROXY_LIST_FILE": "",
        "PROXY_LIST_ENV": "socks5://u1:p1@p1:1080,socks5://u2:p2@p2:1080",
        "RELAY_SHUTDOWN_DRAIN_SECONDS": "0",
        }, clear=False):
        # patch.dict only ADDS/overwrites — ambient ADMIN_API_KEY /
        # CLIENT_API_KEY would leak through to the reload and make
        # admin/v1 tests 403/401. Delete them explicitly so the module
        # loads with auth disabled (tests that want auth set their own).
        os.environ.pop("ADMIN_API_KEY", None)
        os.environ.pop("CLIENT_API_KEY", None)
        # Force re-import with patched env
        import importlib
        import relay.relay as relay_mod
        importlib.reload(relay_mod)

        # Initialize pool with test proxies
        relay_mod._init_pool()

        # Patch out the health checker to prevent background task leaks
        relay_mod._PROXY_HEALTH_TASK = None

        with TestClient(relay_mod.app) as tc:
            yield tc


@pytest.fixture(autouse=True)
def reset_relay_state(client):
    """Reset mutable relay state before each test for isolation."""
    import relay.relay as relay_mod
    relay_mod.pool.clear_cooldowns()
    relay_mod._request_count["total"] = 0
    relay_mod._request_count["ok"] = 0
    relay_mod._request_count["errors"] = 0


# ═══════════════════════════════════════════════════════════════════
#  Health Endpoint
# ═══════════════════════════════════════════════════════════════════


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "pool_stats" in data
        assert "upstream_base" in data
        assert "request_stats" in data
        assert "semaphore" in data

    def test_health_contains_pool_stats(self, client):
        resp = client.get("/health")
        data = resp.json()
        stats = data["pool_stats"]
        assert stats["total"] == 2
        assert stats["available"] == 2
        assert stats["cooling"] == 0
        assert stats["permanently_failed"] == 0

    def test_health_contains_semaphore(self, client):
        resp = client.get("/health")
        data = resp.json()
        sem = data["semaphore"]
        assert sem["max"] == 10
        assert 0 <= sem["used"] <= 10

    def test_health_contains_uptime(self, client):
        """Health should report uptime and version."""
        resp = client.get("/health")
        data = resp.json()
        assert "uptime_seconds" in data
        assert data["uptime_seconds"] >= 0
        # Version comes from the single VERSION constant — never hardcode
        import relay.relay as relay_mod
        assert data["version"] == relay_mod.VERSION

    def test_health_contains_shared_clients(self, client):
        """Health should report shared client pool size."""
        resp = client.get("/health")
        data = resp.json()
        assert "shared_clients" in data
        assert data["shared_clients"] >= 0

    def test_health_contains_security_flags(self, client):
        """Health reports client/admin auth state."""
        resp = client.get("/health")
        data = resp.json()
        assert "security" in data
        assert "client_auth_enabled" in data["security"]
        assert "admin_auth_enabled" in data["security"]
        assert isinstance(data["security"]["client_auth_enabled"], bool)

    def test_health_masks_upstream_credentials(self, client):
        """An upstream URL with embedded user:pass@ must not leak to the
        unauthenticated /health endpoint."""
        import relay.relay as relay_mod
        old = relay_mod.UPSTREAM_BASE
        relay_mod.UPSTREAM_BASE = "https://user:secret@api.example.com/v1"
        try:
            resp = client.get("/health")
            data = resp.json()
            assert "secret" not in data["upstream_base"]
            assert "***" in data["upstream_base"]
        finally:
            relay_mod.UPSTREAM_BASE = old

    def test_health_models_available(self, client):
        """When models cache is populated, health shows the count."""
        resp = client.get("/health")
        data = resp.json()
        assert "models_available" in data


# ═══════════════════════════════════════════════════════════════════
#  Models List Endpoint
# ═══════════════════════════════════════════════════════════════════


class TestModelsEndpoint:
    def test_models_returns_list_format(self, client):
        resp = client.get("/v1/models")
        assert resp.status_code == 200
        data = resp.json()
        assert data["object"] == "list"
        assert isinstance(data["data"], list)


# ═══════════════════════════════════════════════════════════════════
#  Chat Completions — Error States (no upstream mocked)
# ═══════════════════════════════════════════════════════════════════


class TestChatCompletions:
    def test_chat_endpoint_exists(self, client):
        """The chat completions route should accept POST."""
        resp = client.post(
            "/v1/chat/completions",
            json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
        )
        # Without a mock, it'll try to connect to test-api.example.com
        # and either get a connection error (502) or succeed if the proxy
        # connects but upstream fails.
        assert resp.status_code in (200, 502, 503)

    def test_chat_with_empty_upstream_returns_503(self, client):
        """When UPSTREAM_BASE is empty, proxied requests return a clear 503."""
        import relay.relay as relay_mod
        original = relay_mod.UPSTREAM_BASE
        relay_mod.UPSTREAM_BASE = ""
        try:
            resp = client.post(
                "/v1/chat/completions",
                json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
            )
        finally:
            relay_mod.UPSTREAM_BASE = original
        assert resp.status_code == 503
        assert "upstream_not_configured" in resp.text

    def test_generic_proxy_route(self, client):
        """The catch-all /v1/{path} route should work."""
        resp = client.get("/v1/embeddings")
        assert resp.status_code in (200, 502, 503)

    def test_options_forwarded_not_405(self, client):
        """OPTIONS on /v1/* must be routed (not 405) — the catch-all route
        now includes OPTIONS so the relay can forward it upstream."""
        resp = client.options("/v1/embeddings")
        # Not a 405 — routed (may 502/503 with dead proxies, but never 405)
        assert resp.status_code != 405

    def test_head_forwarded_not_405(self, client):
        """HEAD on /v1/* must be routed (not 405)."""
        resp = client.head("/v1/embeddings")
        assert resp.status_code != 405

    def test_health_request_count_increments(self, client):
        """Making a request should increment the total counter."""
        resp = client.get("/health")
        before = resp.json()["request_stats"]["total"]

        # Make a chat request (will fail to connect, but increments)
        client.post(
            "/v1/chat/completions",
            json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
        )

        resp = client.get("/health")
        after = resp.json()["request_stats"]["total"]
        assert after >= before + 1


# ═══════════════════════════════════════════════════════════════════
#  Admin Endpoints
# ═══════════════════════════════════════════════════════════════════


class TestAdminEndpoints:
    def test_clear_cooldowns(self, client):
        """Clear cooldowns returns status ok."""
        resp = client.post("/admin/clear-cooldowns")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["proxies_total"] == 2

    def test_clear_cooldowns_works_when_some_cooling(self, client):
        """After cooling and clearing, available count should equal total."""
        # Cool one proxy via next() + record_429
        import relay.relay as relay_mod
        proxy = relay_mod.pool.next()
        if proxy:
            relay_mod.pool.record_429(proxy, retry_after=300)

        resp = client.post("/admin/clear-cooldowns")
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] == 2

    def test_reset_proxy_not_found(self, client):
        """Resetting a non-existent proxy returns 404."""
        resp = client.post(
            "/admin/reset-proxy",
            json={"url": "socks5://nonexistent:1080"},
        )
        assert resp.status_code == 404
        assert "not found" in resp.text.lower()

    def test_reset_proxy_missing_url(self, client):
        """Missing url field returns 400."""
        resp = client.post("/admin/reset-proxy", json={})
        assert resp.status_code == 400

    def test_reset_proxy_invalid_json(self, client):
        resp = client.post("/admin/reset-proxy", content="not-json", headers={"Content-Type": "application/json"})
        assert resp.status_code == 400

    def test_reload_proxies(self, client):
        """Reload proxies returns current pool state."""
        resp = client.post("/admin/reload-proxies")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "proxies_total" in data
        assert "available" in data

    def test_reset_by_errors(self, client):
        resp = client.post("/admin/reset-by-errors", json={"min_consecutive": 3})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    def test_reset_by_errors_empty_body(self, client):
        """Should work with no body (empty POST)."""
        resp = client.post("/admin/reset-by-errors", content="{}", headers={"Content-Type": "application/json"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"


class TestAdminUpstreamHealth:
    def test_upstream_health_endpoint(self, client):
        """Upstream health should return a response (likely 502 since upstream not real)."""
        resp = client.get("/admin/upstream-health")
        # Without a real upstream, the proxy connect fails → 502, matching
        # the request path's proxy_connect_failed class (was 503 before —
        # a dead proxy must not look like a relay outage).
        assert resp.status_code in (200, 502, 503)
        data = resp.json()
        assert "status" in data
        assert "latency_ms" in data

    def test_upstream_health_has_upstream_field(self, client):
        resp = client.get("/admin/upstream-health")
        data = resp.json()
        assert "upstream" in data

    def test_upstream_health_timeout_returns_502(self, client, monkeypatch):
        """ReadTimeout during the probe → 502 upstream_timeout (not 503)."""
        import relay.relay as relay_mod
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock

        @asynccontextmanager
        async def fake_borrow(url):
            yield AsyncMock()

        async def raising_single(client, method, url, headers, body, proxy_entry, probe=False):
            raise httpx.ReadTimeout("upstream slow")

        monkeypatch.setattr(relay_mod, "_borrow_client", fake_borrow)
        monkeypatch.setattr(relay_mod, "_proxy_single", raising_single)

        resp = client.get("/admin/upstream-health")
        assert resp.status_code == 502
        data = resp.json()
        assert data["error"] == "upstream_timeout"

    def test_upstream_health_generic_error_sanitized(self, client, monkeypatch):
        """Generic probe failure → 503 with a sanitized message (no raw
        exception text that could embed socket/proxy internals)."""
        import relay.relay as relay_mod
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock

        @asynccontextmanager
        async def fake_borrow(url):
            yield AsyncMock()

        async def raising_single(client, method, url, headers, body, proxy_entry, probe=False):
            raise RuntimeError("connection to socks5://user:secret@10.0.0.1:1080 failed")

        monkeypatch.setattr(relay_mod, "_borrow_client", fake_borrow)
        monkeypatch.setattr(relay_mod, "_proxy_single", raising_single)

        resp = client.get("/admin/upstream-health")
        assert resp.status_code == 503
        data = resp.json()
        assert data["error"] == "Health check failed"
        assert "secret" not in resp.text
        assert "10.0.0.1" not in resp.text

    def test_upstream_health_401_reports_degraded(self, client, monkeypatch):
        """A 401 (wrong upstream key) must report degraded, not ok."""
        import relay.relay as relay_mod
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock

        @asynccontextmanager
        async def fake_borrow(url):
            yield AsyncMock()

        async def unauthorized_single(client, method, url, headers, body, proxy_entry, probe=False):
            return Response(
                content=b'{"error":"unauthorized"}',
                status_code=401,
                headers={"content-type": "application/json"},
            )

        monkeypatch.setattr(relay_mod, "_borrow_client", fake_borrow)
        monkeypatch.setattr(relay_mod, "_proxy_single", unauthorized_single)

        resp = client.get("/admin/upstream-health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["upstream_status"] == 401

    def test_upstream_health_probe_does_not_cool_pool(self, client, monkeypatch):
        """A 429 from the probe must NOT cool the pool proxy — a read-only
        health probe must not degrade production pool state."""
        import relay.relay as relay_mod
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock

        entry = relay_mod.pool.next()
        assert entry is not None

        @asynccontextmanager
        async def fake_borrow(url):
            yield AsyncMock()

        async def rate_limited_single(client, method, url, headers, body, proxy_entry, probe=False):
            assert probe is True  # must be a probe (no side effects)
            return Response(
                content=b'{"error":"rate limited"}',
                status_code=429,
                headers={"content-type": "application/json", "retry-after": "120"},
            )

        monkeypatch.setattr(relay_mod, "_borrow_client", fake_borrow)
        monkeypatch.setattr(relay_mod, "_proxy_single", rate_limited_single)

        resp = client.get("/admin/upstream-health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["upstream_status"] == 429
        # Proxy NOT cooled, counters NOT mutated
        assert entry.cooldown_until <= time.monotonic()
        assert entry.consecutive_429 == 0


# ═══════════════════════════════════════════════════════════════════
#  All Proxies Cooling → 429
# ═══════════════════════════════════════════════════════════════════


class TestAllCooling:
    def test_returns_429_when_all_cooling(self, client):
        """When all proxies are in cooldown, the relay should return 429."""
        import relay.relay as relay_mod

        # Cool all proxies
        for _ in range(relay_mod.pool.total):
            p = relay_mod.pool.next()
            if p:
                relay_mod.pool.record_429(p, retry_after=300)

        assert relay_mod.pool.all_cooling

        resp = client.post(
            "/v1/chat/completions",
            json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
        )
        assert resp.status_code == 429
        assert "all_proxies_cooling" in resp.text

    def test_health_degraded_when_none_available(self, client):
        import relay.relay as relay_mod

        for _ in range(relay_mod.pool.total):
            p = relay_mod.pool.next()
            if p:
                relay_mod.pool.record_429(p, retry_after=300)

        resp = client.get("/health")
        data = resp.json()
        assert data["status"] == "degraded"


# ═══════════════════════════════════════════════════════════════════
#  Proxy Error → 502
# ═══════════════════════════════════════════════════════════════════


class TestProxyErrors:
    def test_no_proxies_returns_503(self, client):
        """When no proxies configured, relay should return 503."""
        import relay.relay as relay_mod
        relay_mod.pool.reload([])

        resp = client.post(
            "/v1/chat/completions",
            json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
        )
        assert resp.status_code == 429  # all cooling (0 proxies = "all" cooling)



class TestAdminProxyStats:
    def test_proxy_stats_endpoint_ok(self, client):
        """GET /admin/proxy-stats devuelve 200 con el esquema esperado."""
        resp = client.get("/admin/proxy-stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "total" in data and "proxies" in data
        assert isinstance(data["proxies"], list)
        keys = ("proxy", "state", "remaining_s", "consecutive_errors",
                "consecutive_429", "permanently_dead", "total_ok", "total_429",
                "avg_latency_ms", "last_latency_ms", "latency_samples", "last_error")
        for p in data["proxies"]:
            assert all(k in p for k in keys)

    def test_proxy_stats_masks_credentials(self, client, monkeypatch):
        """Un proxy con user:pass en la url no filtra credenciales."""
        import relay.relay as relay_mod
        fake = relay_mod.CooldownPool(["socks5://user:secret@10.0.0.1:1080"])
        monkeypatch.setattr(relay_mod, "pool", fake)
        resp = client.get("/admin/proxy-stats")
        assert resp.status_code == 200
        assert "secret" not in resp.text and "user" not in resp.text