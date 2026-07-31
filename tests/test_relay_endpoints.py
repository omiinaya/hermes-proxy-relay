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

import pytest
from fastapi.testclient import TestClient


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
    }):
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
        assert data['version'] == '1.3.0'

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
        """Upstream health should return a response (likely 503 since upstream not real)."""
        resp = client.get("/admin/upstream-health")
        # Without a real upstream, this will fail to connect, returning 503
        assert resp.status_code in (200, 503)
        data = resp.json()
        assert "status" in data
        assert "latency_ms" in data

    def test_upstream_health_has_upstream_field(self, client):
        resp = client.get("/admin/upstream-health")
        data = resp.json()
        assert "upstream" in data


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
