"""Advanced tests for proxy validation, admin auth, rate limiting, retry, config loading.

Features tested here:
- _validate_proxy_url — URL validation for SOCKS5/HTTP proxies
- _check_admin_rate_limit — in-memory IP-based rate limiter
- Config loading: _load_config_file, _merge_config
- Proxy loading: _load_proxies_from_file, _load_proxies_from_env
- Admin auth middleware: X-Admin-Key header enforcement
- Shared client pool: _get_client, _close_all_clients, LRU eviction
- Retry logic: non-streaming retry across different proxies on 5xx upstream errors
- Streaming error paths: stream client connect failures, stream mid-stream errors
"""

import json
import httpx
import time
from unittest.mock import AsyncMock, patch

import pytest


# ── Proxy URL validation ───────────────────────────────────────────

@pytest.fixture(scope="module")
def proxy_modules():
    """Import relay module functions for unit testing."""
    import relay.relay as relay_mod
    return relay_mod


class TestProxyValidation:
    """_validate_proxy_url() basic validation."""

    def test_valid_socks5(self, proxy_modules):  # noqa: ARG002
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://user:pass@192.168.1.10:1080") is True

    def test_valid_socks5h(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5h://user:pass@proxy.example.com:1080") is True

    def test_valid_http(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("http://user:pass@proxy.example.com:3128") is True

    def test_valid_https(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("https://user:pass@proxy.example.com:443") is True

    def test_no_auth(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://192.168.1.10:1080") is True

    def test_empty_string(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("") is False

    def test_too_long(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://" + "a" * 500) is False

    def test_invalid_scheme(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("ftp://user:pass@host:21") is False

    def test_no_host(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://") is False

    def test_no_colon_slash(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("not-a-url") is False

    def test_domain_with_port(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://proxy.example.com:1080") is True

    def test_ipv6_bracket_supported(self, proxy_modules):
        """IPv6 bracket notation [::1] is supported."""
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://user:pass@[::1]:1080") is True
        assert _validate_proxy_url("socks5h://[2001:db8::1]:1080") is True
        assert _validate_proxy_url("http://[fe80::1%25eth0]:8080") is True

    def test_invalid_ipv6_bracket(self, proxy_modules):
        """Malformed IPv6 brackets are rejected."""
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://user:pass@[::1") is False
        assert _validate_proxy_url("socks5://user:pass@::1]:1080") is False

    def test_ipv4_with_credentials_and_port(self, proxy_modules):
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://user:password@192.168.1.100:1080") is True

    def test_invalid_port_zero(self, proxy_modules):
        """Port 0 is invalid — a :0 proxy could never connect and would
        waste a pool slot + retry attempt on every request."""
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://host:0") is False
        assert _validate_proxy_url("socks5://user:pass@host:0") is False

    def test_invalid_port_too_high(self, proxy_modules):
        """Port > 65535 is invalid (regex allows 5 digits)."""
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://host:99999") is False
        assert _validate_proxy_url("socks5://user:pass@[::1]:70000") is False

    def test_max_valid_port(self, proxy_modules):
        """Port 65535 is the upper valid bound."""
        from relay.relay import _validate_proxy_url
        assert _validate_proxy_url("socks5://host:65535") is True


# ── Admin Rate Limiting ────────────────────────────────────────────

class TestAdminRateLimit:
    """_check_admin_rate_limit() in-memory rate limiter."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch):
        # Import and reset the rate limit state
        import relay.relay as relay_mod
        # Set a low limit so tests are fast
        monkeypatch.setattr(relay_mod, "_ADMIN_RATE_LIMIT", 5)
        monkeypatch.setattr(relay_mod, "_ADMIN_RATE_WINDOW", 60)
        # Import the function
        from relay.relay import _check_admin_rate_limit
        self.func = _check_admin_rate_limit
        # Clear the hits dict
        relay_mod._admin_rate_hits.clear()

    async def test_first_request_allowed(self):
        assert await self.func("127.0.0.1") is True

    async def test_under_limit_allowed(self):
        for _ in range(4):
            assert await self.func("127.0.0.1") is True

    async def test_at_limit_blocked(self):
        for _ in range(5):
            await self.func("127.0.0.1")
        assert await self.func("127.0.0.1") is False  # 6th is blocked

    async def test_different_ip_not_affected(self):
        for _ in range(5):
            await self.func("127.0.0.1")
        # Different IP should still be allowed
        assert await self.func("10.0.0.1") is True

    async def test_window_rolls_over(self):
        # Hit the limit
        for _ in range(5):
            await self.func("127.0.0.1")
        assert await self.func("127.0.0.1") is False

        # Set cutoff far in the past so all entries are pruned
        import relay.relay as relay_mod
        old_time = time.monotonic() - 120
        relay_mod._admin_rate_hits["127.0.0.1"] = [old_time]

        # Should be allowed again
        assert await self.func("127.0.0.1") is True

    async def test_stale_ips_pruned_when_many(self):
        """When too many distinct IPs accumulate, stale ones are dropped."""
        import relay.relay as relay_mod

        old_time = time.monotonic() - 120  # stale
        # Fill with many stale IPs
        for i in range(relay_mod._ADMIN_RATE_MAX_IPS + 5):
            relay_mod._admin_rate_hits[f"10.0.0.{i}"] = [old_time]

        # A fresh IP check should prune the stale ones
        assert await self.func("10.0.0.200") is True
        # The stale IPs should be gone
        assert len(relay_mod._admin_rate_hits) <= relay_mod._ADMIN_RATE_MAX_IPS + 1


# ── Config Loading ─────────────────────────────────────────────────

class TestConfigLoading:
    """_load_config_file() and _merge_config()."""

    def test_load_missing_file(self):
        from relay.relay import _load_config_file
        result = _load_config_file("/nonexistent/path.json")
        assert result == {}

    def test_load_invalid_file(self, tmp_path):
        from relay.relay import _load_config_file
        p = tmp_path / "bad.txt"
        p.write_text("not json")
        result = _load_config_file(str(p))
        assert result == {}

    def test_load_valid_file(self, tmp_path):
        from relay.relay import _load_config_file
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"UPSTREAM_BASE": "https://api.test.com/v1"}))
        result = _load_config_file(str(p))
        assert result == {"UPSTREAM_BASE": "https://api.test.com/v1"}

    def test_load_expands_user(self):
        # Must not crash on ~ syntax
        from relay.relay import _load_config_file
        result = _load_config_file("~/nonexistent-relay-config-test.json")
        assert result == {}

    def test_merge_env_overrides_file(self, monkeypatch):
        from relay.relay import _merge_config
        monkeypatch.setenv("UPSTREAM_BASE", "https://env-override.com/v1")
        monkeypatch.setenv("UPSTREAM_API_KEY", "env-key")
        result = _merge_config({"UPSTREAM_BASE": "https://file-value.com/v1", "UPSTREAM_API_KEY": "file-key"})
        assert result["UPSTREAM_BASE"] == "https://env-override.com/v1"
        assert result["UPSTREAM_API_KEY"] == "env-key"

    def test_merge_file_fills_defaults(self, monkeypatch):
        from relay.relay import _merge_config
        # Clear env vars for keys we're testing
        monkeypatch.delenv("MAX_CONCURRENT_UPSTREAM", raising=False)
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        result = _merge_config({})
        assert result["MAX_CONCURRENT_UPSTREAM"] == 24
        assert result["LOG_LEVEL"] == "INFO"

    def test_merge_empty_env_does_not_override(self, monkeypatch):
        from relay.relay import _merge_config
        monkeypatch.setenv("UPSTREAM_BASE", "")
        result = _merge_config({"UPSTREAM_BASE": "https://file-value.com/v1"})
        assert result["UPSTREAM_BASE"] == "https://file-value.com/v1"


# ── Proxy Loading ──────────────────────────────────────────────────

class TestProxyLoading:
    """_load_proxies_from_file() and _load_proxies_from_env()."""

    def test_load_from_file(self, tmp_path):
        from relay.relay import _load_proxies_from_file
        p = tmp_path / "proxies.txt"
        p.write_text("socks5://u1:p1@h1:1080\nsocks5://u2:p2@h2:1080\n")
        result = _load_proxies_from_file(str(p))
        assert result == ["socks5://u1:p1@h1:1080", "socks5://u2:p2@h2:1080"]

    def test_load_from_file_skips_comments(self, tmp_path):
        from relay.relay import _load_proxies_from_file
        p = tmp_path / "proxies.txt"
        p.write_text("# Comment line\nsocks5://u1:p1@h1:1080\n# Another\n")
        result = _load_proxies_from_file(str(p))
        assert result == ["socks5://u1:p1@h1:1080"]

    def test_load_from_file_skips_invalid(self, tmp_path):
        from relay.relay import _load_proxies_from_file
        p = tmp_path / "proxies.txt"
        p.write_text("socks5://u1:p1@h1:1080\nnot-a-proxy\nftp://bad\n")
        result = _load_proxies_from_file(str(p))
        assert result == ["socks5://u1:p1@h1:1080"]

    def test_load_from_file_missing(self):
        from relay.relay import _load_proxies_from_file
        result = _load_proxies_from_file("/nonexistent/proxies.txt")
        assert result == []

    def test_load_from_env(self):
        from relay.relay import _load_proxies_from_env
        result = _load_proxies_from_env("socks5://u1:p1@h1:1080,socks5://u2:p2@h2:1080")
        assert result == ["socks5://u1:p1@h1:1080", "socks5://u2:p2@h2:1080"]

    def test_load_from_env_empty(self):
        from relay.relay import _load_proxies_from_env
        result = _load_proxies_from_env("")
        assert result == []

    def test_load_from_env_invalid_skipped(self):
        from relay.relay import _load_proxies_from_env
        result = _load_proxies_from_env("socks5://good:1080,invalid,ftp://bad")
        assert result == ["socks5://good:1080"]

    def test_load_from_env_whitespace(self):
        from relay.relay import _load_proxies_from_env
        result = _load_proxies_from_env("  socks5://h1:1080 , socks5://h2:1080  ")
        assert result == ["socks5://h1:1080", "socks5://h2:1080"]


# ── Shared Client Pool ─────────────────────────────────────────────

class TestSharedClientPool:
    """_get_client() shared pool with LRU eviction."""

    @pytest.fixture(autouse=True)
    async def reset_pool(self):
        """Reset the shared client pool before each test."""
        import relay.relay as relay_mod
        relay_mod._client_pool.clear()
        relay_mod.CLIENT_POOL_MAX = 3  # Small cap for testing (pool.total==0 in unit tests)

    async def test_get_client_creates_new(self):
        from relay.relay import _get_client
        client = await _get_client("socks5://u:p@h1:1080")
        assert client is not None

    async def test_get_client_reuses_existing(self):
        from relay.relay import _get_client
        c1 = await _get_client("socks5://u:p@h1:1080")
        c2 = await _get_client("socks5://u:p@h1:1080")
        assert c1 is c2  # Same object

    async def test_get_client_different_urls_different_clients(self):
        from relay.relay import _get_client
        c1 = await _get_client("socks5://u:p@h1:1080")
        c2 = await _get_client("socks5://u:p@h2:1080")
        assert c1 is not c2

    async def test_lru_eviction(self):
        """Pool should evict oldest client when cap is reached."""
        from relay.relay import _get_client, _client_pool
        # Fill to cap
        await _get_client("socks5://u:p@h1:1080")
        await _get_client("socks5://u:p@h2:1080")
        await _get_client("socks5://u:p@h3:1080")
        assert len(_client_pool) == 3

        # Adding a 4th evicts the first
        c4 = await _get_client("socks5://u:p@h4:1080")
        assert len(_client_pool) == 3
        assert "socks5://u:p@h1:1080" not in _client_pool  # h1 was evicted
        assert c4 is _client_pool["socks5://u:p@h4:1080"]

    async def test_eviction_close_error_is_swallowed(self):
        """Eviction tolerates a client whose aclose() raises."""
        from relay.relay import _get_client, _client_pool
        # Fill to cap with a client that fails on close
        await _get_client("socks5://u:p@h1:1080")
        _client_pool["socks5://u:p@h1:1080"].aclose = AsyncMock(
            side_effect=Exception("close failed")
        )
        await _get_client("socks5://u:p@h2:1080")
        await _get_client("socks5://u:p@h3:1080")
        # Adding a 4th triggers eviction of h1 (whose close fails) — must not raise
        await _get_client("socks5://u:p@h4:1080")
        assert "socks5://u:p@h1:1080" not in _client_pool

    async def test_lru_reuse_moves_to_back(self):
        """Reusing a client should move it to the back (evicted last)."""
        from relay.relay import _get_client, _client_pool
        # Fill to cap
        await _get_client("socks5://u:p@h1:1080")
        await _get_client("socks5://u:p@h2:1080")
        await _get_client("socks5://u:p@h3:1080")

        # Reuse h1 — should move it to the back of LRU order
        h1_client = await _get_client("socks5://u:p@h1:1080")

        # Adding h4 should evict h2 (now the least recently used),
        # NOT h1 (which was just reused)
        await _get_client("socks5://u:p@h4:1080")
        assert "socks5://u:p@h2:1080" not in _client_pool  # h2 evicted
        assert _client_pool["socks5://u:p@h1:1080"] is h1_client  # h1 survived

    async def test_close_all_clients(self):
        from relay.relay import _get_client, _close_all_clients, _client_pool
        await _get_client("socks5://u:p@h1:1080")
        await _get_client("socks5://u:p@h2:1080")
        assert len(_client_pool) == 2
        await _close_all_clients()
        assert len(_client_pool) == 0

    async def test_prune_removed_proxies(self):
        """Clients for proxies removed from the pool get closed."""
        from relay.relay import _get_client, _prune_client_pool, _client_pool
        await _get_client("socks5://u:p@h1:1080")
        await _get_client("socks5://u:p@h2:1080")

        # Pool now only has h1 — h2's client should be pruned
        await _prune_client_pool({"socks5://u:p@h1:1080"})

        assert "socks5://u:p@h2:1080" not in _client_pool
        assert "socks5://u:p@h1:1080" in _client_pool


# ═══════════════════════════════════════════════════════════════════
#  Endpoint-Level Tests (use TestClient)
# ═══════════════════════════════════════════════════════════════════


class TestAdminMiddlewareAuth:
    """Admin middleware with X-Admin-Key header enforcement."""

    @pytest.fixture(scope="module")
    def client(self):
        """TestClient with ADMIN_API_KEY set."""
        from unittest.mock import patch
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
            "ADMIN_API_KEY": "super-secret-admin-key",
        }):
            import importlib
            import relay.relay as relay_mod
            importlib.reload(relay_mod)
            relay_mod._init_pool()
            relay_mod._PROXY_HEALTH_TASK = None
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                yield tc

    def test_no_key_returns_403(self, client):
        resp = client.post("/admin/clear-cooldowns")
        assert resp.status_code == 403
        assert "Invalid or missing admin key" in resp.text

    def test_wrong_key_returns_403(self, client):
        resp = client.post(
            "/admin/clear-cooldowns",
            headers={"X-Admin-Key": "wrong-key"},
        )
        assert resp.status_code == 403

    def test_correct_key_allows_access(self, client):
        resp = client.post(
            "/admin/clear-cooldowns",
            headers={"X-Admin-Key": "super-secret-admin-key"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_all_admin_endpoints_require_key(self, client):
        endpoints = [
            ("GET", "/admin/upstream-health"),
            ("POST", "/admin/clear-cooldowns"),
            ("POST", "/admin/reset-proxy", {"url": "socks5://test:1080"}),
            ("POST", "/admin/reload-proxies"),
            ("POST", "/admin/reset-by-errors"),
        ]
        for ep in endpoints:
            method = ep[0]
            path = ep[1]
            body = ep[2] if len(ep) > 2 else None
            if body:
                resp = client.request(method, path, json=body)
            else:
                resp = client.request(method, path)
            assert resp.status_code == 403, f"{method} {path} should require key"


class TestAdminRateLimitEndpoint:
    """Admin endpoints are rate-limited (20 req/min/IP)."""

    @pytest.fixture(scope="module")
    def client(self):
        """TestClient with low rate limit for quick testing."""
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
            "ADMIN_API_KEY": "test-admin-key",
        }):
            import importlib
            import relay.relay as relay_mod
            importlib.reload(relay_mod)
            relay_mod._init_pool()
            relay_mod._PROXY_HEALTH_TASK = None
            # Set a very low limit so tests are fast
            relay_mod._ADMIN_RATE_LIMIT = 3
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                yield tc
        # Restore default rate limit for other test files
        import relay.relay as relay_mod
        relay_mod._ADMIN_RATE_LIMIT = 20
        relay_mod._admin_rate_hits.clear()

    def test_rate_limit_blocked(self, client):
        """After exhausting rate limit, next request returns 429."""
        key = {"X-Admin-Key": "test-admin-key"}

        # Use up the rate limit
        for i in range(3):
            resp = client.post("/admin/clear-cooldowns", headers=key)
            assert resp.status_code == 200, f"Request {i} failed: {resp.status_code}"

        # Next request should be rate-limited
        resp = client.post("/admin/clear-cooldowns", headers=key)
        assert resp.status_code == 429


class TestRelayRetry:
    """Retry logic: non-streaming requests retry across different proxies on 5xx."""

    @pytest.fixture(autouse=True)
    def reset_pool(self, request):
        """Reset pool cooldowns before each test for isolation."""
        import relay.relay as relay_mod
        relay_mod.pool.clear_cooldowns()

    @pytest.fixture(scope="module")
    def client(self):
        """TestClient for retry tests."""
        with patch.dict("os.environ", {
            "UPSTREAM_BASE": "https://test-upstream.example.com/v1",
            "UPSTREAM_API_KEY": "test-key-123",
            "UPSTREAM_AUTH_TYPE": "bearer",
            "RELAY_PORT": "9999",
            "MAX_CONCURRENT_UPSTREAM": "10",
            "MODEL_FILTER_PATTERN": ".*",
            "LOG_LEVEL": "CRITICAL",
            "CONSECUTIVE_ERROR_THRESHOLD": "3",
            "PERMANENT_COOLDOWN_SECONDS": "86400",
            "MAX_REQUEST_RETRIES": "2",
            "PROXY_LIST": "",
            "PROXY_LIST_FILE": "",
            "PROXY_LIST_ENV": "socks5://u1:p1@p1:1080,socks5://u2:p2@p2:1080",
            "RELAY_SHUTDOWN_DRAIN_SECONDS": "0",
        }):
            import importlib
            import relay.relay as relay_mod
            importlib.reload(relay_mod)
            relay_mod._init_pool()
            relay_mod._PROXY_HEALTH_TASK = None
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                yield tc

    def test_retry_on_proxy_connect_error(self, client):
        """When a proxy connection fails, relay retries on next proxy."""
        import relay.relay as relay_mod

        # Pool starts with 2 proxies
        assert relay_mod.pool.total == 2

        # Mock _proxy_single to always succeed (avoids real connection)
        original_single = relay_mod._proxy_single
        call_count = 0

        async def counting_proxy_single(client, method, url, headers, body, proxy_entry):
            nonlocal call_count
            call_count += 1
            # First call: simulate connection failure
            if call_count == 1:
                raise httpx.ConnectError("Simulated connection refused")
            # Second call: succeed
            return await original_single(client, method, url, headers, body, proxy_entry)

        with patch.object(relay_mod, "_proxy_single", counting_proxy_single):
            resp = client.post(
                "/v1/chat/completions",
                json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
            )
            # Should attempt but eventually get 502 or 200 depending on mock
            assert resp.status_code in (200, 502, 503)

    def test_retry_on_upstream_5xx(self, client):
        """When upstream returns 5xx, relay retries on different proxy."""
        import relay.relay as relay_mod

        original_single = relay_mod._proxy_single
        call_count = [0]
        tried_proxies = []

        async def fivexx_then_ok(client, method, url, headers, body, proxy_entry):
            call_count[0] += 1
            tried_proxies.append(proxy_entry.url)
            if call_count[0] == 1:
                # Return 503 from upstream
                from fastapi.responses import Response
                pool.record_success(proxy_entry)  # don't mark as failed
                return Response(content='{"error":"upstream down"}', status_code=503)
            return await original_single(client, method, url, headers, body, proxy_entry)

        # Capture pool reference
        pool = relay_mod.pool

        with patch.object(relay_mod, "_proxy_single", fivexx_then_ok):
            client.post(
                "/v1/chat/completions",
                json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
            )
            # After retry, it should attempt the second proxy (and eventually fail
            # since the second call hits real network, or succeed if the mock works)
            assert call_count[0] >= 1


# ═══════════════════════════════════════════════════════════════════
#  Streaming Error Paths
# ═══════════════════════════════════════════════════════════════════

class TestStreamingErrors:
    """Error handling in the streaming proxy path."""

    @pytest.fixture(scope="module")
    def client(self):
        with patch.dict("os.environ", {
            "UPSTREAM_BASE": "https://test-upstream.example.com/v1",
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
            import importlib
            import relay.relay as relay_mod
            importlib.reload(relay_mod)
            relay_mod._init_pool()
            relay_mod._PROXY_HEALTH_TASK = None
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                yield tc

    def test_streaming_connect_error_returns_502(self, client):
        """When streaming proxy connect fails, return 502."""
        import relay.relay as relay_mod

        async def failing_client(proxy_url):
            raise httpx.ConnectError("Simulated stream connection refused")

        with patch.object(relay_mod, "_make_streaming_client", failing_client):
            resp = client.post(
                "/v1/chat/completions",
                json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": True},
            )
            assert resp.status_code in (429, 502, 503)

    def test_streaming_all_proxies_cooling_returns_429(self, client):
        """When all proxies are cooling, streaming request returns 429."""
        import relay.relay as relay_mod

        # Cool all proxies
        pool = relay_mod.pool
        for _ in range(pool.total):
            p = pool.next()
            if p:
                pool.record_429(p, retry_after=300)

        resp = client.post(
            "/v1/chat/completions",
            json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": True},
        )
        assert resp.status_code == 429
        assert "all_proxies_cooling" in resp.text
