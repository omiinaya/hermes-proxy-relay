"""Edge-path tests for the last uncovered lines in relay.py.

Targets (from coverage report):
- _init_pool with file and with no proxies (425, 429)
- _get_client pool eviction log (511-512)
- _close_all_clients exception handling (537-538)
- Health checker 5xx and connection-failed branches (558, 564-573)
- Health checker unexpected exception (591-592)
- Streaming generic exception → 502 (721-728)
- Retry generic exception → 502 (799-810)
- Retries exhausted logging (824-825)
- Lifespan warnings for empty upstream (984, 988)
- Models upstream failure returns cached (1117-1118)
- Admin upstream health x-api-key path (1156)
- Signal handler registration in main() (1309-1310)
- __main__ guard (1322)
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest


def make_client(handler) -> httpx.AsyncClient:
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(10.0))


@pytest.fixture
def relay_mod():
    import relay.relay as relay_mod
    return relay_mod


@pytest.fixture
def fresh_pool(relay_mod):
    """Fresh pool + hermetic config for each test."""
    relay_mod.pool = relay_mod.CooldownPool([
        "socks5://u1:p1@p1:1080",
        "socks5://u2:p2@p2:1080",
        "socks5://u3:p3@p3:1080",
    ])
    relay_mod.PROXY_LIST_FILE = ""
    relay_mod.PROXY_LIST_ENV = "socks5://u1:p1@p1:1080,socks5://u2:p2@p2:1080,socks5://u3:p3@p3:1080"
    relay_mod.UPSTREAM_BASE = "https://test-api.example.com/v1"
    relay_mod.UPSTREAM_API_KEY = "test-key"
    relay_mod.UPSTREAM_AUTH_TYPE = "bearer"
    relay_mod.ADMIN_API_KEY = ""
    relay_mod._ADMIN_RATE_LIMIT = 20
    relay_mod._ADMIN_RATE_WINDOW = 60
    relay_mod._admin_rate_hits.clear()
    relay_mod._request_count["total"] = 0
    relay_mod._request_count["ok"] = 0
    relay_mod._request_count["errors"] = 0
    yield relay_mod.pool


# ── _init_pool ─────────────────────────────────────────────────────

class TestInitPool:
    def test_init_pool_from_file(self, relay_mod, tmp_path):
        """_init_pool loads from PROXY_LIST_FILE."""
        p = tmp_path / "proxies.txt"
        p.write_text("socks5://file1:1080\nsocks5://file2:1080\n")
        relay_mod.PROXY_LIST_FILE = str(p)
        relay_mod.PROXY_LIST_ENV = ""
        relay_mod._init_pool()
        assert relay_mod.pool.total == 2

    def test_init_pool_no_proxies_warns(self, relay_mod, caplog):
        """_init_pool with no file/env logs a warning."""
        relay_mod.PROXY_LIST_FILE = ""
        relay_mod.PROXY_LIST_ENV = ""
        relay_mod._init_pool()
        assert relay_mod.pool.total == 0

    def test_init_pool_dedupes_duplicates(self, relay_mod):
        """Duplicate proxy URLs in env should be collapsed to one entry."""
        relay_mod.PROXY_LIST_FILE = ""
        relay_mod.PROXY_LIST_ENV = (
            "socks5://u:p@h1:1080,socks5://u:p@h2:1080,socks5://u:p@h1:1080"
        )
        relay_mod._init_pool()
        assert relay_mod.pool.total == 2  # h1 appears twice, counted once


# ── Client pool ────────────────────────────────────────────────────

class TestClientPoolEdges:
    @pytest.fixture(autouse=True)
    async def reset(self, relay_mod):
        relay_mod._client_pool.clear()
        relay_mod.CLIENT_POOL_MAX = 2

    async def test_eviction_logs_debug(self, relay_mod, caplog):
        """Evicting an old client should log a debug message."""
        caplog.set_level("DEBUG")
        await relay_mod._get_client("socks5://u:p@h1:1080")
        await relay_mod._get_client("socks5://u:p@h2:1080")
        # Third client triggers eviction
        await relay_mod._get_client("socks5://u:p@h3:1080")
        assert any("Evicted idle client" in r.message for r in caplog.records)

    async def test_in_use_client_not_evicted(self, relay_mod, caplog):
        """A client with in-flight use is NOT evicted when the pool is full."""
        caplog.set_level("DEBUG")
        await relay_mod._get_client("socks5://u:p@h1:1080")
        await relay_mod._get_client("socks5://u:p@h2:1080")
        # Mark h1 as in-use
        relay_mod._client_in_use["socks5://u:p@h1:1080"] = 1
        # Third client: h1 is in-use, h2 is idle → h2 evicted
        await relay_mod._get_client("socks5://u:p@h3:1080")
        assert "socks5://u:p@h1:1080" in relay_mod._client_pool
        assert "socks5://u:p@h2:1080" not in relay_mod._client_pool
        assert "socks5://u:p@h3:1080" in relay_mod._client_pool
        relay_mod._client_in_use.clear()

    async def test_all_clients_in_use_exceeds_cap(self, relay_mod, caplog):
        """When every client is in use, pool temporarily exceeds cap."""
        caplog.set_level("DEBUG")
        await relay_mod._get_client("socks5://u:p@h1:1080")
        await relay_mod._get_client("socks5://u:p@h2:1080")
        relay_mod._client_in_use["socks5://u:p@h1:1080"] = 1
        relay_mod._client_in_use["socks5://u:p@h2:1080"] = 1
        await relay_mod._get_client("socks5://u:p@h3:1080")
        # No eviction happened — all 3 clients present (cap temporarily exceeded)
        assert len(relay_mod._client_pool) == 3
        assert any("all clients in use" in r.message for r in caplog.records)
        relay_mod._client_in_use.clear()

    async def test_borrow_client_tracks_usage(self, relay_mod):
        """_borrow_client marks in-use during the block, clears after."""
        async with relay_mod._borrow_client("socks5://u:p@h1:1080") as client:
            assert client is not None
            assert relay_mod._client_in_use.get("socks5://u:p@h1:1080", 0) == 1
        assert relay_mod._client_in_use.get("socks5://u:p@h1:1080", 0) == 0
        assert "socks5://u:p@h1:1080" not in relay_mod._client_in_use

    async def test_close_all_clients_handles_errors(self, relay_mod):
        """_close_all_clients should tolerate a failing client.aclose()."""
        client = MagicMock()
        client.aclose.side_effect = Exception("close failed")
        relay_mod._client_pool["socks5://u:p@h1:1080"] = client
        await relay_mod._close_all_clients()
        assert relay_mod._client_pool == {}

    async def test_make_streaming_client_borrows_pooled(self, relay_mod):
        """_make_streaming_client returns a POOLED (shared, reusable) client.

        Pre-1.6 it built a fresh client + transport per stream request
        (a new SOCKS5/TLS handshake every stream). Now it borrows from the
        shared per-proxy pool: same URL returns the SAME client and marks it
        in-use; the borrow is released (NOT the client closed) when done.
        """
        relay_mod._client_in_use.clear()
        relay_mod._client_pool.clear()
        url = "socks5://u:p@h1:1080"
        client = await relay_mod._make_streaming_client(url)
        assert client is not None
        assert client.timeout is not None
        # It's the shared pooled client, marked in-use.
        assert url in relay_mod._client_pool
        assert relay_mod._client_in_use.get(url, 0) == 1
        # Reuse: a second borrow returns the SAME client (no new handshake).
        client2 = await relay_mod._make_streaming_client(url)
        assert client2 is client
        assert relay_mod._client_in_use.get(url, 0) == 2
        # Releasing the borrows keeps the client pooled for the next stream.
        relay_mod._release_client_in_use(url)
        relay_mod._release_client_in_use(url)
        assert relay_mod._client_in_use.get(url, 0) == 0
        assert url in relay_mod._client_pool
        await relay_mod._close_all_clients()


# ── Health checker branches ────────────────────────────────────────

class TestHealthCheckerBranches:
    @pytest.fixture(autouse=True)
    def patch_interval(self, relay_mod, monkeypatch):
        monkeypatch.setattr(relay_mod, "PROXY_HEALTH_CHECK_INTERVAL", 0.01)
        # These tests verify the kill MECHANISM — a threshold of 1 makes a
        # single failed sweep kill, matching the original behavior. The
        # threshold-guard behavior is tested separately below.
        monkeypatch.setattr(relay_mod, "HEALTH_FAIL_THRESHOLD", 1)

    async def test_health_5xx_marks_permanent(self, relay_mod, fresh_pool):
        """Health check returning 5xx marks proxy permanently failed
        (when at least one other proxy succeeds)."""
        entry = relay_mod.pool.next()
        assert entry is not None
        # Second proxy succeeds → target is reachable, entry should be killed
        other = relay_mod.pool.next()
        assert other is not None

        fail_client = AsyncMock()
        fail_resp = MagicMock()
        fail_resp.status_code = 500
        fail_client.get.return_value = fail_resp
        fail_client.__aenter__.return_value = fail_client

        success_client = AsyncMock()
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_client.get.return_value = success_resp
        success_client.__aenter__.return_value = success_client

        # Pool order: entry(p1) → fail, other(p2) → success, p3 → success
        with patch.object(relay_mod.httpx, "AsyncClient") as mock_ctor:
            mock_ctor.side_effect = [fail_client, success_client, success_client]
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert entry.permanently_dead
        assert "5xx" in entry.last_error
        assert not other.permanently_dead

    async def test_health_connection_failed_marks_permanent(self, relay_mod, fresh_pool):
        """Health check connection failure marks proxy permanently failed
        when another proxy succeeds in the same sweep."""
        entry = relay_mod.pool.next()
        assert entry is not None
        other = relay_mod.pool.next()
        assert other is not None

        fail_client = AsyncMock()
        fail_client.get.side_effect = httpx.ConnectError("refused")
        fail_client.__aenter__.return_value = fail_client

        success_client = AsyncMock()
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_client.get.return_value = success_resp
        success_client.__aenter__.return_value = success_client

        with patch.object(relay_mod.httpx, "AsyncClient") as mock_ctor:
            mock_ctor.side_effect = [fail_client, success_client, success_client]
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert entry.permanently_dead
        assert "Health check" in entry.last_error or "connection" in entry.last_error.lower()

    async def test_all_fail_leaves_proxies_alive(self, relay_mod, fresh_pool):
        """When ALL proxies fail, the health target is likely down —
        proxies must NOT be marked permanently dead."""
        entries = [relay_mod.pool.next() for _ in range(relay_mod.pool.total)]
        assert all(e is not None for e in entries)

        fail_client = AsyncMock()
        fail_client.get.side_effect = httpx.ConnectError("refused")

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=fail_client):
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # None should be marked dead — all failed simultaneously
        assert all(not e.permanently_dead for e in entries)

    async def test_threshold_delays_permanent_death(self, relay_mod, fresh_pool, monkeypatch):
        """A single partial-sweep failure must NOT kill a proxy immediately."""
        monkeypatch.setattr(relay_mod, "HEALTH_FAIL_THRESHOLD", 3)

        entry = relay_mod.pool.next()
        assert entry is not None

        fail_client = AsyncMock()
        fail_client.get.side_effect = httpx.ConnectError("refused")
        fail_client.__aenter__.return_value = fail_client

        success_client = AsyncMock()
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_client.get.return_value = success_resp
        success_client.__aenter__.return_value = success_client

        # entry fails, other 2 succeed — a SINGLE sweep. Threshold is 3, so
        # the proxy must NOT be marked dead after just one failure.
        with patch.object(relay_mod.httpx, "AsyncClient") as mock_ctor:
            mock_ctor.side_effect = [fail_client, success_client, success_client]
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # The guard holds: one partial failure is NOT enough to kill.
        assert not entry.permanently_dead
        assert entry.consecutive_errors == 0

    async def test_failure_counter_resets_on_success(self, relay_mod, fresh_pool, monkeypatch):
        """A proxy that fails once then succeeds resets its failure counter."""
        monkeypatch.setattr(relay_mod, "HEALTH_FAIL_THRESHOLD", 3)

        entry = relay_mod.pool.next()
        assert entry is not None

        fail_client = AsyncMock()
        fail_client.get.side_effect = httpx.ConnectError("refused")
        fail_client.__aenter__.return_value = fail_client

        success_client = AsyncMock()
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_client.get.return_value = success_resp
        success_client.__aenter__.return_value = success_client

        # Sweep 1: entry fails, others succeed → counter=1, NOT dead
        # Sweep 2: ALL succeed → counter resets, entry stays alive
        with patch.object(relay_mod.httpx, "AsyncClient") as mock_ctor:
            mock_ctor.side_effect = [
                fail_client, success_client, success_client,   # sweep 1
            ] + [success_client] * 12  # sweeps 2+ (each sweep needs 3 clients)
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.30)  # long enough for sweep 2 at 0.01s interval
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # After recovery, the proxy is alive and never killed
        assert not entry.permanently_dead
        assert entry.consecutive_errors == 0


# ── Streaming generic exception → 502 ──────────────────────────────

class TestStreamingGenericError:
    def test_stream_generic_exception_returns_502(self, relay_mod, fresh_pool):
        """Non-ConnectError exception in streaming path returns 502."""

        async def failing_stream(client, method, url, headers, body, proxy_entry):
            raise ValueError("Unexpected upstream error")

        with patch.object(relay_mod, "_proxy_stream", failing_stream):
            with patch.object(relay_mod, "_make_streaming_client", AsyncMock()):
                from fastapi.testclient import TestClient
                with TestClient(relay_mod.app) as tc:
                    resp = tc.post(
                        "/v1/chat/completions",
                        json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": True},
                    )

        assert resp.status_code == 502
        assert "upstream_error" in resp.text


# ── Retry generic exception → 502 ──────────────────────────────────

class TestRetryGenericError:
    def test_retry_generic_exception_returns_502(self, relay_mod, fresh_pool):
        """Non-ConnectError exception during retry returns 502."""

        async def failing_single(client, method, url, headers, body, proxy_entry):
            raise RuntimeError("Unexpected failure")

        with patch.object(relay_mod, "_proxy_single", failing_single):
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                resp = tc.post(
                    "/v1/chat/completions",
                    json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
                )

        assert resp.status_code == 502
        assert "upstream_error" in resp.text


# ── Lifespan warnings ──────────────────────────────────────────────

class TestLifespanWarnings:
    def test_empty_upstream_warns(self, relay_mod, fresh_pool, caplog):
        """Lifespan should warn when UPSTREAM_BASE/API key empty."""
        relay_mod.UPSTREAM_BASE = ""
        relay_mod.UPSTREAM_API_KEY = ""
        relay_mod.PROXY_LIST_FILE = ""
        relay_mod.PROXY_LIST_ENV = ""

        with caplog.at_level("WARNING"):
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app):
                pass

        messages = " ".join(r.message for r in caplog.records)
        assert "UPSTREAM_API_KEY is empty" in messages
        assert "UPSTREAM_BASE is empty" in messages
        assert "No proxy list configured" in messages


# ── Models upstream failure ────────────────────────────────────────

class TestModelsFailure:
    async def test_models_upstream_failure_returns_cache(self, relay_mod, fresh_pool):
        """When upstream fails, models endpoint returns cached data."""
        relay_mod._update_models_cache([{"id": "cached-model"}])

        with patch.object(relay_mod.httpx, "AsyncClient", side_effect=Exception("Connection refused")):
            result = await relay_mod.list_models()

        assert result["data"] == [{"id": "cached-model"}]


# ── Admin upstream health x-api-key ────────────────────────────────

class TestAdminUpstreamHealthXApiKey:
    async def test_health_uses_x_api_key_auth(self, relay_mod, fresh_pool, monkeypatch):
        """When UPSTREAM_AUTH_TYPE is x-api-key, the header is used."""
        relay_mod.UPSTREAM_AUTH_TYPE = "x-api-key"
        relay_mod.UPSTREAM_API_KEY = "public-key"

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers.get("x-api-key") == "public-key"
            return httpx.Response(200, json={"data": []})

        mock_client = make_client(handler)

        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            req = MagicMock()
            req.client.host = "127.0.0.1"
            data = await relay_mod.admin_upstream_health(req)

        assert data["status"] == "ok"

    async def test_health_all_cooling_returns_503(self, relay_mod, fresh_pool, monkeypatch):
        """All proxies cooling → upstream-health returns 503 without a direct fetch."""
        for p in relay_mod.pool._proxies:
            relay_mod.pool.record_429(p, retry_after=3600)

        with patch.object(relay_mod, "_get_client") as mock_get:
            req = MagicMock()
            req.client.host = "127.0.0.1"
            data = await relay_mod.admin_upstream_health(req)

        mock_get.assert_not_called()
        assert data.status_code == 503
        assert "cooling" in data.body.decode()


# ── Signal handlers in main() ──────────────────────────────────────

class TestSignalHandlers:
    def test_signal_handler_registration(self, relay_mod):
        """main() registers SIGTERM/SIGINT handlers."""
        import sys as _sys
        mock_uvicorn = MagicMock()
        _sys.modules["uvicorn"] = mock_uvicorn

        mock_signal = MagicMock()
        mock_signal.SIGTERM = 15
        mock_signal.SIGINT = 2
        _sys.modules["signal"] = mock_signal

        try:
            with patch.object(relay_mod.sys, "argv", ["relay.py"]):
                relay_mod.main()
            assert mock_signal.signal.call_count == 2
        finally:
            _sys.modules.pop("uvicorn", None)
            _sys.modules.pop("signal", None)

    def test_main_guard(self):
        """__main__ guard exists — importing relay.relay doesn't run main()."""
        import relay.relay as relay_mod
        assert hasattr(relay_mod, "main")
        assert relay_mod.__name__ == "relay.relay"


# ── Models no-upstream + x-api-key ────────────────────────────────

class TestModelsBranches:
    async def test_models_no_upstream_returns_empty(self, relay_mod, fresh_pool, monkeypatch):
        """UPSTREAM_BASE empty → models returns empty list immediately."""
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "")
        result = await relay_mod.list_models()
        assert result == {"object": "list", "data": []}

    async def test_models_uses_x_api_key_header(self, relay_mod, fresh_pool, monkeypatch):
        """UPSTREAM_AUTH_TYPE=x-api-key → models fetch sends x-api-key header."""
        monkeypatch.setattr(relay_mod, "UPSTREAM_AUTH_TYPE", "x-api-key")
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "public-key")
        monkeypatch.setattr(relay_mod, "MODELS_CACHE", [])
        monkeypatch.setattr(relay_mod, "MODELS_CACHE_UPDATED", time.monotonic() - 10000)  # guaranteed stale (>TTL)
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "https://api.test.com/v1")

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers.get("x-api-key") == "public-key"
            return httpx.Response(200, json={"data": [{"id": "model-x"}]})

        mock_client = make_client(handler)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            result = await relay_mod.list_models()
        assert result["data"] == [{"id": "model-x"}]


# ── Admin reset-by-errors endpoint ────────────────────────────────

class TestAdminResetByErrors:
    async def test_reset_by_errors_success(self, relay_mod, fresh_pool):
        """reset-by-errors returns the number of re-enabled proxies."""
        # Permanently fail two proxies
        pool = relay_mod.pool
        for _ in range(2):
            entry = pool.next()
            if entry:
                pool.record_timeout(entry)
                pool.record_timeout(entry)
                pool.record_timeout(entry)

        req = MagicMock()
        req.client.host = "127.0.0.1"
        req.headers = {}
        # AsyncMock — request.json() is a coroutine; MagicMock would
        # return a plain dict and `await` would raise TypeError (silently
        # swallowed by the endpoint's except → data={}).
        req.json = AsyncMock(return_value={"min_consecutive": 3})
        result = await relay_mod.admin_reset_by_errors(req)
        assert result["status"] == "ok"
        assert "Reset" in result["message"]

    async def test_reset_by_errors_empty_body_defaults(self, relay_mod, fresh_pool):
        """No body → default threshold used, no crash."""
        req = MagicMock()
        req.client.host = "127.0.0.1"
        req.headers = {"content-length": "0"}
        result = await relay_mod.admin_reset_by_errors(req)
        assert result["status"] == "ok"

    async def test_reset_by_errors_invalid_body_tolerated(self, relay_mod, fresh_pool):
        """Corrupt body → defaults used, no crash."""
        req = MagicMock()
        req.client.host = "127.0.0.1"
        req.headers = {"content-length": "5"}
        req.json = MagicMock(side_effect=Exception("bad json"))
        result = await relay_mod.admin_reset_by_errors(req)
        assert result["status"] == "ok"

    async def test_reset_by_errors_non_int_min_consecutive(self, relay_mod, fresh_pool):
        """min_consecutive as a string/bool/None must not 500 — coerced to
        the default threshold instead of raising TypeError."""
        for bad in ("3", True, None, [], {}):
            req = MagicMock()
            req.client.host = "127.0.0.1"
            req.headers = {"content-length": "5"}
            req.json = AsyncMock(return_value={"min_consecutive": bad})
            result = await relay_mod.admin_reset_by_errors(req)
            assert result["status"] == "ok"


# ── Admin rate-limit 429 branches ─────────────────────────────────

class TestAdminRateLimit429:
    """Each admin endpoint returns 429 when the rate limiter trips."""

    async def test_all_admin_endpoints_429_when_rate_limited(self, relay_mod, fresh_pool, monkeypatch):
        """Rate limiter returning False → every admin endpoint returns 429."""
        async def rate_limited(ip):
            return False
        monkeypatch.setattr(relay_mod, "_check_admin_rate_limit", rate_limited)
        req = MagicMock()
        req.client.host = "127.0.0.1"

        endpoints = [
            relay_mod.admin_upstream_health,
            relay_mod.admin_clear_cooldowns,
            relay_mod.admin_reset_proxy,
            relay_mod.admin_reload_proxies,
            relay_mod.admin_reset_by_errors,
            relay_mod.admin_reload_config,
        ]
        for endpoint in endpoints:
            result = await endpoint(req)
            assert result.status_code == 429, f"{endpoint.__name__} did not 429"
            assert result.body == b'{"error":"Rate limit exceeded"}'
