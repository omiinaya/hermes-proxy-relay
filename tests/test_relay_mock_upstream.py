"""End-to-end tests for _proxy_single and _proxy_stream using httpx.MockTransport.

These tests exercise the actual request/response forwarding logic with a
mocked upstream transport — no real network calls.

Features tested:
- _proxy_single: success (2xx), 429 cooldown, 4xx error, header stripping
- _proxy_stream: success stream, 429, 4xx error, header forwarding
- /v1/models cache refresh with mocked upstream
- Streaming mid-stream error handling
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest


def make_client(handler) -> httpx.AsyncClient:
    """Build an AsyncClient backed by a MockTransport handler."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(10.0))


# ═══════════════════════════════════════════════════════════════════
#  _proxy_single
# ═══════════════════════════════════════════════════════════════════


class TestProxySingle:
    """Direct unit tests of _proxy_single forwarding logic."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        # Fresh pool with known proxies — avoids cross-test counter pollution
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        return relay_mod

    @pytest.fixture
    def entry(self, relay):
        """Get a fresh proxy entry from the pool."""
        proxy = relay.pool.next()
        assert proxy is not None
        return proxy

    async def test_success_returns_200(self, relay, entry):
        """Successful upstream response should relay status 200 and body."""
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "POST"
            assert request.url.path == "/v1/chat/completions"
            return httpx.Response(
                200,
                json={"id": "cmpl-1", "choices": [{"message": {"content": "hello"}}]},
                headers={"x-request-id": "req-123", "openai-version": "2024-01-01"},
            )

        client = make_client(handler)
        resp = await relay._proxy_single(
            client,
            "POST",
            "https://upstream.example.com/v1/chat/completions",
            {"Content-Type": "application/json"},
            b'{"model": "gpt-4"}',
            entry,
        )
        await client.aclose()

        assert resp.status_code == 200
        assert "cmpl-1" in resp.body.decode()
        # Upstream headers should be forwarded
        assert resp.headers.get("x-request-id") == "req-123"
        assert resp.headers.get("openai-version") == "2024-01-01"

    async def test_success_records_success(self, relay, entry):
        """2xx response should call pool.record_success."""
        client = make_client(lambda req: httpx.Response(200, json={"ok": True}))
        resp = await relay._proxy_single(
            client, "GET", "https://upstream.example.com/v1/models",
            {}, None, entry,
        )
        await client.aclose()
        assert resp.status_code == 200
        assert entry.total_ok == 1
        assert entry.consecutive_errors == 0

    async def test_429_cools_proxy(self, relay, entry):
        """429 upstream response should cool the proxy."""
        client = make_client(
            lambda req: httpx.Response(429, json={"error": "rate limited"}, headers={"retry-after": "120"})
        )
        resp = await relay._proxy_single(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b"{}", entry,
        )
        await client.aclose()

        assert resp.status_code == 429
        assert entry.total_429 == 1
        # Proxy should be cooling now
        assert entry.cooldown_until > time.monotonic()

    async def test_4xx_client_error_does_not_cool(self, relay, entry):
        """400 (client error) is relayed without cooling the proxy."""
        client = make_client(lambda req: httpx.Response(400, json={"error": "bad request"}))
        resp = await relay._proxy_single(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b"{}", entry,
        )
        await client.aclose()

        assert resp.status_code == 400
        assert entry.consecutive_errors == 0  # NOT cooled
        assert entry.total_ok == 0

    async def test_407_proxy_auth_cools(self, relay, entry):
        """407 (proxy auth required) IS proxy-related — cools the proxy."""
        client = make_client(lambda req: httpx.Response(407, json={"error": "proxy auth"}))
        resp = await relay._proxy_single(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b"{}", entry,
        )
        await client.aclose()

        assert resp.status_code == 407
        assert entry.consecutive_errors == 1  # cooled
        assert entry.cooldown_until > time.monotonic()

    async def test_408_timeout_cools(self, relay, entry):
        """408 (request timeout) is proxy-related — cools the proxy."""
        client = make_client(lambda req: httpx.Response(408, json={}))
        resp = await relay._proxy_single(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b"{}", entry,
        )
        await client.aclose()

        assert resp.status_code == 408
        assert entry.consecutive_errors == 1

    async def test_response_headers_stripped(self, relay, entry):
        """Content-Length, Content-Encoding, Transfer-Encoding must be stripped."""
        import gzip

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=gzip.compress(b"hello world"),
                headers={
                    "content-encoding": "gzip",
                    "x-custom": "keep-me",
                },
            )

        client = make_client(handler)
        resp = await relay._proxy_single(
            client, "GET", "https://upstream.example.com/v1/models", {}, None, entry,
        )
        await client.aclose()

        lowered = {k.lower() for k in resp.headers.keys()}
        assert "content-encoding" not in lowered
        assert resp.headers.get("x-custom") == "keep-me"

    async def test_latency_recorded_on_success(self, relay, entry):
        client = make_client(lambda req: httpx.Response(200, json={"ok": True}))
        resp = await relay._proxy_single(
            client, "GET", "https://upstream.example.com/v1/models", {}, None, entry,
        )
        await client.aclose()
        assert resp.status_code == 200
        assert entry.latency_samples == 1
        assert entry.last_latency_ms >= 0


# ═══════════════════════════════════════════════════════════════════
#  _proxy_stream
# ═══════════════════════════════════════════════════════════════════


class TestProxyStream:
    """Direct unit tests of _proxy_stream streaming logic."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        # Fresh pool — avoids cross-test counter pollution
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        return relay_mod

    @pytest.fixture
    def entry(self, relay):
        proxy = relay.pool.next()
        assert proxy is not None
        return proxy

    async def test_success_stream(self, relay, entry):
        """Successful streaming response should relay chunks."""

        async def stream_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b'data: {"choices":[{"delta":{"content":"hel"}}]}\n\n'
                        b'data: {"choices":[{"delta":{"content":"lo"}}]}\n\n'
                        b"data: [DONE]\n\n",
                headers={"content-type": "text/event-stream", "x-request-id": "stream-1"},
            )

        client = make_client(stream_handler)
        resp = await relay._proxy_stream(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        await client.aclose()

        assert resp.status_code == 200
        assert resp.headers.get("x-request-id") == "stream-1"
        assert entry.total_ok == 1

    async def test_stream_429_cools(self, relay, entry):
        client = make_client(
            lambda req: httpx.Response(429, json={"error": "rate limited"}, headers={"retry-after": "60"})
        )
        resp = await relay._proxy_stream(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        await client.aclose()

        assert resp.status_code == 429
        assert entry.total_429 == 1

    async def test_stream_4xx_returns_error(self, relay, entry):
        client = make_client(lambda req: httpx.Response(404, json={"error": "not found"}))
        resp = await relay._proxy_stream(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        await client.aclose()

        assert resp.status_code == 404
        # Client error — proxy NOT cooled
        assert entry.consecutive_errors == 0

    async def test_stream_407_proxy_auth_cools(self, relay, entry):
        """407 in the streaming path IS proxy-related — cools the proxy."""
        client = make_client(lambda req: httpx.Response(407, json={"error": "proxy auth"}))
        resp = await relay._proxy_stream(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        await client.aclose()

        assert resp.status_code == 407
        assert entry.consecutive_errors == 1
        assert entry.cooldown_until > time.monotonic()

    async def test_stream_header_stripping(self, relay, entry):
        client = make_client(
            lambda req: httpx.Response(
                200,
                content=b"data: hello\n\n",
                headers={
                    "content-type": "text/event-stream",
                },
            )
        )
        resp = await relay._proxy_stream(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        await client.aclose()

        lowered = {k.lower() for k in resp.headers.keys()}
        assert "content-length" not in lowered
        assert "content-encoding" not in lowered
        assert resp.headers.get("content-type") is not None  # set via media_type

    async def test_stream_midstream_error_yields_error_chunk(self, relay, entry):
        """If the upstream stream raises mid-body, the generator yields an
        error chunk and records a timeout."""

        # An earlier TestClient teardown may have left the global shutdown
        # event set — clear it so the generator reaches the stream-error path.
        relay._stream_shutdown_event.clear()

        # Build a fake client whose send() returns a fake response whose
        # aiter_bytes() raises after yielding one chunk
        class FailingStream:
            def __init__(self):
                self._called = False

            async def aiter_bytes(self):
                if not self._called:
                    self._called = True
                    yield b'data: {"choices":[{"delta":{"content":"hel"}}]}\n\n'
                raise ConnectionResetError("connection reset mid-stream")

            async def aread(self):
                return b""

            async def aclose(self):
                pass

            @property
            def status_code(self):
                return 200

            @property
            def headers(self):
                return {"content-type": "text/event-stream"}

        fake_resp = FailingStream()
        fake_client = MagicMock()
        fake_client.send = AsyncMock(return_value=fake_resp)
        fake_client.aclose = AsyncMock()
        fake_client.build_request = MagicMock(return_value=MagicMock())

        streaming_resp = await relay._proxy_stream(
            fake_client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        # Consume the generator to trigger the mid-stream exception
        body = b"".join([chunk async for chunk in streaming_resp.body_iterator])

        assert b"stream_error" in body
        assert b"connection reset" in body.lower()
        assert entry.consecutive_errors >= 1
        assert relay._request_count["errors"] >= 1

    async def test_stream_shutdown_event_yields_shutdown_error(self, relay, entry):
        """When the relay is shutting down, in-flight streams yield a
        shutdown_error chunk and stop."""

        relay._stream_shutdown_event.set()
        try:
            class ShutdownStream:
                async def aiter_bytes(self):
                    yield b'data: {"choices":[{"delta":{"content":"hel"}}]}\n\n'
                    yield b'data: {"choices":[{"delta":{"content":"lo"}}]}\n\n'

                async def aread(self):
                    return b""

                async def aclose(self):
                    pass

                @property
                def status_code(self):
                    return 200

                @property
                def headers(self):
                    return {"content-type": "text/event-stream"}

            fake_resp = ShutdownStream()
            fake_client = MagicMock()
            fake_client.send = AsyncMock(return_value=fake_resp)
            fake_client.aclose = AsyncMock()
            fake_client.build_request = MagicMock(return_value=MagicMock())

            streaming_resp = await relay._proxy_stream(
                fake_client, "POST", "https://upstream.example.com/v1/chat/completions",
                {}, b'{"stream": true}', entry,
            )
            body = b"".join([chunk async for chunk in streaming_resp.body_iterator])

            assert b"shutdown_error" in body
            # The stream stops after the shutdown error — no more chunks
            assert b"lo" not in body
        finally:
            relay._stream_shutdown_event.clear()


# ═══════════════════════════════════════════════════════════════════
#  /v1/models endpoint with mocked upstream
# ═══════════════════════════════════════════════════════════════════


class TestModelsEndpointMocked:
    """/v1/models with a mocked upstream response."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        relay_mod.MODELS_CACHE = []
        relay_mod.MODELS_CACHE_UPDATED = 0.0
        return relay_mod

    async def test_models_refresh_populates_cache(self, relay, monkeypatch):
        """When cache is stale, /v1/models fetches from upstream and caches."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"data": [
                    {"id": "gpt-4-free", "object": "model"},
                    {"id": "gpt-4o", "object": "model"},
                    {"id": "claude-sonnet-free", "object": "model"},
                ]},
            )

        mock_client = make_client(handler)

        with patch.object(relay.httpx, "AsyncClient", return_value=mock_client):
            # Call the models endpoint logic directly
            result = await relay.list_models()

        assert result["object"] == "list"
        # MODEL_FILTER_PATTERN is ".*" so all models pass
        assert len(result["data"]) == 3
        assert relay.MODELS_CACHE == result["data"]

    async def test_models_returns_cache_when_fresh(self, relay, monkeypatch):
        """When cache is fresh, /v1/models returns from cache without upstream call."""
        relay._update_models_cache([{"id": "cached-model", "object": "model"}])

        with patch.object(relay.httpx, "AsyncClient") as mock_ctor:
            result = await relay.list_models()

        # No upstream call — served from cache
        mock_ctor.assert_not_called()
        assert result["data"] == [{"id": "cached-model", "object": "model"}]

    async def test_models_filter_applies(self, relay, monkeypatch):
        """MODEL_FILTER_PATTERN should filter models from upstream."""
        import re as _re
        original = relay._model_filter_re
        relay._model_filter_re = _re.compile(r"-free$")

        try:
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    json={"data": [
                        {"id": "gpt-4-free"},
                        {"id": "gpt-4o"},
                    ]},
                )

            mock_client = make_client(handler)
            with patch.object(relay.httpx, "AsyncClient", return_value=mock_client):
                result = await relay.list_models()
        finally:
            relay._model_filter_re = original

        ids = [m["id"] for m in result["data"]]
        assert "gpt-4-free" in ids
        assert "gpt-4o" not in ids

    async def test_models_upstream_failure_returns_cache(self, relay, monkeypatch):
        """When upstream fails, return existing cache (or empty)."""
        with patch.object(relay.httpx, "AsyncClient", side_effect=Exception("Connection refused")):
            result = await relay.list_models()

        assert result["object"] == "list"
        assert isinstance(result["data"], list)


# ═══════════════════════════════════════════════════════════════════
#  Admin upstream health
# ═══════════════════════════════════════════════════════════════════


class TestAdminUpstreamHealthMocked:
    """/admin/upstream-health with mocked upstream."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        return relay_mod

    async def test_health_ok(self, relay, monkeypatch):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"data": [{"id": "model-a"}]})

        mock_client = make_client(handler)

        with patch.object(relay.httpx, "AsyncClient", return_value=mock_client):
            # Build a fake Request object
            req = MagicMock()
            req.client.host = "127.0.0.1"
            data = await relay.admin_upstream_health(req)

        assert data["status"] == "ok"
        assert data["models_count"] == 1

    async def test_health_5xx_degraded(self, relay, monkeypatch):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"error": "upstream down"})

        mock_client = make_client(handler)

        with patch.object(relay.httpx, "AsyncClient", return_value=mock_client):
            req = MagicMock()
            req.client.host = "127.0.0.1"
            data = await relay.admin_upstream_health(req)

        assert data["status"] == "degraded"
        assert data["upstream_status"] == 503


# ═══════════════════════════════════════════════════════════════════
#  Remaining branch coverage
# ═══════════════════════════════════════════════════════════════════


class TestProxyRequestEdgeBranches:
    """Remaining uncovered branches in _proxy_request and related paths."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        return relay_mod

    async def test_all_cooling_after_retry_returns_429(self, relay, monkeypatch):
        """Loop exits with no proxy and no last_error → 429 fallthrough."""
        # MAX_REQUEST_RETRIES=0 → loop never runs, last_error stays None
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 0)
        monkeypatch.setattr(relay, "MAX_CONCURRENT_UPSTREAM", 10)

        # Ensure next() returns None (all cooling)
        p1 = relay.pool._proxies[0]
        p2 = relay.pool._proxies[1]
        relay.pool.record_429(p1, retry_after=3600)
        relay.pool.record_429(p2, retry_after=3600)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b"{}",
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 429
        assert b"all_proxies_cooling" in resp.body

    async def test_chat_completions_handler_forwards_query(self, relay, monkeypatch):
        """chat_completions() passes the query string through to _proxy_request."""
        calls = {}

        async def fake_proxy_request(method, path, body, headers, query):
            calls.update(method=method, path=path, query=query)
            return {"ok": True}

        monkeypatch.setattr(relay, "_proxy_request", fake_proxy_request)
        req = MagicMock()
        req.body = AsyncMock(return_value=b'{"model":"gpt-4"}')
        req.headers = {"content-type": "application/json"}
        req.url.query = "model=gpt-4&stream=true"

        result = await relay.chat_completions(req)
        assert result == {"ok": True}
        assert calls == {"method": "POST", "path": "/chat/completions", "query": "model=gpt-4&stream=true"}

    async def test_reset_proxy_success(self, relay):
        """admin_reset_proxy with a valid URL returns ok."""
        req = MagicMock()
        req.client.host = "127.0.0.1"
        req.json = AsyncMock(return_value={"url": relay.pool._proxies[0].url})
        result = await relay.admin_reset_proxy(req)
        assert result["status"] == "ok"
        assert "Proxy reset" in result["message"]

    async def test_reset_proxy_missing_url_400(self, relay):
        """admin_reset_proxy without a url field returns 400."""
        req = MagicMock()
        req.client.host = "127.0.0.1"
        req.json = AsyncMock(return_value={})
        result = await relay.admin_reset_proxy(req)
        assert result.status_code == 400
        assert b"url" in result.body

    async def test_reset_proxy_not_found_404(self, relay):
        """admin_reset_proxy with an unknown URL returns 404."""
        req = MagicMock()
        req.client.host = "127.0.0.1"
        req.json = AsyncMock(return_value={"url": "socks5://nope:1080"})
        result = await relay.admin_reset_proxy(req)
        assert result.status_code == 404
        assert b"not found" in result.body

    async def test_stream_connect_error_closes_client(self, relay, monkeypatch):
        """Stream path: ConnectError after client creation closes the client.

        Covers the `if streaming_client is not None: await streaming_client.aclose()`
        branch — _make_streaming_client succeeds but _proxy_stream raises a
        connect error mid-flight.
        """
        mock_client = AsyncMock()
        mock_client.aclose = AsyncMock()

        monkeypatch.setattr(relay, "_make_streaming_client", AsyncMock(return_value=mock_client))
        monkeypatch.setattr(relay, "_proxy_stream", AsyncMock(
            side_effect=httpx.ConnectError("connection reset by proxy")
        ))

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 502
        assert b"proxy_connect_failed" in resp.body
        mock_client.aclose.assert_awaited_once()


class TestShutdownBranches:
    """Shutdown drain + health task cancellation paths."""

    @pytest.fixture(autouse=True)
    def relay(self, monkeypatch):
        import relay.relay as relay_mod
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        return relay_mod

    async def test_shutdown_with_positive_drain_and_health_task(self, relay, monkeypatch):
        """lifespan shutdown sleeps the drain window and cancels the health task."""
        import asyncio as _asyncio
        # Patch the startup side-effects so no real tasks are spawned
        monkeypatch.setattr(relay, "_init_pool", lambda: None)
        monkeypatch.setattr(relay, "_auto_star", AsyncMock())

        async def never_ends():
            await _asyncio.Event().wait()  # block forever

        monkeypatch.setattr(relay, "_proxy_health_check", never_ends)
        monkeypatch.setenv("RELAY_SHUTDOWN_DRAIN_SECONDS", "1")

        slept = []
        real_sleep = _asyncio.sleep

        async def short_sleep(delay):
            slept.append(delay)
            await real_sleep(0.01)  # don't actually wait the full drain

        monkeypatch.setattr(relay.asyncio, "sleep", short_sleep)
        monkeypatch.setattr(relay, "_close_all_clients", AsyncMock())

        async with relay.lifespan(relay.app):
            pass
        # Health task created by startup and cancelled by shutdown
        assert relay._PROXY_HEALTH_TASK is not None
        assert relay._PROXY_HEALTH_TASK.cancelled()

    async def test_shutdown_zero_drain_skips_sleep(self, relay, monkeypatch):
        """RELAY_SHUTDOWN_DRAIN_SECONDS=0 → no sleep, no crash."""
        monkeypatch.setattr(relay, "_init_pool", lambda: None)
        monkeypatch.setattr(relay, "_auto_star", AsyncMock())
        monkeypatch.setattr(relay, "_proxy_health_check", AsyncMock())
        monkeypatch.setenv("RELAY_SHUTDOWN_DRAIN_SECONDS", "0")
        monkeypatch.setattr(relay, "_close_all_clients", AsyncMock())

        async with relay.lifespan(relay.app):
            pass  # must not raise


class TestSignalHandlerException:
    """main() tolerates signal module failures."""

    def test_signal_registration_exception_tolerated(self, monkeypatch):
        """signal.signal raising must not crash main() (uvicorn still runs)."""
        import relay.relay as relay_mod
        import sys as _sys
        mock_uvicorn = MagicMock()
        _sys.modules["uvicorn"] = mock_uvicorn

        mock_signal = MagicMock()
        mock_signal.SIGTERM = 15
        mock_signal.SIGINT = 2
        mock_signal.signal.side_effect = Exception("no signals on this platform")
        _sys.modules["signal"] = mock_signal

        try:
            with patch.object(relay_mod.sys, "argv", ["relay.py"]):
                relay_mod.main()
            assert mock_uvicorn.run.call_count == 1
        finally:
            _sys.modules.pop("uvicorn", None)
            _sys.modules.pop("signal", None)
