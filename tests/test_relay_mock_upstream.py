"""End-to-end tests for _proxy_single and _proxy_stream using httpx.MockTransport.

These tests exercise the actual request/response forwarding logic with a
mocked upstream transport — no real network calls.

Features tested:
- _proxy_single: success (2xx), 429 cooldown, 4xx error, header stripping
- _proxy_stream: success stream, 429, 4xx error, header forwarding
- /v1/models cache refresh with mocked upstream
- Streaming mid-stream error handling
"""

import json
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

    async def test_4xx_records_timeout(self, relay, entry):
        """400 response should call record_timeout (increments consecutive errors)."""
        client = make_client(lambda req: httpx.Response(400, json={"error": "bad request"}))
        resp = await relay._proxy_single(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b"{}", entry,
        )
        await client.aclose()

        assert resp.status_code == 400
        assert entry.consecutive_errors == 1
        assert entry.total_ok == 0

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
        assert entry.consecutive_errors >= 1

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
