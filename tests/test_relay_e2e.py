"""End-to-end TestClient tests with mocked upstream transports.

Exercises the full request path through FastAPI: request → _proxy_request →
_proxy_single/_proxy_stream → mocked upstream. Uses httpx.MockTransport
backed clients patched into _get_client/_make_streaming_client.

Features tested:
- Chat completions success (200) through a mocked proxy
- Streaming success through a mocked proxy (SSE relayed)
- Retry: first proxy fails (ConnectError), second succeeds
- Retry: upstream 5xx then success on different proxy
- All retries exhausted → 502
- Streaming connect error → 502
- Models endpoint with mocked upstream + filter
- Admin upstream health 503 (no upstream)
"""

from unittest.mock import patch

import httpx
import pytest


def make_client(handler) -> httpx.AsyncClient:
    """Build an AsyncClient backed by a MockTransport handler."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(10.0))


@pytest.fixture(scope="module")
def relay_mod():
    import relay.relay as relay_mod
    return relay_mod


@pytest.fixture
def fresh_pool(relay_mod):
    """Fresh pool with 3 proxies for each test."""
    relay_mod.pool = relay_mod.CooldownPool([
        "socks5://u1:p1@p1:1080",
        "socks5://u2:p2@p2:1080",
        "socks5://u3:p3@p3:1080",
    ])
    # Ensure lifespan _init_pool() re-loads these same proxies
    relay_mod.PROXY_LIST_FILE = ""
    relay_mod.PROXY_LIST_ENV = "socks5://u1:p1@p1:1080,socks5://u2:p2@p2:1080,socks5://u3:p3@p3:1080"
    # Hermetic upstream config
    relay_mod.UPSTREAM_BASE = "https://test-api.example.com/v1"
    relay_mod.UPSTREAM_API_KEY = "test-key"
    relay_mod.UPSTREAM_AUTH_TYPE = "bearer"
    relay_mod.ADMIN_API_KEY = ""  # clear admin key set by other test files
    # Restore admin rate limit to defaults (other tests may lower it)
    relay_mod._ADMIN_RATE_LIMIT = 20
    relay_mod._ADMIN_RATE_WINDOW = 60
    relay_mod._admin_rate_hits.clear()
    relay_mod._request_count["total"] = 0
    relay_mod._request_count["ok"] = 0
    relay_mod._request_count["errors"] = 0
    yield relay_mod.pool
    # Close any pooled clients (fresh loop — TestClient may have closed its own)
    import asyncio
    asyncio.run(relay_mod._close_all_clients())


# ═══════════════════════════════════════════════════════════════════
#  Chat completions end-to-end
# ═══════════════════════════════════════════════════════════════════


class TestChatCompletionsE2E:
    """Chat completions through mocked upstream."""

    def test_success_returns_openai_shape(self, relay_mod, fresh_pool):
        """A successful chat completion should return the upstream body."""

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/chat/completions"
            return httpx.Response(
                200,
                json={
                    "id": "chatcmpl-123",
                    "object": "chat.completion",
                    "choices": [{"message": {"role": "assistant", "content": "Hello!"}}],
                },
                headers={"x-request-id": "abc-123"},
            )

        mock_client = make_client(handler)

        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                resp = tc.post(
                    "/v1/chat/completions",
                    json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
                )

        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == "chatcmpl-123"
        assert data["choices"][0]["message"]["content"] == "Hello!"
        assert resp.headers.get("x-request-id") == "abc-123"

    def test_streaming_success(self, relay_mod, fresh_pool):
        """Streaming chat completion should relay SSE chunks."""
        sse_body = (
            b'data: {"choices":[{"delta":{"content":"Hel"}}]}\n\n'
            b'data: {"choices":[{"delta":{"content":"lo"}}]}\n\n'
            b"data: [DONE]\n\n"
        )

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/chat/completions"
            return httpx.Response(
                200,
                content=sse_body,
                headers={"content-type": "text/event-stream"},
            )

        mock_client = make_client(handler)

        with patch.object(relay_mod, "_make_streaming_client", return_value=mock_client):
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                resp = tc.post(
                    "/v1/chat/completions",
                    json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": True},
                )

        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type")
        assert "Hel" in resp.text
        assert "lo" in resp.text
        assert "[DONE]" in resp.text

    def test_streaming_all_proxies_cooling_429(self, relay_mod, fresh_pool):
        """All cooling → streaming returns 429 immediately."""
        # Cool all proxies
        for _ in range(fresh_pool.total):
            p = fresh_pool.next()
            if p:
                fresh_pool.record_429(p, retry_after=300)

        from fastapi.testclient import TestClient
        with TestClient(relay_mod.app) as tc:
            resp = tc.post(
                "/v1/chat/completions",
                json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": True},
            )

        assert resp.status_code == 429
        assert "all_proxies_cooling" in resp.text


# ═══════════════════════════════════════════════════════════════════
#  Retry logic end-to-end
# ═══════════════════════════════════════════════════════════════════


class TestRetryE2E:
    """Retry across proxies with mocked transports."""

    def test_retry_after_connect_error(self, relay_mod, fresh_pool):
        """First proxy connect fails → retry on second succeeds."""

        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"id": "ok-after-retry"})

        mock_client = make_client(handler)
        original_make_streaming = relay_mod._make_streaming_client

        async def fake_get_client(proxy_url):
            """First call for proxy A raises, then returns mock client."""
            attempts.append(proxy_url)
            if len(attempts) == 1:
                raise httpx.ConnectError("Simulated connection refused")
            return mock_client

        with patch.object(relay_mod, "_get_client", fake_get_client):
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                resp = tc.post(
                    "/v1/chat/completions",
                    json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
                )

        # First proxy failed, second should succeed
        assert len(attempts) >= 2
        assert resp.status_code == 200
        assert resp.json()["id"] == "ok-after-retry"

    def test_retry_after_upstream_5xx(self, relay_mod, fresh_pool):
        """Upstream 5xx on first proxy → retry on second succeeds."""
        proxy_calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"id": "ok-after-5xx"})

        mock_client = make_client(handler)
        original_single = relay_mod._proxy_single

        async def fivexx_first(client, method, url, headers, body, proxy_entry):
            proxy_calls.append(proxy_entry.url)
            if len(proxy_calls) == 1:
                # First proxy returns 503
                relay_mod.pool.record_success(proxy_entry)
                from fastapi.responses import Response
                return Response(content='{"error":"upstream down"}', status_code=503)
            return await original_single(client, method, url, headers, body, proxy_entry)

        with patch.object(relay_mod, "_proxy_single", fivexx_first):
            with patch.object(relay_mod, "_get_client", return_value=mock_client):
                from fastapi.testclient import TestClient
                with TestClient(relay_mod.app) as tc:
                    resp = tc.post(
                        "/v1/chat/completions",
                        json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
                    )

        assert len(proxy_calls) >= 2
        assert resp.status_code == 200

    def test_retries_exhausted_returns_502(self, relay_mod, fresh_pool):
        """All proxies fail with connect errors → 502."""
        async def failing_get_client(proxy_url):
            raise httpx.ConnectError("All proxies down")

        with patch.object(relay_mod, "_get_client", failing_get_client):
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                resp = tc.post(
                    "/v1/chat/completions",
                    json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
                )

        assert resp.status_code == 502
        assert "proxy_connect_failed" in resp.text

    def test_no_infinite_loop_when_retries_exceed_pool(self, relay_mod, fresh_pool):
        """MAX_REQUEST_RETRIES > pool size with all-5xx must terminate.

        Regression test: the retry loop used `continue` when a proxy was
        already tried without incrementing the attempt counter. With 2
        proxies and MAX_REQUEST_RETRIES=3, after both proxies return 5xx
        the loop would spin forever. It must break after trying all proxies.
        """
        import time

        # Shrink pool to 2 proxies (default MAX_REQUEST_RETRIES is 3)
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@p1:1080",
            "socks5://u2:p2@p2:1080",
        ])

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"error": "upstream down"})

        mock_client = make_client(handler)
        # Patch _get_client to always return the mock (never connect errors)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            from fastapi.testclient import TestClient
            start = time.monotonic()
            with TestClient(relay_mod.app) as tc:
                resp = tc.post(
                    "/v1/chat/completions",
                    json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
                )
            elapsed = time.monotonic() - start

        # Must terminate quickly (would hang forever without the fix)
        assert elapsed < 10
        assert resp.status_code == 503  # last 5xx from upstream


# ═══════════════════════════════════════════════════════════════════
#  Models endpoint
# ═══════════════════════════════════════════════════════════════════


class TestModelsE2E:
    """Models endpoint with mocked upstream."""

    def test_models_endpoint_with_mocked_upstream(self, relay_mod, monkeypatch):
        """GET /v1/models fetches and caches models from upstream."""
        relay_mod.MODELS_CACHE = []
        relay_mod.MODELS_CACHE_UPDATED = 0.0

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"data": [
                    {"id": "gpt-4", "object": "model"},
                    {"id": "gpt-4o-mini", "object": "model"},
                ]},
            )

        mock_client = make_client(handler)

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=mock_client):
            from fastapi.testclient import TestClient
            with TestClient(relay_mod.app) as tc:
                resp = tc.get("/v1/models")

        assert resp.status_code == 200
        data = resp.json()
        assert data["object"] == "list"
        assert len(data["data"]) == 2


# ═══════════════════════════════════════════════════════════════════
#  Admin endpoints
# ═══════════════════════════════════════════════════════════════════


class TestAdminE2E:
    """Admin endpoints with mocked upstream."""

    def test_upstream_health_no_upstream_503(self, relay_mod, fresh_pool, monkeypatch):
        """When UPSTREAM_BASE is empty, upstream-health returns 503."""
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "")

        from fastapi.testclient import TestClient
        with TestClient(relay_mod.app) as tc:
            resp = tc.get("/admin/upstream-health")

        assert resp.status_code == 503
        assert resp.json()["status"] == "error"
