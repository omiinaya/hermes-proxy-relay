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


async def _fake_stream(data: bytes):
    """Yield body bytes in chunks like an ASGI request stream."""
    for i in range(0, len(data), 16):
        yield data[i:i + 16]


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

    async def test_stream_records_latency_only_on_success(self, relay, entry):
        """Latency recorded for 2xx streams but NOT for fast error responses."""

        async def stream_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n' b"data: [DONE]\n\n",
                headers={"content-type": "text/event-stream"},
            )

        client = make_client(stream_handler)
        resp = await relay._proxy_stream(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        await client.aclose()

        assert resp.status_code == 200
        # Success recorded a latency sample on the entry
        assert entry.latency_samples == 1
        assert entry.last_latency_ms >= 0

    async def test_stream_429_does_not_record_latency(self, relay, entry):
        """Fast 429 must not pollute the latency average (skew guard)."""
        client = make_client(
            lambda req: httpx.Response(429, json={"error": "rate limited"}, headers={"retry-after": "60"})
        )
        resp = await relay._proxy_stream(
            client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        await client.aclose()

        assert resp.status_code == 429
        # No latency sample recorded for the error response
        assert entry.latency_samples == 0
        assert entry.total_429 == 1

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

    async def test_stream_error_aclose_raise_no_double_release(self, relay, entry):
        """BUG-2 regression: a client.aclose() that RAISES on the error
        paths (429 AND >=400) must not propagate — otherwise the caller
        never marks semaphore_handed_off and its finally releases the
        semaphore AGAIN (over-credit → concurrency limit exceeded)."""
        import asyncio as _asyncio

        for status, body in ((429, b'{"error": "rate limited"}'), (404, b'{"error": "not found"}')):
            releases = []

            class BadCloseResp:
                async def aread(self):
                    return body

                async def aclose(self):
                    pass

                @property
                def status_code(self):
                    return status

                @property
                def headers(self):
                    return {"content-type": "application/json", "retry-after": "60"}

            fake_resp = BadCloseResp()
            fake_client = MagicMock()
            fake_client.send = AsyncMock(return_value=fake_resp)
            fake_client.aclose = AsyncMock(side_effect=Exception("transport broken"))
            fake_client.build_request = MagicMock(return_value=MagicMock())

            sem = _asyncio.Semaphore(1)
            # Record releases on the semaphore object itself (matches the
            # pattern in test_stream_semaphore_held_until_generator_done).
            sem.release = lambda: releases.append("released")  # type: ignore[method-assign]

            resp = await relay._proxy_stream(
                fake_client, "POST", "https://upstream.example.com/v1/chat/completions",
                {}, b'{"stream": true}', entry, acquired_sem=sem,
            )

            # Response still returned (aclose error swallowed), semaphore
            # released exactly ONCE by the error path — a propagated aclose
            # would trigger the caller's finally double-release.
            assert resp.status_code == status
            assert releases == ["released"], f"status {status}: {releases}"
            assert len(releases) == 1

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
        # The client sees a sanitized message — raw exception text (which
        # may embed socket/upstream internals) is logged server-side only.
        assert b"connection reset" not in body.lower()
        assert b"stream interrupted" in body.lower()
        # Mid-stream errors are TRANSIENT — they must NOT count toward
        # permanent death (a flaky upstream must not kill good proxies).
        assert entry.consecutive_errors == 0
        assert not entry.permanently_dead
        assert relay._request_count["errors"] >= 1

    async def test_stream_midstream_error_non_sse_unframed(self, relay, entry):
        """Non-SSE upstream error yields raw JSON (no data: framing)."""
        relay._stream_shutdown_event.clear()

        class FailingStream:
            async def aiter_bytes(self):
                if False:
                    yield b""
                raise ConnectionResetError("boom mid-stream")

            async def aread(self):
                return b""

            async def aclose(self):
                pass

            @property
            def status_code(self):
                return 200

            @property
            def headers(self):
                return {"content-type": "application/json"}  # NOT SSE

        fake_resp = FailingStream()
        fake_client = MagicMock()
        fake_client.send = AsyncMock(return_value=fake_resp)
        fake_client.aclose = AsyncMock()
        fake_client.build_request = MagicMock(return_value=MagicMock())

        streaming_resp = await relay._proxy_stream(
            fake_client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', entry,
        )
        body = b"".join([chunk async for chunk in streaming_resp.body_iterator])

        assert b"stream_error" in body
        # Non-SSE: plain JSON object, NOT prefixed with "data: "
        assert not body.lstrip().startswith(b"data:")
        assert b"data:" not in body

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

    async def test_stream_read_timeout_not_permanent_death(self, relay, entry, monkeypatch):
        """A stream ReadTimeout (upstream stall) cools the proxy briefly but
        does NOT increment consecutive_errors toward permanent death."""
        async def stall_client(proxy_url):
            client = AsyncMock()
            client.aclose = AsyncMock()
            client.build_request = MagicMock(return_value=MagicMock())
            return client

        async def stall_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            raise httpx.ReadTimeout("upstream slow")

        monkeypatch.setattr(relay, "_make_streaming_client", stall_client)
        monkeypatch.setattr(relay, "_proxy_stream", stall_stream)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 502
        assert b"upstream_timeout" in resp.body
        # Transient: NOT counted toward permanent death
        proxy_entry = relay.pool._proxies[0]
        assert proxy_entry.consecutive_errors == 0
        assert not proxy_entry.permanently_dead

    async def test_stream_semaphore_held_until_generator_done(self, relay, monkeypatch):
        """The concurrency semaphore is held for the WHOLE stream — released
        by the generator's finally, not before the first byte."""
        import asyncio as _asyncio
        releases = []

        async def ok_client(proxy_url):
            client = AsyncMock()
            client.aclose = AsyncMock()
            client.build_request = MagicMock(return_value=MagicMock())
            return client

        class GenStream:
            async def aiter_bytes(self):
                yield b'data: {"ok":true}\n\n'

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

        # Capture the REAL _proxy_stream before patching — the mock delegates
        # to it with the acquired semaphore (no recursion this way).
        real_proxy_stream = relay._proxy_stream

        async def ok_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            fake_resp = GenStream()
            fake_client = MagicMock()
            fake_client.send = AsyncMock(return_value=fake_resp)
            fake_client.aclose = AsyncMock()
            fake_client.build_request = MagicMock(return_value=MagicMock())
            if acquired_sem is not None:
                acquired_sem.release = lambda: releases.append("released")
            return await real_proxy_stream(
                fake_client, method, url, headers, body, proxy_entry, acquired_sem,
            )

        monkeypatch.setattr(relay, "_make_streaming_client", ok_client)
        monkeypatch.setattr(relay, "_proxy_stream", ok_stream)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)
        # These tests exercise HOLD-FOR-STREAM semantics — pin it explicitly so
        # a future default flip (or a live operator config) cannot silently
        # release the permit before the generator finishes.
        monkeypatch.setattr(relay, "HOLD_PERMIT_FOR_STREAM", True)

        # Exhaust the semaphore so only one slot is free
        orig_sem = relay.semaphore
        relay.semaphore = _asyncio.Semaphore(1)
        try:
            resp = await relay._proxy_request(
                "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
                {"content-type": "application/json"}, "",
            )
            # While the response exists, the slot must still be held
            # (_value 0 = the only permit is taken) and release NOT yet called.
            assert releases == []
            assert relay.semaphore._value == 0
            # Consume the generator → finally releases
            body = b"".join([chunk async for chunk in resp.body_iterator])
            assert b"ok" in body
            assert releases == ["released"]
        finally:
            relay.semaphore = orig_sem

    async def test_stream_semaphore_released_via_finalizer_when_never_started(self, relay, monkeypatch):
        """BUG-1 regression: a client disconnect BEFORE the response starts must
        not leak the permit — the weakref finalizer releases it on GC.

        Starlette's stream_response never closes a body iterator it hasn't
        started; if the client disconnects while the relay waits for slow
        upstream headers, the generator's finally never runs. Before the
        finalizer, each such disconnect permanently consumed a semaphore
        slot and the relay degraded toward 503 after enough disconnects.
        """
        import asyncio as _asyncio
        import gc
        releases = []

        async def ok_client(proxy_url):
            client = AsyncMock()
            client.aclose = AsyncMock()
            client.build_request = MagicMock(return_value=MagicMock())
            return client

        class GenStream:
            async def aiter_bytes(self):
                yield b'data: {"ok":true}\n\n'

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

        real_proxy_stream = relay._proxy_stream

        async def ok_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            fake_resp = GenStream()
            fake_client = MagicMock()
            fake_client.send = AsyncMock(return_value=fake_resp)
            fake_client.aclose = AsyncMock()
            fake_client.build_request = MagicMock(return_value=MagicMock())
            if acquired_sem is not None:
                acquired_sem.release = lambda: releases.append("released")
            return await real_proxy_stream(
                fake_client, method, url, headers, body, proxy_entry, acquired_sem,
            )

        monkeypatch.setattr(relay, "_make_streaming_client", ok_client)
        monkeypatch.setattr(relay, "_proxy_stream", ok_stream)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)
        # These tests exercise HOLD-FOR-STREAM semantics — pin it explicitly so
        # a future default flip (or a live operator config) cannot silently
        # release the permit before the generator finishes.
        monkeypatch.setattr(relay, "HOLD_PERMIT_FOR_STREAM", True)

        orig_sem = relay.semaphore
        relay.semaphore = _asyncio.Semaphore(1)
        try:
            resp = await relay._proxy_request(
                "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
                {"content-type": "application/json"}, "",
            )
            # Slot held, generator NOT started (client disconnect before
            # Starlette sends the response start)
            assert releases == []
            assert relay.semaphore._value == 0
            # Drop the response WITHOUT ever iterating the generator —
            # the finally never runs, so only the finalizer can release.
            del resp
            gc.collect()
            await _asyncio.sleep(0.05)
            gc.collect()
            # The release was called exactly once, via the weakref
            # finalizer (the generator's finally never ran — it was
            # never started). Note: _value stays 0 because the wrapper
            # redirected release() to the recording lambda; the call
            # itself is the proof.
            assert releases == ["released"]
        finally:
            relay.semaphore = orig_sem


class TestModelsEndpointMocked:
    """/v1/models with a mocked upstream response."""

    @pytest.fixture
    def relay(self):
        import asyncio as _asyncio
        import relay.relay as relay_mod
        # Reset ALL global state the models path depends on — a prior test
        # class may have left the module-global pool cooling or the cache
        # freshly populated, which would change which list_models branch
        # executes (CI runs files alphabetically; local order can differ).
        relay_mod.MODELS_CACHE = []
        relay_mod.MODELS_CACHE_UPDATED = 0.0  # fresh fixture state — set to stale by tests
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        # Fresh semaphore per test — the module-global one binds to the
        # first event loop it touches; function-scoped pytest-asyncio loops
        # would otherwise raise "bound to a different event loop", which
        # list_models' generic except swallows → coverage branch missed.
        relay_mod.semaphore = _asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
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

        with patch.object(relay, "_get_client", return_value=mock_client):
            # Call the models endpoint logic directly
            result = await relay.list_models()

        assert result["object"] == "list"
        # MODEL_FILTER_PATTERN is ".*" so all models pass
        assert len(result["data"]) == 3
        assert relay.MODELS_CACHE == result["data"]

    async def test_models_returns_cache_when_fresh(self, relay, monkeypatch):
        """When cache is fresh, /v1/models returns from cache without upstream call."""
        relay._update_models_cache([{"id": "cached-model", "object": "model"}])

        with patch.object(relay, "_get_client") as mock_get:
            result = await relay.list_models()

        # No upstream call — served from cache
        mock_get.assert_not_called()
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
            with patch.object(relay, "_get_client", return_value=mock_client):
                result = await relay.list_models()
        finally:
            relay._model_filter_re = original

        ids = [m["id"] for m in result["data"]]
        assert "gpt-4-free" in ids
        assert "gpt-4o" not in ids

    async def test_models_upstream_failure_returns_cache(self, relay, monkeypatch):
        """When upstream fails, return existing cache (or empty)."""
        with patch.object(relay, "_get_client", side_effect=Exception("Connection refused")):
            result = await relay.list_models()

        assert result["object"] == "list"
        assert isinstance(result["data"], list)

    async def test_models_all_cooling_serves_cache(self, relay, monkeypatch):
        """All proxies cooling → serve cache without upstream call."""
        relay._update_models_cache([{"id": "cached-model", "object": "model"}])
        relay.MODELS_CACHE_UPDATED = time.monotonic() - 10000  # force stale cache (>TTL even on fresh boot)
        # Cool every proxy
        for p in relay.pool._proxies:
            relay.pool.record_429(p, retry_after=3600)

        with patch.object(relay, "_get_client") as mock_get:
            result = await relay.list_models()

        mock_get.assert_not_called()
        assert result["data"] == [{"id": "cached-model", "object": "model"}]

    async def test_models_semaphore_busy_serves_cache(self, relay, monkeypatch):
        """Semaphore exhausted → models served from cache without upstream call."""
        relay.pool = relay.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        relay._update_models_cache([{"id": "cached-model", "object": "model"}])
        relay.MODELS_CACHE_UPDATED = time.monotonic() - 10000  # force stale cache (>TTL even on fresh boot)

        # Exhaust the semaphore so acquisition times out
        acquired = []
        for _ in range(relay.MAX_CONCURRENT_UPSTREAM):
            acquired.append(await relay.semaphore.acquire())
        try:
            monkeypatch.setattr(relay, "SEMAPHORE_WAIT_SECONDS", 0.01)
            with patch.object(relay, "_get_client") as mock_get:
                result = await relay.list_models()
        finally:
            for _ in acquired:
                relay.semaphore.release()

        mock_get.assert_not_called()
        assert result["data"] == [{"id": "cached-model", "object": "model"}]


# ═══════════════════════════════════════════════════════════════════
#  Admin upstream health
# ═══════════════════════════════════════════════════════════════════


class TestAdminUpstreamHealthMocked:
    """/admin/upstream-health with mocked upstream."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        # Fresh semaphore per test — the module-global one binds to the
        # first event loop it touches (see TestProxyRequestEdgeBranches).
        import asyncio as _asyncio
        relay_mod.semaphore = _asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
        return relay_mod

    async def test_health_ok(self, relay, monkeypatch):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"data": [{"id": "model-a"}]})

        mock_client = make_client(handler)

        with patch.object(relay, "_get_client", return_value=mock_client):
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

        with patch.object(relay, "_get_client", return_value=mock_client):
            req = MagicMock()
            req.client.host = "127.0.0.1"
            data = await relay.admin_upstream_health(req)

        assert data["status"] == "degraded"
        assert data["upstream_status"] == 503

    async def test_health_unparseable_body_counts_zero(self, relay, monkeypatch):
        """200 with non-JSON body → models_count 0, status ok."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"<html>not json</html>")

        mock_client = make_client(handler)

        with patch.object(relay, "_get_client", return_value=mock_client):
            req = MagicMock()
            req.client.host = "127.0.0.1"
            data = await relay.admin_upstream_health(req)

        assert data["status"] == "ok"
        assert data["models_count"] == 0

    async def test_health_semaphore_busy_returns_503(self, relay, monkeypatch):
        """Semaphore exhausted → upstream health returns 503 at capacity."""
        # Exhaust the semaphore so acquisition times out
        acquired = []
        for _ in range(relay.MAX_CONCURRENT_UPSTREAM):
            acquired.append(await relay.semaphore.acquire())
        try:
            monkeypatch.setattr(relay, "SEMAPHORE_WAIT_SECONDS", 0.01)
            with patch.object(relay, "_get_client") as mock_get:
                req = MagicMock()
                req.client.host = "127.0.0.1"
                resp = await relay.admin_upstream_health(req)
        finally:
            for _ in acquired:
                relay.semaphore.release()

        assert resp.status_code == 503
        assert "at capacity" in resp.body.decode()
        mock_get.assert_not_called()


class TestSecurityFixes:
    """Security hardening: credential masking, auth-before-body, header strip."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://user1:pass1@192.168.1.10:1080",
            "socks5://user2:pass2@192.168.1.11:1080",
        ])
        import asyncio as _asyncio
        relay_mod.semaphore = _asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
        return relay_mod

    def test_mask_proxy_url_hides_credentials(self, relay):
        """_mask_proxy_url never reveals user:pass."""
        assert relay._mask_proxy_url("socks5://user:pass@host:1080") == "socks5://***@host:1080"
        # No credentials → unchanged
        assert relay._mask_proxy_url("socks5://host:1080") == "socks5://host:1080"
        assert relay._mask_proxy_url("") == ""

    def test_mask_proxy_url_scheme_less_credentials(self, relay):
        """BUG-5 regression: a scheme-less URL with creds
        (`user:pass@host:1080`) must be masked too — the old partition('://')
        check fell through and leaked raw credentials into /health."""
        assert relay._mask_proxy_url("user:pass@host:1080") == "***@host:1080"
        # userinfo with no password, no scheme
        assert relay._mask_proxy_url("user@host:1080") == "***@host:1080"

    def test_stats_masks_proxy_urls(self, relay):
        """pool.stats() cooling details must not expose credentials."""
        p = relay.pool.next()
        assert p is not None
        relay.pool.record_429(p, retry_after=3600)
        stats = relay.pool.stats()
        all_urls = []
        for details in (stats["cooling_details"], stats["permanently_failed_details"]):
            for entry in details:
                all_urls.append(entry["proxy"])
        assert all_urls, "expected at least one cooling proxy"
        for url in all_urls:
            assert "user1:pass1" not in url
            assert "user2:pass2" not in url
            assert "@" not in url.replace("://***@", "")

    def test_build_headers_strips_client_x_api_key(self, relay, monkeypatch):
        """Client-supplied X-API-Key must not reach the upstream under bearer auth."""
        monkeypatch.setattr(relay, "UPSTREAM_AUTH_TYPE", "bearer")
        monkeypatch.setattr(relay, "UPSTREAM_API_KEY", "relay-key")
        headers = relay._build_headers({
            "X-API-Key": "client-key",
            "content-type": "application/json",
        })
        assert "x-api-key" not in {k.lower() for k in headers}
        assert headers.get("Authorization") == "Bearer relay-key"

    def test_build_headers_injects_upstream_x_api_key_when_configured(self, relay, monkeypatch):
        """Under x-api-key auth, the relay injects its own key and drops the client's."""
        monkeypatch.setattr(relay, "UPSTREAM_AUTH_TYPE", "x-api-key")
        monkeypatch.setattr(relay, "UPSTREAM_API_KEY", "relay-key")
        headers = relay._build_headers({"X-API-Key": "client-key"})
        assert headers.get("x-api-key") == "relay-key"

    async def test_chat_completions_auth_before_body(self, relay, monkeypatch):
        """Unauthenticated request is rejected before the body is read."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "s3cret")
        sent = {}

        async def fake_proxy(method, path, body, headers, query, go=False):
            sent["called"] = True
            return {"ok": True}

        with patch.object(relay, "_proxy_request", fake_proxy):
            from fastapi.testclient import TestClient
            with TestClient(relay.app) as tc:
                resp = tc.post(
                    "/v1/chat/completions",
                    content=b'{"model":"gpt-4"}',
                    headers={"content-type": "application/json"},
                )

        assert resp.status_code == 401
        assert "called" not in sent
        assert relay._request_count["auth_failed"] == 1

    async def test_proxy_all_auth_before_body(self, relay, monkeypatch):
        """Unauthenticated generic-route request rejected before body read."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "s3cret")
        sent = {}

        async def fake_proxy(method, path, body, headers, query, go=False):
            sent["called"] = True
            return {"ok": True}

        with patch.object(relay, "_proxy_request", fake_proxy):
            from fastapi.testclient import TestClient
            with TestClient(relay.app) as tc:
                resp = tc.post(
                    "/v1/embeddings",
                    content=b'{"input":"x"}',
                    headers={"content-type": "application/json"},
                )

        assert resp.status_code == 401
        assert "called" not in sent


class TestProxyAllMethodBodies:
    """Body handling across proxy_all HTTP methods."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
        ])
        import asyncio as _asyncio
        relay_mod.semaphore = _asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
        relay_mod.CLIENT_API_KEY = ""
        return relay_mod

    async def test_delete_body_forwarded(self, relay, monkeypatch):
        """DELETE with a JSON body must forward that body upstream."""
        sent = {}

        async def fake_proxy(method, path, body, headers, query, go=False):
            sent["method"] = method
            sent["body"] = body
            return {"ok": True}

        with patch.object(relay, "_proxy_request", fake_proxy):
            from fastapi.testclient import TestClient
            with TestClient(relay.app) as tc:
                resp = tc.request(
                    "DELETE",
                    "/v1/files/f-123",
                    content=b'{"confirm": true}',
                    headers={"content-type": "application/json"},
                )

        assert resp.status_code == 200
        assert sent["method"] == "DELETE"
        assert sent["body"] == b'{"confirm": true}'

    async def test_get_body_not_read(self, relay, monkeypatch):
        """GET must not attempt to read a body (Content-Length absent)."""
        sent = {}

        async def fake_proxy(method, path, body, headers, query, go=False):
            sent["method"] = method
            sent["body"] = body
            return {"ok": True}

        with patch.object(relay, "_proxy_request", fake_proxy):
            from fastapi.testclient import TestClient
            with TestClient(relay.app) as tc:
                resp = tc.get("/v1/models/test", headers={"x-test": "1"})

        assert resp.status_code == 200
        assert sent["method"] == "GET"
        assert sent["body"] is None


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
        # Fresh semaphore per test — the module-global one binds to the
        # first event loop it touches; function-scoped pytest-asyncio loops
        # would otherwise raise "bound to a different event loop".
        import asyncio as _asyncio
        relay_mod.semaphore = _asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
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
        # Retries disabled → no attempt was made → explicit 503, not a
        # misleading "all proxies cooling" 429.
        assert resp.status_code == 503
        assert b"retries_disabled" in resp.body

    async def test_chat_completions_handler_forwards_query(self, relay, monkeypatch):
        """chat_completions() passes the query string through to _proxy_request."""
        calls = {}

        async def fake_proxy_request(method, path, body, headers, query, go=False):
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

    async def test_stream_connect_error_releases_client_borrow(self, relay, monkeypatch):
        """Stream path: ConnectError after client creation RELEASES the pooled
        client borrow (pre-1.6 closed the client).

        Covers the `if streaming_client is not None: _release_client_in_use(...)`
        branch — _make_streaming_client borrows a pooled client but
        _proxy_stream raises a connect error mid-flight. With the cross-proxy
        retry loop, each failed attempt's borrow is released and the request
        fails only after ALL proxies have been tried.
        """
        mock_client = AsyncMock()
        mock_client.aclose = AsyncMock()

        released = []
        real_release = relay._release_client_in_use
        monkeypatch.setattr(
            relay, "_release_client_in_use",
            lambda url: (released.append(url), real_release(url)),
        )
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
        # Both proxies in the pool were tried; each RELEASED its borrow. The
        # client is NOT torn down — it stays pooled for reuse.
        assert len(released) == relay.pool.total
        assert mock_client.aclose.await_count == 0

    async def test_stream_connect_error_recovers_on_second_proxy(self, relay, monkeypatch):
        """Stream path: first proxy connect fails, retry succeeds on the second.

        This is the whole point of the stream retry loop — a dead proxy
        shouldn't kill a streaming request when another proxy is healthy.
        """
        calls = []

        async def flaky_streaming_client(proxy_url):
            calls.append(proxy_url)
            client = AsyncMock()
            client.aclose = AsyncMock()
            client.build_request = MagicMock(return_value=MagicMock())
            return client

        async def flaky_proxy_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            # First proxy fails to connect; second proxy streams fine.
            if calls[0] == proxy_entry.url and len(calls) == 1:
                raise httpx.ConnectError("first proxy dead")
            resp = AsyncMock()
            resp.status_code = 200
            return resp

        monkeypatch.setattr(relay, "_make_streaming_client", flaky_streaming_client)
        monkeypatch.setattr(relay, "_proxy_stream", flaky_proxy_stream)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 200
        # Both proxies were attempted: first failed, second succeeded
        assert len(calls) == relay.pool.total
        assert calls[0] != calls[1]

    async def test_stream_5xx_retries_next_proxy(self, relay, monkeypatch):
        """Stream path: upstream 5xx (pre-stream) retries on the next proxy."""
        calls = []

        async def flaky_streaming_client(proxy_url):
            calls.append(proxy_url)
            client = AsyncMock()
            client.aclose = AsyncMock()
            client.build_request = MagicMock(return_value=MagicMock())
            return client

        async def flaky_proxy_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            # First proxy returns 503 from upstream; second proxy streams fine.
            if calls[0] == proxy_entry.url and len(calls) == 1:
                resp = AsyncMock()
                resp.status_code = 503
                return resp
            resp = AsyncMock()
            resp.status_code = 200
            return resp

        monkeypatch.setattr(relay, "_make_streaming_client", flaky_streaming_client)
        monkeypatch.setattr(relay, "_proxy_stream", flaky_proxy_stream)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 200
        assert len(calls) == relay.pool.total
        assert calls[0] != calls[1]

    async def test_stream_all_proxies_tried_stops_loop(self, relay, monkeypatch):
        """Stream retry loop stops once every proxy has been tried."""
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 10)  # > pool size (2)

        async def failing_streaming_client(proxy_url):
            client = AsyncMock()
            client.aclose = AsyncMock()
            client.build_request = MagicMock(return_value=MagicMock())
            return client

        async def failing_proxy_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            raise httpx.ConnectError("all proxies dead")

        monkeypatch.setattr(relay, "_make_streaming_client", failing_streaming_client)
        monkeypatch.setattr(relay, "_proxy_stream", failing_proxy_stream)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 502
        assert b"proxy_connect_failed" in resp.body

    async def test_stream_5xx_all_proxies_tried_stops_loop(self, relay, monkeypatch):
        """5xx responses don't cool proxies — the rotation-stall guard must stop
        the loop when next() keeps returning already-tried proxies."""
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 10)  # > pool size (2)

        async def always_503_client(proxy_url):
            client = AsyncMock()
            client.aclose = AsyncMock()
            client.build_request = MagicMock(return_value=MagicMock())
            return client

        async def always_503_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            resp = AsyncMock()
            resp.status_code = 503
            return resp

        monkeypatch.setattr(relay, "_make_streaming_client", always_503_client)
        monkeypatch.setattr(relay, "_proxy_stream", always_503_stream)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        # Loop broke via the guard (all proxies tried) — returns last 503
        assert resp.status_code == 503

    async def test_stream_dup_scan_guard_partial_cooling(self, relay, monkeypatch):
        """Rotation-stall dup_scan guard: one proxy cooling, rest 5xx.

        next() never returns the cooling proxy, so the loop keeps seeing
        already-tried proxies. The dup_scan guard breaks after a full
        rotation of duplicates instead of spinning.
        """
        # Need a 3-proxy pool: one cooling (never returned), two 5xx-ing
        relay.pool = relay.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
            "socks5://u3:p3@192.168.1.12:1080",
        ])
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 10)

        # Cool the THIRD proxy for a long time so it's never returned
        p3 = relay.pool._proxies[2]
        relay.pool.record_429(p3, retry_after=3600)

        async def always_503_client(proxy_url):
            client = AsyncMock()
            client.aclose = AsyncMock()
            client.build_request = MagicMock(return_value=MagicMock())
            return client

        async def always_503_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            resp = AsyncMock()
            resp.status_code = 503
            return resp

        monkeypatch.setattr(relay, "_make_streaming_client", always_503_client)
        monkeypatch.setattr(relay, "_proxy_stream", always_503_stream)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        # dup_scan guard broke the loop — returns the last 503
        assert resp.status_code == 503

    async def test_stream_zero_retries_returns_429(self, relay, monkeypatch):
        """MAX_REQUEST_RETRIES=0 → stream loop never runs → 503 retries_disabled."""
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 0)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 503
        assert b"retries_disabled" in resp.body

    async def test_semaphore_timeout_returns_503_stream(self, relay, monkeypatch):
        """Stream path: all concurrency slots busy → bounded wait → 503."""
        # Exhaust the semaphore
        acquired = []
        for _ in range(relay.MAX_CONCURRENT_UPSTREAM):
            acquired.append(await relay.semaphore.acquire())

        try:
            monkeypatch.setattr(relay, "SEMAPHORE_WAIT_SECONDS", 0.01)
            resp = await relay._proxy_request(
                "POST", "/chat/completions", b'{"stream":true,"model":"gpt-4"}',
                {"content-type": "application/json"}, "",
            )
            assert resp.status_code == 503
            assert b"relay_at_capacity" in resp.body
            assert resp.headers.get("retry-after") == "10"
        finally:
            for _ in acquired:
                relay.semaphore.release()

    async def test_semaphore_timeout_returns_503_nonstream(self, relay, monkeypatch):
        """Non-stream path: all concurrency slots busy → bounded wait → 503."""
        acquired = []
        for _ in range(relay.MAX_CONCURRENT_UPSTREAM):
            acquired.append(await relay.semaphore.acquire())

        try:
            monkeypatch.setattr(relay, "SEMAPHORE_WAIT_SECONDS", 0.01)
            resp = await relay._proxy_request(
                "POST", "/chat/completions", b'{"model":"gpt-4"}',
                {"content-type": "application/json"}, "",
            )
            assert resp.status_code == 503
            assert b"relay_at_capacity" in resp.body
        finally:
            for _ in acquired:
                relay.semaphore.release()

    async def test_semaphore_acquired_released(self, relay, monkeypatch):
        """Normal request acquires and releases the semaphore."""
        async def fake_client(url, mark_in_use=False):
            mock = AsyncMock()
            return mock

        async def fake_single(client, method, url, headers, body, proxy_entry):
            from fastapi.responses import Response
            return Response(content='{"ok":true}', status_code=200)

        monkeypatch.setattr(relay, "_get_client", fake_client)
        monkeypatch.setattr(relay, "_proxy_single", fake_single)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)

        before = relay.semaphore._value
        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 200
        assert relay.semaphore._value == before  # released after use

    async def test_acquire_semaphore_no_timeout(self, relay):
        """_acquire_semaphore() without timeout returns the acquired semaphore."""
        acquired = await relay._acquire_semaphore()
        assert acquired is relay.semaphore
        acquired.release()  # restore

    async def test_acquire_semaphore_timeout_returns_none(self, relay, monkeypatch):
        """_acquire_semaphore() returns None when the wait times out."""
        # Exhaust the semaphore so acquisition can't succeed
        acquired = []
        for _ in range(relay.MAX_CONCURRENT_UPSTREAM):
            acquired.append(await relay.semaphore.acquire())
        try:
            monkeypatch.setattr(relay, "SEMAPHORE_WAIT_SECONDS", 0.01)
            result = await relay._acquire_semaphore(0.01)
            assert result is None
        finally:
            for _ in acquired:
                relay.semaphore.release()

    async def test_acquire_semaphore_permit_race_returns_sem(self, relay, monkeypatch):
        """If the acquire completes in the same tick the timeout fires, the
        permit is OURS — return the semaphore so the caller releases it
        (otherwise the permit leaks forever, drifting capacity down)."""
        import asyncio as _asyncio

        # Force the race: wait_for raises TimeoutError but the inner task
        # has ALREADY acquired the permit (task.done() == True).
        real_wait_for = relay.asyncio.wait_for

        async def racing_wait_for(task, timeout):
            # Let the acquire complete first...
            await task
            # ...then raise the timeout anyway (the race window)
            raise _asyncio.TimeoutError

        monkeypatch.setattr(relay.asyncio, "wait_for", racing_wait_for)

        # Semaphore with a free permit — the inner acquire will complete
        # immediately, then racing_wait_for raises TimeoutError.
        sem = _asyncio.Semaphore(1)
        monkeypatch.setattr(relay, "semaphore", sem)

        result = await relay._acquire_semaphore(0.01)
        assert result is sem  # permit handed back — caller must release
        # Clean up the taken permit
        result.release()
        assert sem._value == 1

    async def test_acquire_semaphore_cancel_race_releases_permit(self, relay, monkeypatch):
        """Outer-task cancellation racing a completed acquire must release the permit.

        If the request task is cancelled in the same tick the inner acquire
        completes, wait_for cancels the (already-done) inner task and
        re-raises CancelledError; the taken permit would leak forever
        without the release in the cancellation handler.
        """
        import asyncio as _asyncio

        # Semaphore with a free permit — the inner acquire completes
        # immediately; the fake wait_for then raises CancelledError to
        # simulate the outer task being cancelled in that same tick.
        sem = _asyncio.Semaphore(1)
        monkeypatch.setattr(relay, "semaphore", sem)

        async def cancelling_wait_for(task, timeout):
            await task  # acquire completes — permit is taken
            assert task.done() and not task.cancelled()
            raise _asyncio.CancelledError

        monkeypatch.setattr(relay.asyncio, "wait_for", cancelling_wait_for)

        with pytest.raises(_asyncio.CancelledError):
            await relay._acquire_semaphore(0.01)

        # The taken permit was released before the cancellation propagated
        assert sem._value == 1

    async def test_client_auth_rejects_missing_key(self, relay, monkeypatch):
        """CLIENT_API_KEY set + no key → 401."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "client-secret")
        monkeypatch.setattr(relay, "_request_count", {"total": 0, "ok": 0, "errors": 0, "auth_failed": 0})
        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 401
        assert b"invalid_client_key" in resp.body
        assert resp.headers.get("www-authenticate") == "Bearer"
        assert relay._request_count["auth_failed"] == 1

    async def test_client_auth_accepts_bearer(self, relay, monkeypatch):
        """CLIENT_API_KEY set + correct Bearer key → proceeds (no 401)."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "client-secret")

        async def fake_single(client, method, url, headers, body, proxy_entry):
            from fastapi.responses import Response
            return Response(content='{"ok":true}', status_code=200)

        async def fake_get(url, mark_in_use=False):
            return AsyncMock()

        monkeypatch.setattr(relay, "_get_client", fake_get)
        monkeypatch.setattr(relay, "_proxy_single", fake_single)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)
        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"model":"gpt-4"}',
            {"content-type": "application/json", "authorization": "Bearer client-secret"}, "",
        )
        assert resp.status_code == 200

    async def test_client_auth_accepts_x_api_key(self, relay, monkeypatch):
        """CLIENT_API_KEY set + correct X-API-Key header → proceeds."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "client-secret")

        async def fake_single(client, method, url, headers, body, proxy_entry):
            from fastapi.responses import Response
            return Response(content='{"ok":true}', status_code=200)

        async def fake_get(url, mark_in_use=False):
            return AsyncMock()

        monkeypatch.setattr(relay, "_get_client", fake_get)
        monkeypatch.setattr(relay, "_proxy_single", fake_single)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)
        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"model":"gpt-4"}',
            {"content-type": "application/json", "x-api-key": "client-secret"}, "",
        )
        assert resp.status_code == 200

    async def test_client_auth_rejects_wrong_key(self, relay, monkeypatch):
        """CLIENT_API_KEY set + wrong key → 401."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "client-secret")
        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"model":"gpt-4"}',
            {"content-type": "application/json", "authorization": "Bearer wrong"}, "",
        )
        assert resp.status_code == 401

    async def test_client_auth_disabled_by_default(self, relay, monkeypatch):
        """CLIENT_API_KEY empty → no auth required."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "")

        async def fake_single(client, method, url, headers, body, proxy_entry):
            from fastapi.responses import Response
            return Response(content='{"ok":true}', status_code=200)

        async def fake_get(url, mark_in_use=False):
            return AsyncMock()

        monkeypatch.setattr(relay, "_get_client", fake_get)
        monkeypatch.setattr(relay, "_proxy_single", fake_single)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)
        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 200

    async def test_models_gated_by_client_key(self, relay, monkeypatch):
        """list_models with CLIENT_API_KEY set + no key → 401."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "client-secret")
        monkeypatch.setattr(relay, "_request_count", {"total": 0, "ok": 0, "errors": 0, "auth_failed": 0})
        relay.MODELS_CACHE = []
        relay.MODELS_CACHE_UPDATED = time.monotonic() - 10000  # guaranteed stale (>TTL)

        from fastapi import Request as FastAPIRequest
        scope = {"type": "http", "headers": []}
        req = FastAPIRequest(scope)
        result = await relay.list_models(req)
        assert result.status_code == 401
        assert b"invalid_client_key" in result.body
        assert relay._request_count["auth_failed"] == 1

    async def test_models_allows_valid_client_key(self, relay, monkeypatch):
        """list_models with correct key → serves models (not 401)."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "client-secret")
        relay.MODELS_CACHE = [{"id": "cached-model"}]
        relay.MODELS_CACHE_UPDATED = time.monotonic() - 10000  # guaranteed stale (>TTL)

        from fastapi import Request as FastAPIRequest
        scope = {
            "type": "http",
            "path": "/v1/models",
            "headers": [(b"authorization", b"Bearer client-secret")],
        }
        req = FastAPIRequest(scope)
        result = await relay.list_models(req)
        # Cache is stale → proxy path with mocked client
        assert result["object"] == "list"

    async def test_client_key_valid_helper(self, relay, monkeypatch):
        """_client_key_valid: bearer, x-api-key, case-insensitive headers."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "s3cret")
        assert relay._client_key_valid({"Authorization": "Bearer s3cret"}) is True
        assert relay._client_key_valid({"X-API-Key": "s3cret"}) is True
        assert relay._client_key_valid({"authorization": "Bearer wrong"}) is False
        assert relay._client_key_valid({}) is False
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "")
        assert relay._client_key_valid({}) is True  # disabled → always valid

    def test_body_too_large_header_check(self, relay, monkeypatch):
        """_body_too_large: Content-Length header pre-reject."""
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 100)
        from starlette.datastructures import Headers

        big_req = MagicMock()
        big_req.headers = Headers({"content-length": "5000"})
        assert relay._body_too_large(big_req) is True

        small_req = MagicMock()
        small_req.headers = Headers({"content-length": "50"})
        assert relay._body_too_large(small_req) is False

        # Missing header → not pre-rejected (body read decides)
        no_len = MagicMock()
        no_len.headers = Headers({})
        assert relay._body_too_large(no_len) is False

        # Malformed Content-Length → not pre-rejected (ignored, body decides)
        bad_len = MagicMock()
        bad_len.headers = Headers({"content-length": "not-a-number"})
        assert relay._body_too_large(bad_len) is False

        # Disabled (0/negative) → never rejects
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 0)
        assert relay._body_too_large(big_req) is False

    async def test_read_body_capped_rejects_oversized(self, relay, monkeypatch):
        """_read_body_capped returns None for bodies over the cap."""
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 64)
        req = MagicMock()
        req.headers = {}
        req.stream = lambda: _fake_stream(b"x" * 200)
        assert await relay._read_body_capped(req) is None

    async def test_read_body_capped_accepts_under_limit(self, relay, monkeypatch):
        """_read_body_capped returns the body when within the cap."""
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 1024)
        req = MagicMock()
        req.headers = {}
        req.stream = lambda: _fake_stream(b'{"model":"gpt-4"}')
        body = await relay._read_body_capped(req)
        assert body == b'{"model":"gpt-4"}'

    async def test_read_body_capped_disabled(self, relay, monkeypatch):
        """MAX_BODY_SIZE=0 → body read unfiltered."""
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 0)
        req = MagicMock()
        req.body = AsyncMock(return_value=b"anything")
        assert await relay._read_body_capped(req) == b"anything"

    async def test_read_body_capped_falls_back_when_stream_unsupported(self, relay, monkeypatch):
        """Streaming raises → falls back to request.body()."""
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 1024)
        req = MagicMock()
        req.headers = {}
        req.stream = MagicMock(side_effect=RuntimeError("stream not available"))
        req.body = AsyncMock(return_value=b'{"model":"gpt-4"}')
        assert await relay._read_body_capped(req) == b'{"model":"gpt-4"}'

    async def test_read_body_capped_fallback_rejects_oversized(self, relay, monkeypatch):
        """Fallback body() path also enforces the cap."""
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 64)
        req = MagicMock()
        req.headers = {}
        req.stream = MagicMock(side_effect=RuntimeError("stream not available"))
        req.body = AsyncMock(return_value=b"x" * 500)
        assert await relay._read_body_capped(req) is None

    async def test_read_body_capped_partial_consumption_returns_none(self, relay, monkeypatch):
        """Stream error AFTER yielding chunks → None (no double-read)."""
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 1024)

        async def partial_then_fail():
            yield b"partial"
            raise RuntimeError("client disconnected mid-upload")

        req = MagicMock()
        req.headers = {}
        req.stream = partial_then_fail
        req.body = AsyncMock(side_effect=RuntimeError("Stream consumed"))
        # Must return None, NOT raise the fallback's RuntimeError
        assert await relay._read_body_capped(req) is None

    async def test_read_body_capped_both_fail_returns_none(self, relay, monkeypatch):
        """Stream AND fallback both fail → None (never raises)."""
        monkeypatch.setattr(relay, "MAX_BODY_SIZE", 1024)
        req = MagicMock()
        req.headers = {}
        req.stream = MagicMock(side_effect=RuntimeError("stream broken"))
        req.body = AsyncMock(side_effect=RuntimeError("Stream consumed"))
        assert await relay._read_body_capped(req) is None

    async def test_resize_semaphore_recreates_on_change(self, relay, monkeypatch):
        """MAX_CONCURRENT_UPSTREAM change → semaphore recreated with new bound."""
        old = relay.semaphore
        monkeypatch.setattr(relay, "MAX_CONCURRENT_UPSTREAM", 3)
        monkeypatch.setattr(relay, "_semaphore_max", 10)

        result = relay._resize_semaphore()
        assert result is True
        assert relay.semaphore is not old
        assert relay.semaphore._value == 3
        assert relay._semaphore_max == 3

    async def test_resize_semaphore_noop_when_unchanged(self, relay, monkeypatch):
        """Same MAX_CONCURRENT_UPSTREAM → semaphore untouched."""
        old = relay.semaphore
        monkeypatch.setattr(relay, "MAX_CONCURRENT_UPSTREAM", relay._semaphore_max)

        result = relay._resize_semaphore()
        assert result is False
        assert relay.semaphore is old

    async def test_reload_config_resizes_semaphore(self, relay, monkeypatch, tmp_path):
        """Hot-reload with a different concurrency limit recreates the semaphore."""
        import json as _json
        cfg_path = tmp_path / "relay-config.json"
        cfg_path.write_text(_json.dumps({
            "UPSTREAM_BASE": "https://api.example.com/v1",
            "UPSTREAM_API_KEY": "key",
            "UPSTREAM_AUTH_TYPE": "bearer",
            "MAX_CONCURRENT_UPSTREAM": 2,
            "PROXY_LIST_ENV": "socks5://u:p@h1:1080,socks5://u:p@h2:1080",
        }))
        monkeypatch.setattr(relay, "_CONFIG_PATH", str(cfg_path))
        monkeypatch.setattr(relay, "MAX_CONCURRENT_UPSTREAM", 10)
        monkeypatch.setattr(relay, "_semaphore_max", 10)
        monkeypatch.delenv("MAX_CONCURRENT_UPSTREAM", raising=False)
        monkeypatch.delenv("PROXY_LIST_ENV", raising=False)
        monkeypatch.delenv("PROXY_LIST", raising=False)
        monkeypatch.delenv("UPSTREAM_BASE", raising=False)
        monkeypatch.delenv("UPSTREAM_API_KEY", raising=False)
        monkeypatch.delenv("UPSTREAM_AUTH_TYPE", raising=False)
        monkeypatch.delenv("SEMAPHORE_WAIT_SECONDS", raising=False)

        old = relay.semaphore
        result = relay._reload_upstream_config()
        assert result["status"] == "ok"
        assert relay.semaphore is not old
        assert relay.semaphore._value == 2
        assert relay.MAX_CONCURRENT_UPSTREAM == 2

    async def test_reload_config_updates_cooldown_constants(self, relay, monkeypatch, tmp_path):
        """Hot-reload picks up CONSECUTIVE_ERROR_THRESHOLD,
        PERMANENT_COOLDOWN_SECONDS, and MAX_RETRY_AFTER_SECONDS from the
        config file (previously these were read once at startup only)."""
        import json as _json
        cfg_path = tmp_path / "relay-config.json"
        cfg_path.write_text(_json.dumps({
            "UPSTREAM_BASE": "https://api.example.com/v1",
            "UPSTREAM_API_KEY": "key",
            "UPSTREAM_AUTH_TYPE": "bearer",
            "MAX_CONCURRENT_UPSTREAM": 5,
            "CONSECUTIVE_ERROR_THRESHOLD": 7,
            "PERMANENT_COOLDOWN_SECONDS": 12345,
            "MAX_RETRY_AFTER_SECONDS": 99,
            "PROXY_LIST_ENV": "socks5://u:p@h1:1080,socks5://u:p@h2:1080",
        }))
        monkeypatch.setattr(relay, "_CONFIG_PATH", str(cfg_path))
        monkeypatch.setattr(relay, "CONSECUTIVE_ERROR_THRESHOLD", 3)
        monkeypatch.setattr(relay, "PERMANENT_COOLDOWN_SECONDS", 86400)
        monkeypatch.setattr(relay, "MAX_RETRY_AFTER_SECONDS", 3600)
        for var in ("CONSECUTIVE_ERROR_THRESHOLD", "PERMANENT_COOLDOWN_SECONDS",
                    "MAX_RETRY_AFTER_SECONDS", "MAX_CONCURRENT_UPSTREAM",
                    "PROXY_LIST_ENV", "PROXY_LIST", "UPSTREAM_BASE",
                    "UPSTREAM_API_KEY", "UPSTREAM_AUTH_TYPE", "SEMAPHORE_WAIT_SECONDS",
                    "HEALTH_FAIL_THRESHOLD", "MAX_BODY_SIZE"):
            monkeypatch.delenv(var, raising=False)

        relay._reload_upstream_config()
        assert relay.CONSECUTIVE_ERROR_THRESHOLD == 7
        assert relay.PERMANENT_COOLDOWN_SECONDS == 12345
        assert relay.MAX_RETRY_AFTER_SECONDS == 99

    async def test_reload_config_invalidates_models_cache(self, relay, monkeypatch, tmp_path):
        """Hot-reload clears the models cache — stale models must not outlive an upstream switch."""
        import json as _json
        relay._update_models_cache([{"id": "old-upstream-model"}])
        assert relay.MODELS_CACHE  # precondition: cache populated

        cfg_path = tmp_path / "relay-config.json"
        cfg_path.write_text(_json.dumps({
            "UPSTREAM_BASE": "https://new-upstream.example.com/v1",
            "UPSTREAM_API_KEY": "key",
            "UPSTREAM_AUTH_TYPE": "bearer",
            "MAX_CONCURRENT_UPSTREAM": 5,
            "PROXY_LIST_ENV": "socks5://u:p@h1:1080,socks5://u:p@h2:1080",
        }))
        monkeypatch.setattr(relay, "_CONFIG_PATH", str(cfg_path))
        monkeypatch.delenv("MAX_CONCURRENT_UPSTREAM", raising=False)
        monkeypatch.delenv("PROXY_LIST_ENV", raising=False)
        monkeypatch.delenv("PROXY_LIST", raising=False)
        monkeypatch.delenv("UPSTREAM_BASE", raising=False)
        monkeypatch.delenv("UPSTREAM_API_KEY", raising=False)
        monkeypatch.delenv("UPSTREAM_AUTH_TYPE", raising=False)
        monkeypatch.delenv("SEMAPHORE_WAIT_SECONDS", raising=False)

        result = relay._reload_upstream_config()
        assert result["status"] == "ok"
        assert relay.UPSTREAM_BASE == "https://new-upstream.example.com/v1"
        # Cache invalidated — next fetch goes to the new upstream
        assert relay.MODELS_CACHE == []
        assert relay.MODELS_CACHE_UPDATED == 0.0

    async def test_single_read_timeout_not_permanent_death(self, relay, monkeypatch):
        """Non-streaming ReadTimeout (upstream stall) cools briefly but does
        NOT increment consecutive_errors toward permanent death."""
        async def stall_single(client, method, url, headers, body, proxy_entry):
            raise httpx.ReadTimeout("upstream slow")

        async def fake_get(url, mark_in_use=False):
            return AsyncMock()

        monkeypatch.setattr(relay, "_get_client", fake_get)
        monkeypatch.setattr(relay, "_proxy_single", stall_single)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)

        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"model":"gpt-4"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 502
        assert b"upstream_timeout" in resp.body
        proxy_entry = relay.pool._proxies[0]
        assert proxy_entry.consecutive_errors == 0
        assert not proxy_entry.permanently_dead

    async def test_client_auth_lowercase_bearer(self, relay, monkeypatch):
        """`bearer <key>` (lowercase scheme) is accepted — RFC 7235 schemes
        are case-insensitive, so `Bearer ` prefix matching must be too."""
        monkeypatch.setattr(relay, "CLIENT_API_KEY", "client-secret")

        async def fake_single(client, method, url, headers, body, proxy_entry):
            from fastapi.responses import Response
            return Response(content='{"ok":true}', status_code=200)

        async def fake_get(url, mark_in_use=False):
            return AsyncMock()

        monkeypatch.setattr(relay, "_get_client", fake_get)
        monkeypatch.setattr(relay, "_proxy_single", fake_single)
        monkeypatch.setattr(relay, "MAX_REQUEST_RETRIES", 1)
        resp = await relay._proxy_request(
            "POST", "/chat/completions", b'{"model":"gpt-4"}',
            {"content-type": "application/json", "authorization": "bearer client-secret"}, "",
        )
        assert resp.status_code == 200

    async def test_proxy_stream_429_releases_semaphore(self, relay, monkeypatch):
        """_proxy_stream error path (429) releases the handed-off semaphore
        before returning the plain Response."""
        import asyncio as _asyncio

        class Resp429:
            @property
            def status_code(self):
                return 429

            @property
            def headers(self):
                return {"retry-after": "30"}

            async def aread(self):
                return b'{"error":"rate limited"}'

            async def aclose(self):
                pass

        fake_resp = Resp429()
        fake_client = MagicMock()
        fake_client.send = AsyncMock(return_value=fake_resp)
        fake_client.aclose = AsyncMock()
        fake_client.build_request = MagicMock(return_value=MagicMock())

        sem = _asyncio.Semaphore(2)
        releases = []
        sem.release = lambda: releases.append("released")

        from relay.relay import ProxyEntry as _ProxyEntry
        proxy_entry = _ProxyEntry(url=relay.pool._proxies[0].url)

        resp = await relay._proxy_stream(
            fake_client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', proxy_entry, sem,
        )
        assert resp.status_code == 429
        assert releases == ["released"]

    async def test_proxy_stream_4xx_releases_semaphore(self, relay, monkeypatch):
        """_proxy_stream error path (4xx non-429) releases the semaphore."""
        import asyncio as _asyncio

        class Resp400:
            @property
            def status_code(self):
                return 400

            @property
            def headers(self):
                return {}

            async def aread(self):
                return b'{"error":"bad request"}'

            async def aclose(self):
                pass

        fake_resp = Resp400()
        fake_client = MagicMock()
        fake_client.send = AsyncMock(return_value=fake_resp)
        fake_client.aclose = AsyncMock()
        fake_client.build_request = MagicMock(return_value=MagicMock())

        sem = _asyncio.Semaphore(2)
        releases = []
        sem.release = lambda: releases.append("released")

        from relay.relay import ProxyEntry as _ProxyEntry
        proxy_entry = _ProxyEntry(url=relay.pool._proxies[0].url)

        resp = await relay._proxy_stream(
            fake_client, "POST", "https://upstream.example.com/v1/chat/completions",
            {}, b'{"stream": true}', proxy_entry, sem,
        )
        assert resp.status_code == 400
        assert releases == ["released"]


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


class TestRequestLogging:
    """Request logging middleware — health at DEBUG, traffic at INFO."""

    @pytest.fixture
    def relay(self):
        import relay.relay as relay_mod
        return relay_mod

    async def test_logs_traffic_at_info_includes_query(self, relay, monkeypatch, caplog):
        """Non-health request → INFO log with query string."""
        import logging
        async def fake_next(request):
            from fastapi.responses import JSONResponse
            return JSONResponse({"ok": True})

        req = MagicMock()
        req.method = "POST"
        req.url.path = "/v1/chat/completions"
        req.url.query = "model=gpt-4&stream=true"

        with caplog.at_level(logging.INFO, logger="proxy-relay"):
            await relay.log_requests(req, fake_next)

        info_msgs = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("/v1/chat/completions?model=gpt-4&stream=true" in m for m in info_msgs)

    async def test_health_logged_at_debug(self, relay, monkeypatch, caplog):
        """Health poll → DEBUG log, absent from INFO."""
        import logging
        async def fake_next(request):
            from fastapi.responses import JSONResponse
            return JSONResponse({"status": "ok"})

        req = MagicMock()
        req.method = "GET"
        req.url.path = "/health"
        req.url.query = ""

        with caplog.at_level(logging.DEBUG, logger="proxy-relay"):
            await relay.log_requests(req, fake_next)

        info_msgs = [r.message for r in caplog.records if r.levelno == logging.INFO]
        debug_msgs = [r.message for r in caplog.records if r.levelno == logging.DEBUG]
        assert not any("/health" in m for m in info_msgs)
        assert any("/health" in m for m in debug_msgs)

    def test_redact_query_hides_credentials(self, relay):
        """api_key/token values are redacted; benign params preserved."""
        redacted = relay._redact_query("model=gpt-4&api_key=sk-secret123&stream=true")
        assert "sk-secret123" not in redacted
        assert "api_key=***" in redacted
        assert "model=gpt-4" in redacted
        assert "stream=true" in redacted

    def test_redact_query_case_insensitive(self, relay):
        """Param names matched case-insensitively."""
        redacted = relay._redact_query("API_KEY=abc&Token=def")
        assert "abc" not in redacted
        assert "def" not in redacted

    def test_redact_query_empty_and_plain(self, relay):
        """Empty query and query without secrets pass through unchanged."""
        assert relay._redact_query("") == ""
        assert relay._redact_query("model=gpt-4&stream=true") == "model=gpt-4&stream=true"

    def test_redact_query_encoded_and_dashed_variants(self, relay):
        """Percent-encoded (`api%5Fkey`), dashed (`api-key`) and x-api-key
        param names must be redacted too — a secret value must not leak
        just because the name wasn't the literal `api_key`."""
        redacted = relay._redact_query("api%5Fkey=sk-enc123")
        assert "sk-enc123" not in redacted
        assert "api%5Fkey=***" in redacted

        redacted2 = relay._redact_query("api-key=sk-dash456")
        assert "sk-dash456" not in redacted2
        assert "api-key=***" in redacted2

        redacted3 = relay._redact_query("x-api-key=sk-x789&client_secret=s3cret")
        assert "sk-x789" not in redacted3
        assert "s3cret" not in redacted3

    async def test_log_redacts_credential_query(self, relay, monkeypatch, caplog):
        """Credential query params never reach the log."""
        import logging
        async def fake_next(request):
            from fastapi.responses import JSONResponse
            return JSONResponse({"ok": True})

        req = MagicMock()
        req.method = "GET"
        req.url.path = "/v1/chat/completions"
        req.url.query = "api_key=supersecret123"

        with caplog.at_level(logging.INFO, logger="proxy-relay"):
            await relay.log_requests(req, fake_next)

        msgs = " ".join(r.message for r in caplog.records)
        assert "supersecret123" not in msgs
        assert "api_key=***" in msgs
