"""Scaling / performance tests for v1.6.0.

Covers the bottleneck-pass changes:
- bounded semaphore backlog (`MAX_QUEUED_REQUESTS`) in _acquire_semaphore
- pooled streaming clients (_make_streaming_client / _proxy_stream borrow
  lifecycle, never-started generator finalizer)
- HOLD_PERMIT_FOR_STREAM=false permit release at connection setup
- stream auth-switch retry (the 401 → probe → re-borrow → retry path)
- windowed byte-scan stream detection for large bodies
- parallel health-check sweep (gather + bounded concurrency)
- RELAY_WORKERS / _run_config_check new knobs
- v1.5.0 coverage gap: AuthSwitcher disabled-probe, probe read-timeout,
  state-persistence failure, load_state-no-path, admin-health bearer branch
"""

import asyncio
import gc
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import Response


def make_client(handler) -> httpx.AsyncClient:
    """Build an AsyncClient backed by a MockTransport handler."""
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
    relay_mod._admin_rate_hits.clear()
    relay_mod._request_count["total"] = 0
    relay_mod._request_count["ok"] = 0
    relay_mod._request_count["errors"] = 0
    relay_mod._request_count["auth_failed"] = 0
    relay_mod._client_in_use.clear()
    relay_mod._client_last_used.clear()
    relay_mod._waiting_count = 0
    # TestClient teardown (lifespan shutdown) leaves the module-global
    # stream-shutdown event set — a stream test must never inherit it.
    relay_mod._stream_shutdown_event.clear()
    relay_mod.auth_switcher.reset()
    relay_mod.auth_switcher.enabled = True
    relay_mod.HOLD_PERMIT_FOR_STREAM = True
    relay_mod.MAX_QUEUED_REQUESTS = 100
    relay_mod.HEALTH_CHECK_CONCURRENCY = 20
    relay_mod.RETRY_BACKOFF_BASE = 0.0
    relay_mod.RETRY_BACKOFF_MAX = 1.0
    relay_mod.RETRY_SEMAPHORE_WAIT_SECONDS = 2.0
    relay_mod.LATENCY_SKIP_THRESHOLD_MS = 0.0
    relay_mod.CLIENT_IDLE_TTL = 0.0
    relay_mod.MAX_RESPONSE_SIZE = 200 * 1024 * 1024
    relay_mod.UPSTREAM_CONNECT_TIMEOUT = 15.0
    relay_mod.UPSTREAM_READ_TIMEOUT = 120.0
    relay_mod.RELAY_LOG_REQUESTS = True
    # Fresh semaphore per test — the module-global binds to the first loop.
    relay_mod.semaphore = asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
    return relay_mod


# ═══════════════════════════════════════════════════════════════════
#  Bounded semaphore backlog
# ═══════════════════════════════════════════════════════════════════


class TestBoundedBacklog:
    async def test_fails_fast_when_backlog_full(self, relay_mod, fresh_pool, caplog):
        """MAX_QUEUED_REQUESTS reached → fail fast (None → 503) instead of queueing."""
        caplog.set_level("WARNING")
        relay_mod.MAX_QUEUED_REQUESTS = 2
        relay_mod._waiting_count = 2  # pretend 2 requests already queued
        got = await relay_mod._acquire_semaphore(timeout=0.01)
        assert got is None
        assert any("backlog full" in r.message for r in caplog.records)

    async def test_acquires_when_under_backlog_and_restores_count(self, relay_mod, fresh_pool):
        """Under the cap the wait proceeds and _waiting_count returns to 0."""
        relay_mod.MAX_QUEUED_REQUESTS = 5
        relay_mod._waiting_count = 0
        sem = await relay_mod._acquire_semaphore(timeout=None)
        assert sem is not None
        assert relay_mod._waiting_count == 0  # finally decremented
        sem.release()

    async def test_zero_means_unlimited(self, relay_mod, fresh_pool):
        """MAX_QUEUED_REQUESTS=0 restores the old unlimited behavior."""
        relay_mod.MAX_QUEUED_REQUESTS = 0
        relay_mod._waiting_count = 999
        sem = await relay_mod._acquire_semaphore(timeout=None)
        assert sem is not None
        assert relay_mod._waiting_count == 999
        sem.release()


# ═══════════════════════════════════════════════════════════════════
#  Pooled streaming clients
# ═══════════════════════════════════════════════════════════════════


class TestStreamClientPooling:
    async def test_proxy_stream_releases_pooled_client_after_body(self, relay_mod, fresh_pool):
        """A streamed response releases the borrow at stream end; the pooled
        client stays in the pool (NOT closed per stream)."""
        entry = relay_mod.pool.next()
        assert entry is not None

        def handler(request):
            return httpx.Response(
                200, content=b"data: hello\n\n",
                headers={"content-type": "text/event-stream"},
            )

        client = make_client(handler)
        relay_mod._client_pool[entry.url] = client
        relay_mod._client_in_use[entry.url] = 1

        # The caller acquires the permit BEFORE handing it to _proxy_stream;
        # mirror that so a release (back to 5) is a real release, not an
        # over-credit.
        sem = asyncio.Semaphore(5)
        await sem.acquire()
        resp = await relay_mod._proxy_stream(
            client, "POST", "https://up.example.com/v1/chat/completions",
            {}, b"{}", entry, sem,
        )
        assert resp.status_code == 200
        # Default HOLD_PERMIT_FOR_STREAM=True: permit held at response creation.
        assert sem._value == 4
        assert relay_mod._client_in_use.get(entry.url, 0) == 1
        # Stream the body to completion.
        chunks = b"".join([c async for c in resp.body_iterator])
        assert b"hello" in chunks
        # Borrow released, client still pooled, permit released.
        assert relay_mod._client_in_use.get(entry.url, 0) == 0
        assert entry.url in relay_mod._client_pool
        assert sem._value == 5
        await relay_mod._close_all_clients()

    async def test_proxy_stream_never_started_generator_releases(self, relay_mod, fresh_pool):
        """Client disconnect BEFORE the generator starts: the GC finalizer
        must release BOTH the borrow and the semaphore permit."""
        entry = relay_mod.pool.next()
        assert entry is not None

        def handler(request):
            return httpx.Response(
                200, content=b"data: x\n\n",
                headers={"content-type": "text/event-stream"},
            )

        client = make_client(handler)
        relay_mod._client_pool[entry.url] = client
        relay_mod._client_in_use[entry.url] = 1

        sem = asyncio.Semaphore(5)
        await sem.acquire()
        resp = await relay_mod._proxy_stream(
            client, "POST", "https://up.example.com/v1/chat/completions",
            {}, b"{}", entry, sem,
        )
        assert sem._value == 4
        assert relay_mod._client_in_use.get(entry.url, 0) == 1
        # Drop the response WITHOUT iterating — the generator's finally never
        # runs; the weakref finalizer must clean up both resources.
        del resp
        gc.collect()
        await asyncio.sleep(0)  # let the scheduled agen aclose run
        assert sem._value == 5
        assert relay_mod._client_in_use.get(entry.url, 0) == 0
        await relay_mod._close_all_clients()

    async def test_release_client_in_use_noop_when_not_borrowed(self, relay_mod):
        """Releasing a URL that was never borrowed is a safe no-op."""
        relay_mod._client_in_use.clear()
        relay_mod._release_client_in_use("socks5://u:p@nope:1080")
        assert relay_mod._client_in_use == {}


# ═══════════════════════════════════════════════════════════════════
#  HOLD_PERMIT_FOR_STREAM
# ═══════════════════════════════════════════════════════════════════


class TestHoldPermitForStream:
    async def test_false_releases_permit_at_response_creation(self, relay_mod, fresh_pool):
        """HOLD_PERMIT_FOR_STREAM=false: the permit only gates connection
        setup; it is released before the body streams and never double-
        released by the generator."""
        relay_mod.HOLD_PERMIT_FOR_STREAM = False
        entry = relay_mod.pool.next()
        assert entry is not None

        def handler(request):
            return httpx.Response(
                200, content=b"data: x\n\n",
                headers={"content-type": "text/event-stream"},
            )

        client = make_client(handler)
        relay_mod._client_pool[entry.url] = client
        relay_mod._client_in_use[entry.url] = 1

        sem = asyncio.Semaphore(5)
        await sem.acquire()
        resp = await relay_mod._proxy_stream(
            client, "POST", "https://up.example.com/v1/chat/completions",
            {}, b"{}", entry, sem,
        )
        # Permit released BEFORE the body streams.
        assert sem._value == 5
        # Iterating the stream must NOT double-release (would over-credit).
        chunks = b"".join([c async for c in resp.body_iterator])
        assert b"x" in chunks
        assert sem._value == 5
        assert relay_mod._client_in_use.get(entry.url, 0) == 0
        await relay_mod._close_all_clients()


# ═══════════════════════════════════════════════════════════════════
#  Stream auth-switch retry
# ═══════════════════════════════════════════════════════════════════


class TestStreamAuthSwitchRetry:
    async def test_stream_401_switch_retries_with_new_auth(self, relay_mod, fresh_pool, monkeypatch):
        """Streaming request: 401 → probe → switch → re-borrow → retry → 200."""
        body = b'{"model":"m1","messages":[{"role":"user","content":"hi"}],"stream":true}'
        relay_mod.auth_switcher._consecutive_401 = relay_mod.auth_switcher.trigger_threshold
        relay_mod.auth_switcher._last_probe_ts = 0.0
        relay_mod.auth_switcher.cooldown_s = 0
        monkeypatch.setattr(relay_mod.auth_switcher, "probe_and_switch", AsyncMock(return_value=True))

        mock_client = AsyncMock()
        calls = {"n": 0}

        async def fake_proxy_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return Response(status_code=401, content=b'{"error":"auth"}')
            sr = MagicMock()
            sr.status_code = 200
            sr.headers = {"content-type": "text/event-stream"}
            return sr

        monkeypatch.setattr(relay_mod, "_proxy_stream", fake_proxy_stream)
        monkeypatch.setattr(relay_mod, "_make_streaming_client", AsyncMock(return_value=mock_client))

        resp = await relay_mod._proxy_request(
            "POST", "/chat/completions", body,
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 200
        assert calls["n"] == 4  # stream auth switch retry sweeps across proxies

    async def test_stream_401_switch_semaphore_busy_not_retried(self, relay_mod, fresh_pool, monkeypatch, caplog):
        """Auth switched but no semaphore slot free → retry skipped with a warning."""
        caplog.set_level("WARNING")
        body = b'{"model":"m1","messages":[{"role":"user","content":"hi"}],"stream":true}'
        relay_mod.auth_switcher._consecutive_401 = relay_mod.auth_switcher.trigger_threshold
        relay_mod.auth_switcher._last_probe_ts = 0.0
        relay_mod.auth_switcher.cooldown_s = 0
        monkeypatch.setattr(relay_mod.auth_switcher, "probe_and_switch", AsyncMock(return_value=True))

        mock_client = AsyncMock()
        calls = {"n": 0}

        async def fake_proxy_stream(client, method, url, headers, body, proxy_entry, acquired_sem=None):
            calls["n"] += 1
            return Response(status_code=401, content=b'{"error":"auth"}')

        monkeypatch.setattr(relay_mod, "_proxy_stream", fake_proxy_stream)
        monkeypatch.setattr(relay_mod, "_make_streaming_client", AsyncMock(return_value=mock_client))

        real_acquire = relay_mod._acquire_semaphore
        acquire_calls = {"n": 0}

        async def fake_acquire(timeout=None):
            acquire_calls["n"] += 1
            if acquire_calls["n"] >= 2:
                return None  # retry slot unavailable
            return await real_acquire(timeout)

        monkeypatch.setattr(relay_mod, "_acquire_semaphore", fake_acquire)

        resp = await relay_mod._proxy_request(
            "POST", "/chat/completions", body,
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 401  # original rejection returned, no retry
        assert calls["n"] == 1
        assert any("semaphore busy" in r.message for r in caplog.records)


# ═══════════════════════════════════════════════════════════════════
#  Stream detection — windowed byte scan for large bodies
# ═══════════════════════════════════════════════════════════════════


class TestStreamDetectionWindowedScan:
    """Bodies over the JSON-parse limit (>256KB) use the windowed byte scan."""

    @staticmethod
    def _large(prefix: bytes, suffix: bytes) -> bytes:
        return prefix + b"x" * 300000 + suffix

    def test_second_occurrence_matches(self, relay_mod):
        """First `"stream"` key is false; a LATER key is true → detected."""
        body = self._large(b'{"stream": false, "meta": {', b'"stream": true}}')
        assert len(body) > 256 * 1024
        assert relay_mod._detect_stream_request(body) is True

    def test_key_present_but_never_true(self, relay_mod):
        """`"stream"` keys exist but never `: true` → not a stream request."""
        body = self._large(b'{"stream": false, "meta": {', b'"stream": "no"}}')
        assert relay_mod._detect_stream_request(body) is False

    def test_no_lowercase_key_uppercase_fallback(self, relay_mod):
        """No lowercase key → IGNORECASE fallback still catches "STREAM": true."""
        body = self._large(b'{"STREAM": true, "meta": {', b"}}")
        assert relay_mod._detect_stream_request(body) is True


# ═══════════════════════════════════════════════════════════════════
#  Admin upstream health — bearer header branch
# ═══════════════════════════════════════════════════════════════════


class TestAdminUpstreamHealthBearer:
    async def test_bearer_health_ok(self, relay_mod, fresh_pool, monkeypatch):
        """admin_upstream_health with bearer auth (the else branch)."""
        monkeypatch.setattr(relay_mod, "UPSTREAM_AUTH_TYPE", "bearer")

        def handler(request):
            return httpx.Response(200, json={"data": [{"id": "m1"}]})

        mock_client = make_client(handler)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            req = MagicMock()
            req.client.host = "127.0.0.1"
            data = await relay_mod.admin_upstream_health(req)

        assert data["status"] == "ok"
        assert data["models_count"] == 1


# ═══════════════════════════════════════════════════════════════════
#  AuthSwitcher branch coverage (v1.5.0 gap)
# ═══════════════════════════════════════════════════════════════════


class TestAuthSwitcherBranches:
    async def test_probe_and_switch_disabled(self, relay_mod):
        """probe_and_switch() with the switcher disabled returns False."""
        relay_mod.auth_switcher.enabled = False
        assert await relay_mod.auth_switcher.probe_and_switch() is False

    async def test_probe_auth_read_timeout_inconclusive(self, relay_mod, fresh_pool, monkeypatch):
        """A probe that stalls (ReadTimeout) is NOT an auth signal."""
        relay_mod.UPSTREAM_BASE = "https://up.example.com/v1"

        @asynccontextmanager
        async def borrow(url):
            client = AsyncMock()
            client.request.side_effect = httpx.ReadTimeout("stalled")
            yield client

        monkeypatch.setattr(relay_mod, "_borrow_client", borrow)
        result = await relay_mod.auth_switcher._probe_auth("x-api-key")
        assert result == "inconclusive"

    async def test_save_state_failure_warns(self, relay_mod, caplog, tmp_path):
        """A failed persist (unwritable state path) warns but never crashes."""
        caplog.set_level("WARNING")
        blocker = tmp_path / "blocker"
        blocker.write_text("i am a file, not a dir")
        relay_mod.auth_switcher.state_path = str(blocker / "state.json")
        relay_mod.auth_switcher._switch_history.append({"ts": "now"})
        relay_mod.auth_switcher._save_state()
        assert any("failed to persist auth state" in r.message for r in caplog.records)

    def test_load_state_without_path_returns_none(self, relay_mod):
        """load_state() with no state path configured returns None."""
        old = relay_mod.auth_switcher.state_path
        relay_mod.auth_switcher.state_path = ""
        try:
            assert relay_mod.auth_switcher.load_state() is None
        finally:
            relay_mod.auth_switcher.state_path = old


# ═══════════════════════════════════════════════════════════════════
#  Health sweep — all-success resets failure counters
# ═══════════════════════════════════════════════════════════════════


class TestHealthCheckAllSuccessReset:
    async def test_all_success_after_partial_failure_resets_counters(self, relay_mod, fresh_pool, monkeypatch):
        """Sweep 1: one proxy fails, others succeed (counter=1, not dead).
        Sweep 2: ALL succeed → the per-proxy failure counter resets."""
        monkeypatch.setattr(relay_mod, "PROXY_HEALTH_CHECK_INTERVAL", 0.01)
        monkeypatch.setattr(relay_mod, "HEALTH_FAIL_THRESHOLD", 3)
        relay_mod.HEALTH_CHECK_CONCURRENCY = 20

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

        with patch.object(relay_mod.httpx, "AsyncClient") as mock_ctor:
            # Sweep 1: entry fails, others succeed. Sweep 2+: ALL succeed.
            mock_ctor.side_effect = [fail_client, success_client, success_client] + [success_client] * 12
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.30)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert not entry.permanently_dead
        assert entry.consecutive_errors == 0


# ═══════════════════════════════════════════════════════════════════
#  Config check + RELAY_WORKERS
# ═══════════════════════════════════════════════════════════════════


class TestConfigCheckNewKnobs:
    def test_rejects_invalid_new_knobs(self, relay_mod, monkeypatch, capsys):
        """_run_config_check errors on the new invalid knobs."""
        monkeypatch.setattr(relay_mod, "MAX_QUEUED_REQUESTS", -1)
        monkeypatch.setattr(relay_mod, "HEALTH_CHECK_CONCURRENCY", 0)
        monkeypatch.setattr(relay_mod, "RELAY_WORKERS", 0)
        with pytest.raises(SystemExit) as ei:
            relay_mod._run_config_check()
        assert ei.value.code == 1
        out = capsys.readouterr().out
        assert "Invalid MAX_QUEUED_REQUESTS" in out
        assert "Invalid HEALTH_CHECK_CONCURRENCY" in out
        assert "Invalid RELAY_WORKERS" in out


class TestRelayWorkers:
    def test_main_workers_gt_one_warns_and_passes_workers(self, relay_mod, monkeypatch, caplog):
        """RELAY_WORKERS>1 logs the per-worker-state warning and passes
        workers=N to uvicorn.run (and skips the single-process signal
        handlers)."""
        caplog.set_level("WARNING")
        monkeypatch.setattr(relay_mod, "RELAY_WORKERS", 4)

        import sys as _sys
        mock_uvicorn = MagicMock()
        _sys.modules["uvicorn"] = mock_uvicorn
        try:
            with patch.object(relay_mod.sys, "argv", ["relay.py"]):
                relay_mod.main()
        finally:
            _sys.modules.pop("uvicorn", None)

        assert mock_uvicorn.run.call_count == 1
        assert mock_uvicorn.run.call_args.kwargs["workers"] == 4
        assert any("RELAY_WORKERS=4" in r.message for r in caplog.records)
