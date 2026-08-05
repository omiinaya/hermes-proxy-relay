"""Resilience tests for v1.7.0 — the full audit-pass fixes.

Covers:
- CLIENT_IDLE_TTL stale-keep-alive reaping (_reap_stale_clients_locked)
- latency-aware proxy selection (pool._maybe_skip_slow)
- health sweep skips fully-healthy pools (no upstream load when healthy)
- AuthSwitcher probe semaphore gate (busy → inconclusive, no borrow)
- MAX_RESPONSE_SIZE cap in _proxy_single (runaway upstream protection)
- retry exponential backoff (RETRY_BACKOFF_BASE/MAX)
- models refresh: non-200 breaks without retry; connect error retries next proxy
- config-check validation of the new knobs + socks5h recommendation
- uvicorn inbound connection caps (RELAY_MAX_CONNECTIONS / RELAY_BACKLOG)
"""

import asyncio
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest


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
    relay_mod.MAX_REQUEST_RETRIES = 3
    relay_mod.semaphore = asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
    return relay_mod


# ═══════════════════════════════════════════════════════════════════
#  CLIENT_IDLE_TTL — stale-keep-alive reaping
# ═══════════════════════════════════════════════════════════════════


class TestIdleReaping:
    async def test_reaps_stale_idle_client_on_borrow(self, relay_mod, fresh_pool):
        """A pooled client idle > CLIENT_IDLE_TTL is closed BEFORE reuse."""
        relay_mod.CLIENT_IDLE_TTL = 10.0
        relay_mod._client_pool.clear()
        relay_mod._client_last_used.clear()

        await relay_mod._get_client("socks5://stale@1:1080")
        # Backdate the stamp so it looks stale.
        relay_mod._client_last_used["socks5://stale@1:1080"] = time.monotonic() - 100

        await relay_mod._get_client("socks5://fresh@2:1080")
        # The stale client was reaped (removed from the pool and closed).
        assert "socks5://stale@1:1080" not in relay_mod._client_pool
        assert "socks5://stale@1:1080" not in relay_mod._client_last_used
        assert "socks5://fresh@2:1080" in relay_mod._client_pool
        await relay_mod._close_all_clients()

    async def test_does_not_reap_fresh_or_in_use(self, relay_mod, fresh_pool):
        """Fresh clients and in-use clients survive the reap scan."""
        relay_mod.CLIENT_IDLE_TTL = 10.0
        relay_mod._client_pool.clear()
        relay_mod._client_last_used.clear()

        await relay_mod._get_client("socks5://a@1:1080")
        await relay_mod._get_client("socks5://b@2:1080")
        relay_mod._client_in_use["socks5://b@2:1080"] = 1
        # Make A stale but B in-use (in-use protects it).
        relay_mod._client_last_used["socks5://a@1:1080"] = time.monotonic() - 100

        # Reaping A: A is stale+idle → reaped. B in-use → kept.
        await relay_mod._get_client("socks5://c@3:1080")
        assert "socks5://a@1:1080" not in relay_mod._client_pool
        assert "socks5://b@2:1080" in relay_mod._client_pool
        # Fresh client (recently borrowed) survives.
        assert "socks5://c@3:1080" in relay_mod._client_pool
        relay_mod._client_in_use.clear()
        await relay_mod._close_all_clients()

    async def test_reap_disabled_when_ttl_zero(self, relay_mod, fresh_pool):
        """CLIENT_IDLE_TTL=0 (default) disables age-based reaping."""
        relay_mod.CLIENT_IDLE_TTL = 0.0
        relay_mod._client_pool.clear()
        relay_mod._client_last_used.clear()

        await relay_mod._get_client("socks5://old@1:1080")
        relay_mod._client_last_used["socks5://old@1:1080"] = time.monotonic() - 100000

        await relay_mod._get_client("socks5://new@2:1080")
        assert "socks5://old@1:1080" in relay_mod._client_pool  # NOT reaped
        await relay_mod._close_all_clients()


# ═══════════════════════════════════════════════════════════════════
#  Latency-aware proxy selection
# ═══════════════════════════════════════════════════════════════════


class TestLatencyAwareSelection:
    def test_skips_slow_proxy_when_faster_available(self, relay_mod, monkeypatch):
        monkeypatch.setattr(relay_mod, "LATENCY_SKIP_THRESHOLD_MS", 500)
        pool = relay_mod.CooldownPool([
            "socks5://slow@1:1", "socks5://fast@2:2", "socks5://unknown@3:3",
        ])
        slow, fast, unknown = pool._proxies
        slow.latency_samples = 10
        slow.avg_latency_ms = 2000
        fast.latency_samples = 10
        fast.avg_latency_ms = 100

        got = pool.next()
        assert got is fast  # slow was skipped for the faster available proxy

    def test_falls_back_to_slow_when_no_faster(self, relay_mod, monkeypatch):
        monkeypatch.setattr(relay_mod, "LATENCY_SKIP_THRESHOLD_MS", 500)
        pool = relay_mod.CooldownPool([
            "socks5://a@1:1", "socks5://b@2:2", "socks5://c@3:3",
        ])
        for e in pool._proxies:
            e.latency_samples = 5
            e.avg_latency_ms = 3000  # all slow

        got = pool.next()
        assert got is pool._proxies[0]  # round-robin candidate served anyway

    def test_unknown_latency_counts_as_fast(self, relay_mod, monkeypatch):
        monkeypatch.setattr(relay_mod, "LATENCY_SKIP_THRESHOLD_MS", 500)
        pool = relay_mod.CooldownPool([
            "socks5://slow@1:1", "socks5://untested@2:2",
        ])
        pool._proxies[0].latency_samples = 5
        pool._proxies[0].avg_latency_ms = 3000
        # second proxy has NO latency data → treated as fast → chosen
        got = pool.next()
        assert got is pool._proxies[1]

    def test_fast_candidate_not_skipped(self, relay_mod, monkeypatch):
        """A candidate already under the threshold is served directly."""
        monkeypatch.setattr(relay_mod, "LATENCY_SKIP_THRESHOLD_MS", 500)
        pool = relay_mod.CooldownPool(["socks5://fast@1:1"])
        pool._proxies[0].latency_samples = 5
        pool._proxies[0].avg_latency_ms = 100
        got = pool.next()
        assert got is pool._proxies[0]


# ═══════════════════════════════════════════════════════════════════
#  Health sweep — skip fully-healthy pools
# ═══════════════════════════════════════════════════════════════════


class TestHealthSweepSkipsHealthyPool:
    async def test_no_probes_when_pool_fully_healthy(self, relay_mod, fresh_pool, monkeypatch):
        """A pool where every proxy has succeeded, is not cooling and is not
        dead triggers NO upstream probes — zero health-check load when healthy."""
        monkeypatch.setattr(relay_mod, "PROXY_HEALTH_CHECK_INTERVAL", 0.01)
        for e in relay_mod.pool._proxies:
            e.total_ok = 1  # proven working

        with patch.object(relay_mod.httpx, "AsyncClient") as mock_ctor:
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        mock_ctor.assert_not_called()


# ═══════════════════════════════════════════════════════════════════
#  AuthSwitcher probe semaphore gate
# ═══════════════════════════════════════════════════════════════════


class TestProbeSemaphoreGate:
    async def test_probe_inconclusive_when_semaphore_busy(self, relay_mod, fresh_pool, monkeypatch):
        """A probe deferred because the relay is at capacity is 'inconclusive'
        (never an auth signal) and never borrows a client."""
        relay_mod.UPSTREAM_BASE = "https://up.example.com/v1"
        relay_mod.RETRY_SEMAPHORE_WAIT_SECONDS = 0.01  # fast gate timeout

        acquired = []
        while relay_mod.semaphore._value > 0:
            acquired.append(await relay_mod.semaphore.acquire())
        try:
            borrows = {"n": 0}

            @asynccontextmanager
            async def borrow(url):
                borrows["n"] += 1
                yield AsyncMock()

            monkeypatch.setattr(relay_mod, "_borrow_client", borrow)
            result = await relay_mod.auth_switcher._probe_auth("x-api-key")
            assert result == "inconclusive"
            assert borrows["n"] == 0  # gate blocked before any borrow
        finally:
            for s in acquired:
                relay_mod.semaphore.release()


# ═══════════════════════════════════════════════════════════════════
#  MAX_RESPONSE_SIZE — runaway upstream protection
# ═══════════════════════════════════════════════════════════════════


class TestMaxResponseSize:
    async def test_proxy_single_aborts_oversized_response(self, relay_mod, fresh_pool):
        """A response exceeding MAX_RESPONSE_SIZE is aborted with a 502
        response_too_large and the proxy gets a transient cooldown."""
        relay_mod.MAX_RESPONSE_SIZE = 10

        def handler(request):
            return httpx.Response(200, content=b"x" * 50)

        client = make_client(handler)
        proxy_entry = relay_mod.pool.next()
        assert proxy_entry is not None

        resp = await relay_mod._proxy_single(
            client, "GET", "https://up.example.com/v1/x", {}, None, proxy_entry,
        )
        assert resp.status_code == 502
        assert b"response_too_large" in resp.body
        # transient cooldown recorded (not permanent death)
        assert proxy_entry.cooldown_until > time.monotonic()
        assert not proxy_entry.permanently_dead

    async def test_proxy_single_normal_response_unchanged(self, relay_mod, fresh_pool):
        """Under the cap the response relays normally."""
        relay_mod.MAX_RESPONSE_SIZE = 1024

        def handler(request):
            return httpx.Response(200, content=b"ok", headers={"x-request-id": "abc"})

        client = make_client(handler)
        proxy_entry = relay_mod.pool.next()

        resp = await relay_mod._proxy_single(
            client, "GET", "https://up.example.com/v1/x", {}, None, proxy_entry,
        )
        assert resp.status_code == 200
        assert resp.body == b"ok"
        assert resp.headers.get("x-request-id") == "abc"
        assert proxy_entry.total_ok == 1


# ═══════════════════════════════════════════════════════════════════
#  Retry exponential backoff
# ═══════════════════════════════════════════════════════════════════


class TestRetryBackoff:
    async def test_backoff_between_retry_attempts(self, relay_mod, fresh_pool, monkeypatch):
        """RETRY_BACKOFF_BASE: exponential sleep between retries (0.1, 0.2)."""
        relay_mod.RETRY_BACKOFF_BASE = 0.1
        relay_mod.RETRY_BACKOFF_MAX = 1.0

        sleeps = []
        real_sleep = relay_mod.asyncio.sleep

        async def fake_sleep(delay):
            sleeps.append(delay)
            await real_sleep(0)

        monkeypatch.setattr(relay_mod.asyncio, "sleep", fake_sleep)

        @asynccontextmanager
        async def borrow(url):
            yield AsyncMock()

        monkeypatch.setattr(relay_mod, "_borrow_client", borrow)

        results = iter([httpx.Response(500, json={"error": "boom"}),
                        httpx.Response(500, json={"error": "boom"}),
                        httpx.Response(200, json={"ok": True})])

        async def fake_single(client, method, url, headers, body, proxy_entry, probe=False):
            return next(results)

        monkeypatch.setattr(relay_mod, "_proxy_single", fake_single)

        resp = await relay_mod._proxy_request(
            "POST", "/chat/completions", b'{"model":"m1"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 200
        # attempt 1: no backoff; attempt 2: base×2^0 = 0.1; attempt 3: base×2^1 = 0.2
        assert sleeps == [0.1, 0.2]

    async def test_no_backoff_when_disabled(self, relay_mod, fresh_pool, monkeypatch):
        """RETRY_BACKOFF_BASE=0 → immediate retries (no sleeps)."""
        relay_mod.RETRY_BACKOFF_BASE = 0.0

        sleeps = []
        real_sleep = relay_mod.asyncio.sleep

        async def fake_sleep(delay):
            sleeps.append(delay)
            await real_sleep(0)

        monkeypatch.setattr(relay_mod.asyncio, "sleep", fake_sleep)

        @asynccontextmanager
        async def borrow(url):
            yield AsyncMock()

        monkeypatch.setattr(relay_mod, "_borrow_client", borrow)

        results = iter([httpx.Response(500, json={"error": "boom"}),
                        httpx.Response(200, json={"ok": True})])

        async def fake_single(client, method, url, headers, body, proxy_entry, probe=False):
            return next(results)

        monkeypatch.setattr(relay_mod, "_proxy_single", fake_single)

        resp = await relay_mod._proxy_request(
            "POST", "/chat/completions", b'{"model":"m1"}',
            {"content-type": "application/json"}, "",
        )
        assert resp.status_code == 200
        assert sleeps == []


# ═══════════════════════════════════════════════════════════════════
#  Models refresh — retry only on connect errors
# ═══════════════════════════════════════════════════════════════════


class TestModelsRefreshRetry:
    async def test_non_200_breaks_without_retry(self, relay_mod, fresh_pool):
        """A 5xx models refresh serves the cache after ONE attempt — retrying
        won't change the status, so no wasted proxy hops."""
        relay_mod.MODELS_CACHE.clear()
        relay_mod.MODELS_CACHE_UPDATED = 0.0
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(500, json={"error": "boom"})

        mock_client = make_client(handler)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            req = MagicMock()
            req.client.host = "127.0.0.1"
            data = await relay_mod.list_models(req)

        assert data["data"] == []  # served cache
        assert calls["n"] == 1  # exactly one proxy attempt

    async def test_connect_error_retries_next_proxy(self, relay_mod, fresh_pool, monkeypatch):
        """A connect failure on the first proxy retries through the next one."""
        relay_mod.MODELS_CACHE.clear()
        relay_mod.MODELS_CACHE_UPDATED = 0.0

        # First attempt raises ConnectError; the retry uses the real
        # _proxy_single path through a MockTransport client.
        attempts = {"n": 0}

        def handler(request):
            return httpx.Response(200, json={"data": [{"id": "m1"}]})

        mock_client = make_client(handler)

        async def fake_get(url, mark_in_use=False):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise httpx.ConnectError("refused")
            return mock_client

        monkeypatch.setattr(relay_mod, "_get_client", fake_get)

        req = MagicMock()
        req.client.host = "127.0.0.1"
        data = await relay_mod.list_models(req)

        assert [m["id"] for m in data["data"]] == ["m1"]
        assert attempts["n"] == 2  # first proxy failed, second succeeded


# ═══════════════════════════════════════════════════════════════════
#  Config check — new knobs + socks5h recommendation
# ═══════════════════════════════════════════════════════════════════


class TestConfigCheckResilienceKnobs:
    def test_rejects_invalid_new_knobs(self, relay_mod, monkeypatch, capsys):
        monkeypatch.setattr(relay_mod, "UPSTREAM_CONNECT_TIMEOUT", 0)
        monkeypatch.setattr(relay_mod, "MAX_RESPONSE_SIZE", -1)
        monkeypatch.setattr(relay_mod, "CLIENT_IDLE_TTL", -5)
        with pytest.raises(SystemExit) as ei:
            relay_mod._run_config_check()
        assert ei.value.code == 1
        out = capsys.readouterr().out
        assert "Invalid UPSTREAM_CONNECT_TIMEOUT" in out
        assert "Invalid MAX_RESPONSE_SIZE" in out
        assert "Invalid CLIENT_IDLE_TTL" in out

    def test_socks5_recommendation_warning(self, relay_mod, fresh_pool, capsys):
        """socks5:// URLs (local DNS) get a recommendation to use socks5h://
        (a warning — config still checks out OK)."""
        relay_mod._run_config_check()  # no SystemExit (warning, not error)
        out = capsys.readouterr().out
        assert "socks5h://" in out  # recommendation text present


# ═══════════════════════════════════════════════════════════════════
#  uvicorn inbound connection caps
# ═══════════════════════════════════════════════════════════════════


class TestUvicornInboundCaps:
    def test_main_passes_inbound_limits(self, relay_mod, monkeypatch):
        monkeypatch.setattr(relay_mod, "RELAY_MAX_CONNECTIONS", 100)
        monkeypatch.setattr(relay_mod, "RELAY_BACKLOG", 512)
        monkeypatch.setattr(relay_mod, "RELAY_WORKERS", 1)

        import sys as _sys
        mock_uvicorn = MagicMock()
        _sys.modules["uvicorn"] = mock_uvicorn
        try:
            with patch.object(relay_mod.sys, "argv", ["relay.py"]):
                relay_mod.main()
        finally:
            _sys.modules.pop("uvicorn", None)

        kwargs = mock_uvicorn.run.call_args.kwargs
        assert kwargs["limit_concurrency"] == 100
        assert kwargs["backlog"] == 512

    def test_main_defaults_inbound_limits(self, relay_mod, monkeypatch):
        """Defaults (0) map to uvicorn's own defaults."""
        monkeypatch.setattr(relay_mod, "RELAY_MAX_CONNECTIONS", 0)
        monkeypatch.setattr(relay_mod, "RELAY_BACKLOG", 0)
        monkeypatch.setattr(relay_mod, "RELAY_WORKERS", 1)

        import sys as _sys
        mock_uvicorn = MagicMock()
        _sys.modules["uvicorn"] = mock_uvicorn
        try:
            with patch.object(relay_mod.sys, "argv", ["relay.py"]):
                relay_mod.main()
        finally:
            _sys.modules.pop("uvicorn", None)

        kwargs = mock_uvicorn.run.call_args.kwargs
        assert kwargs["limit_concurrency"] is None
        assert kwargs["backlog"] == 2048


# ═══════════════════════════════════════════════════════════════════
#  Auth-switch retry re-borrows the pooled client (v1.7.1 regression)
# ═══════════════════════════════════════════════════════════════════


class TestAuthSwitchReborrow:
    async def test_auth_retry_holds_fresh_borrow(self, relay_mod, fresh_pool):
        """The auth-switch retry re-borrows the pooled client, so the client is
        marked in-use for the WHOLE retry call.

        Regression: the non-streaming path reused the already-released `client`
        borrow, leaving `_client_in_use == 0` mid-flight — so under load an LRU
        eviction (pool at cap) or _prune_client_pool could aclose() the client
        and abort the retry, misattributing a transient eviction as an upstream
        failure. The streaming path already re-borrows; this test locks the
        single-shot path to the same contract.
        """
        url = "socks5://u1:p1@p1:1080"
        relay_mod._client_pool.clear()
        relay_mod._client_last_used.clear()
        relay_mod._client_in_use.clear()
        relay_mod.auth_switcher.reset()
        relay_mod.auth_switcher.enabled = True
        # Seed the streak so the FIRST 401 observed by the request path crosses
        # the trigger threshold (threshold-1 → observe(401) → threshold → probe).
        relay_mod.auth_switcher._consecutive_401 = (
            relay_mod.AUTH_SWITCH_TRIGGER_THRESHOLD - 1
        )
        relay_mod.auth_switcher._last_probe_ts = 0.0
        relay_mod.auth_switcher._switch_ts.clear()

        observed_in_use = []
        call_count = {"n": 0}

        async def recording_single(client, method, url_, headers, body,
                                   proxy_entry, probe=False):
            observed_in_use.append(relay_mod._client_in_use.get(proxy_entry.url, 0))
            call_count["n"] += 1
            req = httpx.Request("POST", url_)
            if call_count["n"] == 1:
                # First attempt: 401 → triggers probe + retry.
                return httpx.Response(401, json={"error": {"message": "auth"}},
                                      request=req)
            # Retry with the switched auth → success.
            return httpx.Response(200, json={"ok": True}, request=req)

        with patch.object(relay_mod, "_proxy_single", new=recording_single), \
             patch.object(relay_mod.auth_switcher, "probe_and_switch",
                          new=AsyncMock(return_value=True)):
            resp = await relay_mod._proxy_request(
                "POST", "/chat/completions", b'{"model":"m"}',
                {"content-type": "application/json"}, "",
            )

        assert resp.status_code == 200
        assert len(observed_in_use) == 2, observed_in_use
        # BOTH the initial borrow and the auth retry must observe in_use == 1.
        # The pre-fix code saw [1, 0] (retry used a released borrow).
        assert observed_in_use == [1, 1], observed_in_use
        assert relay_mod._client_in_use.get(url, 0) == 0  # borrow fully released
        await relay_mod._close_all_clients()
