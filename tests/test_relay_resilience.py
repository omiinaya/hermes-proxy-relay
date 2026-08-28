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
import json
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
            req.url.path = "/v1/models"
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
        req.url.path = "/v1/models"
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


# ═══════════════════════════════════════════════════════════════════
#  Dynamic cap — auto-tuned concurrency (v1.8)
# ═══════════════════════════════════════════════════════════════════


class TestDynamicCap:
    def test_next_grows_when_headroom(self, relay_mod):
        """CPU well below target-15 → grow toward the ceiling."""
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_STEP = 0.1
        relay_mod.DYNAMIC_CAP_MIN = 10
        relay_mod.DYNAMIC_CAP_MAX = 500
        assert relay_mod._dynamic_cap_next(100, 10.0) > 100

    def test_next_holds_in_hysteresis_band(self, relay_mod):
        """CPU in (target-15, target] → hold (no churn)."""
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_STEP = 0.1
        relay_mod.DYNAMIC_CAP_MIN = 10
        relay_mod.DYNAMIC_CAP_MAX = 500
        assert relay_mod._dynamic_cap_next(100, 85.0) == 100
        assert relay_mod._dynamic_cap_next(100, 90.0) == 100

    def test_next_eases_down_above_target(self, relay_mod):
        """CPU between target and hard max → ease down 1 step."""
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_STEP = 0.1
        relay_mod.DYNAMIC_CAP_MIN = 10
        relay_mod.DYNAMIC_CAP_MAX = 500
        assert relay_mod._dynamic_cap_next(100, 93.0) < 100

    def test_next_hard_backoff_above_max(self, relay_mod):
        """CPU above hard max → 2× step backoff (never peg the core)."""
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_STEP = 0.1
        relay_mod.DYNAMIC_CAP_MIN = 10
        relay_mod.DYNAMIC_CAP_MAX = 500
        assert relay_mod._dynamic_cap_next(100, 99.0) <= 80  # 100 * (1 - 2*0.1)

    def test_next_clamps_to_bounds(self, relay_mod):
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_STEP = 0.1
        relay_mod.DYNAMIC_CAP_MIN = 10
        relay_mod.DYNAMIC_CAP_MAX = 500
        assert relay_mod._dynamic_cap_next(500, 1.0) <= 500   # ceiling
        assert relay_mod._dynamic_cap_next(10, 99.0) >= 10    # floor

    def test_process_cpu_seconds_real(self, relay_mod):
        """The real getrusage path returns monotonic cumulative CPU time."""
        v1 = relay_mod._process_cpu_seconds()
        assert v1 >= 0.0
        v2 = relay_mod._process_cpu_seconds()
        assert v2 >= v1  # cumulative, never decreases

    def test_resize_uses_effective_cap_when_dynamic(self, relay_mod):
        """Dynamic mode: _resize_semaphore applies _EFFECTIVE_CAP."""
        relay_mod.DYNAMIC_CAP_ENABLED = True
        orig_sem = relay_mod.semaphore
        relay_mod._semaphore_max = 24
        relay_mod._EFFECTIVE_CAP = 42
        try:
            assert relay_mod._resize_semaphore() is True
            assert relay_mod.semaphore is not orig_sem
            assert relay_mod._semaphore_max == 42
        finally:
            relay_mod.DYNAMIC_CAP_ENABLED = False
            relay_mod.semaphore = orig_sem
            relay_mod._semaphore_max = relay_mod.MAX_CONCURRENT_UPSTREAM
            relay_mod._EFFECTIVE_CAP = relay_mod.MAX_CONCURRENT_UPSTREAM

    def test_resize_ignores_effective_when_static(self, relay_mod):
        """Static mode: _EFFECTIVE_CAP is ignored; MAX_CONCURRENT_UPSTREAM wins."""
        relay_mod.DYNAMIC_CAP_ENABLED = False
        orig_sem = relay_mod.semaphore
        # NOTE: under pytest the module imports with env MAX_CONCURRENT_UPSTREAM=10
        # (conftest patch_env), so anchor to the ACTUAL static base, not 24.
        relay_mod._semaphore_max = relay_mod.MAX_CONCURRENT_UPSTREAM
        relay_mod._EFFECTIVE_CAP = 42  # must be ignored
        try:
            assert relay_mod._resize_semaphore() is False  # static target == _semaphore_max
            assert relay_mod.semaphore is orig_sem
        finally:
            relay_mod.semaphore = orig_sem
            relay_mod._semaphore_max = relay_mod.MAX_CONCURRENT_UPSTREAM
            relay_mod._EFFECTIVE_CAP = relay_mod.MAX_CONCURRENT_UPSTREAM

    def test_apply_rebases_effective_cap(self, relay_mod):
        """A reload re-merges the knobs and rebases _EFFECTIVE_CAP."""
        relay_mod.MAX_CONCURRENT_UPSTREAM = 60
        relay_mod._EFFECTIVE_CAP = 200
        try:
            relay_mod._apply_dynamic_cap_config({
                "DYNAMIC_CAP_ENABLED": "true",
                "DYNAMIC_CAP_CPU_TARGET_PCT": 85,
                "DYNAMIC_CAP_CPU_MAX_PCT": 92,
                "DYNAMIC_CAP_DISK_TARGET_PCT": 65,
                "DYNAMIC_CAP_DISK_MAX_PCT": 80,
                "DYNAMIC_CAP_MIN": 5,
                "DYNAMIC_CAP_MAX": 400,
                "DYNAMIC_CAP_INTERVAL_S": 3,
                "DYNAMIC_CAP_STEP": 0.15,
                "DYNAMIC_CAP_SMOOTHING": 0.4,
            })
            assert relay_mod.DYNAMIC_CAP_ENABLED is True
            assert relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT == 85.0
            assert relay_mod.DYNAMIC_CAP_DISK_TARGET_PCT == 65.0
            assert relay_mod.DYNAMIC_CAP_MAX == 400
            assert relay_mod._EFFECTIVE_CAP == 60  # rebased onto MAX_CONCURRENT_UPSTREAM
        finally:
            relay_mod.MAX_CONCURRENT_UPSTREAM = 24
            relay_mod._EFFECTIVE_CAP = 24
            relay_mod.DYNAMIC_CAP_ENABLED = False

    async def test_adjuster_tunes_cap_with_cpu(self, relay_mod, monkeypatch):
        """Live loop: sustained high CPU shrinks the cap; low CPU grows it."""
        relay_mod.DYNAMIC_CAP_ENABLED = True
        relay_mod.DYNAMIC_CAP_INTERVAL_S = 0.5  # floored to 0.5s by the adjuster
        relay_mod.DYNAMIC_CAP_SMOOTHING = 1.0  # instant response (no smoothing)
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 50
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 55
        relay_mod.DYNAMIC_CAP_STEP = 0.25
        relay_mod.DYNAMIC_CAP_MIN = 4
        relay_mod.DYNAMIC_CAP_MAX = 100
        relay_mod._EFFECTIVE_CAP = 24
        relay_mod.semaphore = asyncio.Semaphore(24)
        relay_mod._semaphore_max = 24

        # The adjuster floors the interval at 0.5s: 0.5s CPU per 0.5s interval
        # = 100% of one core (peg); 0.05s per interval = 10% (headroom).
        state = {"v": 10.0, "add": 0.5}

        def fake_cpu():
            state["v"] += state["add"]
            return state["v"]

        monkeypatch.setattr(relay_mod, "_process_cpu_seconds", fake_cpu)
        # No disk signal — keep the CPU-only dynamics deterministic (the real
        # /proc/diskstats on the test box must not influence the outcome).
        monkeypatch.setattr(relay_mod, "_read_disk_use", lambda: {})
        task = asyncio.create_task(relay_mod._dynamic_cap_adjuster())
        try:
            await asyncio.sleep(2.0)  # ~4 ticks of pegged CPU: 24→12→6→4
            assert relay_mod._EFFECTIVE_CAP < 24, relay_mod._EFFECTIVE_CAP

            state["add"] = 0.05  # 10% of one core → headroom → grow
            await asyncio.sleep(2.0)
            assert relay_mod._EFFECTIVE_CAP > 4, relay_mod._EFFECTIVE_CAP
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            relay_mod.DYNAMIC_CAP_ENABLED = False
            relay_mod.DYNAMIC_CAP_INTERVAL_S = 5.0
            relay_mod.DYNAMIC_CAP_SMOOTHING = 0.3
            relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
            relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
            relay_mod.DYNAMIC_CAP_STEP = 0.1
            relay_mod.DYNAMIC_CAP_MIN = 10
            relay_mod.DYNAMIC_CAP_MAX = 500
            relay_mod._EFFECTIVE_CAP = relay_mod.MAX_CONCURRENT_UPSTREAM
            relay_mod.semaphore = asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
            relay_mod._semaphore_max = relay_mod.MAX_CONCURRENT_UPSTREAM

    async def test_health_reports_dynamic_cap(self, relay_mod):
        relay_mod.DYNAMIC_CAP_ENABLED = True
        relay_mod._EFFECTIVE_CAP = 37
        relay_mod._dyn_last_cpu_pct = 88.4
        try:
            h = await relay_mod.health()
            dc = h["dynamic_cap"]
            assert dc["enabled"] is True
            assert dc["effective_max"] == 37
            assert dc["cpu_pct"] == 88.4
            assert dc["target_pct"] == relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT
        finally:
            relay_mod.DYNAMIC_CAP_ENABLED = False
            relay_mod._EFFECTIVE_CAP = relay_mod.MAX_CONCURRENT_UPSTREAM
            relay_mod._dyn_last_cpu_pct = 0.0

    def test_config_check_validates_dynamic_cap(self, relay_mod, fresh_pool, monkeypatch, capsys):
        monkeypatch.setattr(relay_mod, "DYNAMIC_CAP_ENABLED", True)
        monkeypatch.setattr(relay_mod, "DYNAMIC_CAP_CPU_TARGET_PCT", 150)  # invalid > 99
        with pytest.raises(SystemExit) as ei:
            relay_mod._run_config_check()
        assert ei.value.code == 1
        out = capsys.readouterr().out
        assert "Invalid DYNAMIC_CAP_CPU_TARGET_PCT" in out

    def test_config_check_warns_hold_false_with_dynamic(self, relay_mod, fresh_pool, monkeypatch, capsys):
        monkeypatch.setattr(relay_mod, "DYNAMIC_CAP_ENABLED", True)
        monkeypatch.setattr(relay_mod, "HOLD_PERMIT_FOR_STREAM", False)
        relay_mod._run_config_check()  # warning only — no SystemExit
        out = capsys.readouterr().out
        assert "CANNOT govern concurrent streams" in out

    # ── Disk-I/O awareness (v1.8.1) ────────────────────────────────

    def test_read_disk_use_parses_real_devices(self, relay_mod, monkeypatch, tmp_path):
        """/proc/diskstats parsing: keeps real disks, skips virtual/overlay."""
        fake = "\n".join([
            "   8       0 sda 100 10 1000 100 200 20 2000 200 0 50 30 0 0 0 0 5 6",
            "   8       1 sda1 50 5 500 50 100 10 1000 100 0 25 15 0 0 0 0 2 3",
            " 252       0 zd0 1000 100 10000 1000 2000 200 20000 2000 0 500 300 0 0 0 0 50 60",
            "   7       0 loop0 10 1 100 10 20 2 200 20 0 5 3 0 0 0 0 1 1",
            " 253       0 dm-0 999 99 9999 999 0 0 0 0 0 999 999 0 0 0 0 9 9",
            "   8       16 sdb",                                    # too few fields → skip
            "   9       0 sdc a b c d e f g h i j X 0 0 0 1 1 1",   # bad int in use col → skip
        ])
        monkeypatch.setattr("builtins.open", lambda *a, **k: __import__("io").StringIO(fake))
        got = relay_mod._read_disk_use()
        assert got == {"sda": 50, "sda1": 25}  # zd/loop/dm- filtered out

    def test_read_disk_use_unavailable_returns_empty(self, relay_mod, monkeypatch):
        def boom(*a, **k):
            raise OSError("no /proc")
        monkeypatch.setattr("builtins.open", boom)
        assert relay_mod._read_disk_use() == {}

    def test_disk_busy_pct_uses_busiest_device(self, relay_mod):
        prev = {"sda": 100, "sdb": 50}
        cur = {"sda": 400, "sdb": 50}  # sda +300ms in 1s = 30%; sdb idle
        assert relay_mod._disk_busy_pct(prev, cur, 1.0) == 30.0

    def test_disk_busy_pct_no_baseline(self, relay_mod):
        assert relay_mod._disk_busy_pct({}, {"sda": 100}, 1.0) == -1.0
        assert relay_mod._disk_busy_pct({"sda": 100}, {}, 1.0) == -1.0
        # New device with no prior snapshot is skipped; reset counter skipped
        assert relay_mod._disk_busy_pct({"sda": 500}, {"sda": 400, "sdb": 900}, 1.0) == 0.0

    def test_next_disk_over_max_hard_backoff(self, relay_mod):
        """Disk pegged forces a hard backoff even with idle CPU."""
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_DISK_TARGET_PCT = 70
        relay_mod.DYNAMIC_CAP_DISK_MAX_PCT = 85
        relay_mod.DYNAMIC_CAP_STEP = 0.1
        relay_mod.DYNAMIC_CAP_MIN = 10
        relay_mod.DYNAMIC_CAP_MAX = 500
        assert relay_mod._dynamic_cap_next(100, 5.0, 95.0) <= 80  # 100 * (1 - 2*0.1)

    def test_next_disk_over_target_eases_down(self, relay_mod):
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_DISK_TARGET_PCT = 70
        relay_mod.DYNAMIC_CAP_DISK_MAX_PCT = 85
        relay_mod.DYNAMIC_CAP_STEP = 0.1
        relay_mod.DYNAMIC_CAP_MIN = 10
        relay_mod.DYNAMIC_CAP_MAX = 500
        assert relay_mod._dynamic_cap_next(100, 5.0, 75.0) < 100

    def test_next_grows_only_when_both_have_headroom(self, relay_mod):
        """Idle CPU but busy-ish disk → hold, not grow (never grow into disk saturation)."""
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_DISK_TARGET_PCT = 70
        relay_mod.DYNAMIC_CAP_DISK_MAX_PCT = 85
        relay_mod.DYNAMIC_CAP_STEP = 0.1
        relay_mod.DYNAMIC_CAP_MIN = 10
        relay_mod.DYNAMIC_CAP_MAX = 500
        # CPU 10% (grow regime), disk 65% (>= 70-15 → no disk headroom) → hold
        assert relay_mod._dynamic_cap_next(100, 10.0, 65.0) == 100
        # Both with headroom → grow
        assert relay_mod._dynamic_cap_next(100, 10.0, 50.0) > 100
        # No disk signal → CPU-only grow preserved
        assert relay_mod._dynamic_cap_next(100, 10.0, None) > 100

    async def test_adjuster_backs_off_on_disk_pegged(self, relay_mod, monkeypatch):
        """Live loop: low CPU but a pegged disk shrinks the cap."""
        relay_mod.DYNAMIC_CAP_ENABLED = True
        relay_mod.DYNAMIC_CAP_INTERVAL_S = 0.5
        relay_mod.DYNAMIC_CAP_SMOOTHING = 1.0
        relay_mod.DYNAMIC_CAP_CPU_TARGET_PCT = 90
        relay_mod.DYNAMIC_CAP_CPU_MAX_PCT = 96
        relay_mod.DYNAMIC_CAP_DISK_TARGET_PCT = 70
        relay_mod.DYNAMIC_CAP_DISK_MAX_PCT = 85
        relay_mod.DYNAMIC_CAP_STEP = 0.25
        relay_mod.DYNAMIC_CAP_MIN = 4
        relay_mod.DYNAMIC_CAP_MAX = 100
        relay_mod._EFFECTIVE_CAP = 24
        relay_mod.semaphore = asyncio.Semaphore(24)
        relay_mod._semaphore_max = 24

        cpu_state = {"v": 10.0, "add": 0.05}  # 10% CPU → CPU would want to GROW

        def fake_cpu():
            cpu_state["v"] += cpu_state["add"]
            return cpu_state["v"]

        disk_state = {"n": 0}

        def fake_disk():
            # Each call advances sda by 500ms of I/O → 100% busy over 0.5s.
            disk_state["n"] += 1
            return {"sda": 1000 + disk_state["n"] * 500}

        monkeypatch.setattr(relay_mod, "_process_cpu_seconds", fake_cpu)
        monkeypatch.setattr(relay_mod, "_read_disk_use", fake_disk)
        task = asyncio.create_task(relay_mod._dynamic_cap_adjuster())
        try:
            await asyncio.sleep(2.0)  # ~4 ticks: disk pegged → 24→12→6→4
            assert relay_mod._EFFECTIVE_CAP < 24, relay_mod._EFFECTIVE_CAP
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            relay_mod.DYNAMIC_CAP_ENABLED = False
            relay_mod.DYNAMIC_CAP_INTERVAL_S = 5.0
            relay_mod.DYNAMIC_CAP_SMOOTHING = 0.3
            relay_mod.DYNAMIC_CAP_STEP = 0.1
            relay_mod.DYNAMIC_CAP_MIN = 10
            relay_mod.DYNAMIC_CAP_MAX = 500
            relay_mod._EFFECTIVE_CAP = relay_mod.MAX_CONCURRENT_UPSTREAM
            relay_mod.semaphore = asyncio.Semaphore(relay_mod.MAX_CONCURRENT_UPSTREAM)
            relay_mod._semaphore_max = relay_mod.MAX_CONCURRENT_UPSTREAM

    async def test_health_reports_disk_fields(self, relay_mod):
        relay_mod.DYNAMIC_CAP_ENABLED = True
        relay_mod._dyn_last_disk_pct = 55.5
        try:
            h = await relay_mod.health()
            dc = h["dynamic_cap"]
            assert dc["disk_pct"] == 55.5
            assert dc["disk_target_pct"] == relay_mod.DYNAMIC_CAP_DISK_TARGET_PCT
            assert dc["disk_hard_max_pct"] == relay_mod.DYNAMIC_CAP_DISK_MAX_PCT
        finally:
            relay_mod.DYNAMIC_CAP_ENABLED = False
            relay_mod._dyn_last_disk_pct = 0.0

    def test_config_check_validates_disk_knobs(self, relay_mod, fresh_pool, monkeypatch, capsys):
        monkeypatch.setattr(relay_mod, "DYNAMIC_CAP_ENABLED", True)
        monkeypatch.setattr(relay_mod, "DYNAMIC_CAP_DISK_TARGET_PCT", 150)  # invalid
        with pytest.raises(SystemExit) as ei:
            relay_mod._run_config_check()
        assert ei.value.code == 1
        out = capsys.readouterr().out
        assert "Invalid DYNAMIC_CAP_DISK_TARGET_PCT" in out


# ═══════════════════════════════════════════════════════════════════
#  Production parity ports (v1.9.0) — Decodo pool, model aliases,
#  per-model budget exhaustion, truncation, /go routing, free filter
# ═══════════════════════════════════════════════════════════════════


class TestProdParityPorts:
    # ── Decodo proxy-group env loader ──────────────────────────────

    def test_proxy_groups_from_env(self, relay_mod, monkeypatch):
        monkeypatch.setenv("DECODO_HOST", "dc.decodo.com")
        monkeypatch.setenv("DECODO_USER", "u1")
        monkeypatch.setenv("DECODO_PASS", "p1")
        monkeypatch.setenv("DECODO_START_PORT", "10001")
        monkeypatch.setenv("DECODO_END_PORT", "10003")
        monkeypatch.setenv("DECODO2_HOST", "dc2.decodo.com")
        monkeypatch.setenv("DECODO2_USER", "u2")
        monkeypatch.setenv("DECODO2_PASS", "p2")
        monkeypatch.setenv("DECODO2_START_PORT", "20001")
        monkeypatch.setenv("DECODO2_END_PORT", "20002")
        urls = relay_mod._load_proxy_groups_from_env()
        assert len(urls) == 5
        assert urls[0] == "socks5://u1:p1@dc.decodo.com:10001"
        assert urls[3] == "socks5://u2:p2@dc2.decodo.com:20001"

    def test_proxy_groups_empty_when_no_env(self, relay_mod, monkeypatch):
        monkeypatch.delenv("DECODO_HOST", raising=False)
        monkeypatch.delenv("DECODO_PASS", raising=False)
        assert relay_mod._load_proxy_groups_from_env() == []

    def test_proxy_groups_env_edges(self, relay_mod, monkeypatch):
        """Group without PASS is skipped; bad ports skipped; reversed range swapped."""
        monkeypatch.setenv("DECODO_HOST", "dc.decodo.com")
        monkeypatch.setenv("DECODO_USER", "u1")
        monkeypatch.delenv("DECODO_PASS", raising=False)  # no pass → group skipped
        monkeypatch.setenv("DECODO2_HOST", "dc2.decodo.com")
        monkeypatch.setenv("DECODO2_USER", "u2")
        monkeypatch.setenv("DECODO2_PASS", "p2")
        monkeypatch.setenv("DECODO2_START_PORT", "not-a-port")  # bad int → skipped
        monkeypatch.setenv("DECODO3_HOST", "dc3.decodo.com")
        monkeypatch.setenv("DECODO3_USER", "u3")
        monkeypatch.setenv("DECODO3_PASS", "p3")
        monkeypatch.setenv("DECODO3_START_PORT", "10005")
        monkeypatch.setenv("DECODO3_END_PORT", "10002")  # reversed → swapped
        urls = relay_mod._load_proxy_groups_from_env()
        assert len(urls) == 4  # only DECODO3 (10002..10005)
        assert urls[0] == "socks5://u3:p3@dc3.decodo.com:10002"
        assert urls[-1] == "socks5://u3:p3@dc3.decodo.com:10005"

    def test_model_exhaust_cap_env(self, relay_mod, monkeypatch):
        monkeypatch.setenv("MODEL_EXHAUST_CAP", "123")
        pool = relay_mod.CooldownPool(["socks5://a@1:1"])
        assert pool._exhaust_cap == 123.0

    def test_valid_response_body_edges(self, relay_mod):
        # Long enough (>10B) to reach the JSON parse, then:
        assert relay_mod._valid_response_body(b'[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]')[0] is False  # json list, not object
        assert relay_mod._valid_response_body(b'{"broken json here')[0] is False              # json decode error
        assert relay_mod._valid_response_body(b'{"valid": true} trailing')[0] is False        # trailing garbage

    def test_translate_model_only_touches_model(self, relay_mod):
        # A non-dict JSON body (array) is returned unchanged.
        assert relay_mod.translate_model(b'[1, 2, 3, 4, 5]') == b'[1, 2, 3, 4, 5]'

    def test_init_pool_uses_env_groups(self, relay_mod, fresh_pool, monkeypatch):
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "")
        monkeypatch.setenv("DECODO_HOST", "dc.decodo.com")
        monkeypatch.setenv("DECODO_USER", "u")
        monkeypatch.setenv("DECODO_PASS", "p")
        monkeypatch.setenv("DECODO_START_PORT", "10001")
        monkeypatch.setenv("DECODO_END_PORT", "10002")
        relay_mod._init_pool()
        assert relay_mod.pool.total == 2
        assert relay_mod.pool._proxies[0].url.startswith("socks5://u:p@dc.decodo.com:")

    # ── Model alias translation ────────────────────────────────────

    def test_translate_model_alias(self, relay_mod):
        out = relay_mod.translate_model(b'{"model": "oc-deepseek-v4-flash", "stream": true}')
        assert out == b'{"model": "deepseek-v4-flash-free", "stream": true}'
        # Already-real id → unchanged
        assert relay_mod.translate_model(b'{"model": "deepseek-v4-flash-free"}') == b'{"model": "deepseek-v4-flash-free"}'
        # No model / non-str model / invalid JSON → unchanged
        assert relay_mod.translate_model(b'{"foo": 1}') == b'{"foo": 1}'
        assert relay_mod.translate_model(b'{"model": 7}') == b'{"model": 7}'
        raw = b"not-json"
        assert relay_mod.translate_model(raw) == raw

    def test_extract_model(self, relay_mod):
        assert relay_mod._extract_model(b'{"model": "m1"}') == "m1"
        assert relay_mod._extract_model(b"garbage") == ""
        assert relay_mod._extract_model(None) == ""

    # ── Single-pass body parse (_parse_request_body) ──────────────

    def test_parse_request_body_plain(self, relay_mod):
        body = b'{"model": "deepseek-v4-flash-free", "stream": true}'
        out, model, is_stream = relay_mod._parse_request_body(body)
        assert out == body  # no alias → unchanged bytes
        assert model == "deepseek-v4-flash-free"
        assert is_stream is True

    def test_parse_request_body_alias(self, relay_mod):
        out, model, is_stream = relay_mod._parse_request_body(
            b'{"model": "oc-deepseek-v4-flash", "stream": true}'
        )
        assert model == "deepseek-v4-flash-free"
        assert is_stream is True
        # Translated body has the real model id.
        assert b"deepseek-v4-flash-free" in out
        assert b"oc-deepseek-v4-flash" not in out

    def test_parse_request_body_non_stream_and_nested(self, relay_mod):
        # stream:true nested inside a tool schema must NOT count as streaming
        # (the regex fallback would false-positive; the parsed-dict path uses
        # the TOP-LEVEL key, which is what the upstream honors).
        body = b'{"model": "m1", "tools": [{"function": {"name": "f", "stream": true}}]}'
        out, model, is_stream = relay_mod._parse_request_body(body)
        assert out == body
        assert model == "m1"
        assert is_stream is False

    def test_parse_request_body_invalid_json_falls_back(self, relay_mod):
        raw = b"not-json-at-all"
        out, model, is_stream = relay_mod._parse_request_body(raw)
        # Fallback path: translate/extract leave it alone, byte-scan finds no stream.
        assert out == raw
        assert model == ""
        assert is_stream is False

    def test_parse_request_body_non_dict_falls_back(self, relay_mod):
        arr = b"[1, 2, 3, 4, 5, 6, 7, 8]"
        out, model, is_stream = relay_mod._parse_request_body(arr)
        assert out == arr
        assert model == ""
        assert is_stream is False

    def test_parse_request_body_large_falls_back_to_byte_scan(self, relay_mod):
        # Body over _STREAM_JSON_PARSE_LIMIT: no object-tree parse; stream
        # detection falls back to the byte scan (must still find a top-level
        # stream:true and extract the model).
        import json as _json
        big = _json.dumps({"model": "oc-deepseek-v4-flash", "stream": True,
                           "messages": [{"role": "user", "content": "x" * 300_000}]}).encode()
        out, model, is_stream = relay_mod._parse_request_body(big)
        assert model == "deepseek-v4-flash-free"
        assert is_stream is True
        assert b"deepseek-v4-flash-free" in out

    # ── Per-model budget exhaustion ────────────────────────────────

    def test_model_exhaust_park_and_skip(self, relay_mod):
        pool = relay_mod.CooldownPool(["socks5://a@1:1", "socks5://b@2:1", "socks5://c@3:1"])
        pool.mark_model_exhaust("socks5://a@1:1", "model-x", 1000)
        got = {pool.next("model-x").url for _ in range(6)}
        assert "socks5://a@1:1" not in got  # skipped for model-x
        assert pool.exhausted_count_for("model-x") == 1
        assert pool.exhausted_models() == {"model-x": 1}
        # Without a model the same proxy is still returned (it's healthy)
        found = any(pool.next().url == "socks5://a@1:1" for _ in range(10))
        assert found

    def test_model_exhaust_expires(self, relay_mod, monkeypatch):
        pool = relay_mod.CooldownPool(["socks5://a@1:1"])
        now = [1000.0]
        monkeypatch.setattr(relay_mod.time, "monotonic", lambda: now[0])
        pool.mark_model_exhaust("socks5://a@1:1", "m", 5.0)
        assert pool.exhausted_count_for("m") == 1
        assert pool.next("m") is None  # skipped
        now[0] += 10.0
        assert pool.exhausted_count_for("m") == 0
        assert pool.next("m") is not None  # skip lifted

    def test_latency_skip_honors_model_exhaust(self, relay_mod, monkeypatch):
        """The latency-skip scan must not pick a model-exhausted alternate."""
        monkeypatch.setattr(relay_mod, "LATENCY_SKIP_THRESHOLD_MS", 500)
        pool = relay_mod.CooldownPool(["socks5://a@1:1", "socks5://b@2:1", "socks5://c@3:1"])
        for i in range(3):
            pool._proxies[i].latency_samples = 5
            pool._proxies[i].avg_latency_ms = 900 if i == 0 else 100
        pool.mark_model_exhaust("socks5://b@2:1", "model-x", 1000)  # B fast but spent
        got = pool.next("model-x")
        assert got.url == "socks5://c@3:1"  # A slow, B exhausted → C

    # ── 429 FreeUsageLimitError helpers ────────────────────────────

    def test_is_model_exhaust_429(self, relay_mod):
        from fastapi.responses import Response as FResp
        r = FResp(content=b'{"error":{"type":"FreeUsageLimitError"}}', status_code=429)
        assert relay_mod._is_model_exhaust_429(r) is True
        r2 = FResp(content=b'{"error":"rate limited"}', status_code=429)
        assert relay_mod._is_model_exhaust_429(r2) is False
        r3 = FResp(content=b'{"error":{"type":"FreeUsageLimitError"}}', status_code=200)
        assert relay_mod._is_model_exhaust_429(r3) is False

    def test_model_exhaust_response(self, relay_mod):
        resp = relay_mod._model_exhaust_response("m1", 5)
        assert resp.status_code == 429
        assert b"FreeUsageLimitError" in resp.body
        assert b"m1" in resp.body

    # ── Truncation validation ──────────────────────────────────────

    def test_valid_response_body(self, relay_mod):
        ok_body = b'{"choices":[{"message":{"role":"assistant","content":"hi"}}]}'
        assert relay_mod._valid_response_body(ok_body) == (True, "")
        assert relay_mod._valid_response_body(b'{"choices":[]}')[0] is False
        assert relay_mod._valid_response_body(b'{"choices":[{"role":"x"}]}')[0] is False
        assert relay_mod._valid_response_body(b"{}")[0] is False
        assert relay_mod._valid_response_body(b"short")[0] is False

    # ── UA spoofing (Cloudflare anti-bot) ──────────────────────────

    def test_build_headers_spoofs_browser_ua(self, relay_mod, monkeypatch):
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "k")
        monkeypatch.setattr(relay_mod, "UPSTREAM_AUTH_TYPE", "bearer")
        h = relay_mod._build_headers({"User-Agent": "Python-urllib/3.11"})
        assert "Mozilla/5.0" in h["User-Agent"]
        assert "Python-urllib" not in h.get("User-Agent", "")

    # ── /go upstream routing ───────────────────────────────────────

    async def test_go_route_wires_go_flag(self, relay_mod, fresh_pool, monkeypatch):
        seen = {}

        async def fake_req(method, path, body, headers, query, go=False):
            seen.update(method=method, path=path, go=go)
            return {"ok": True}

        monkeypatch.setattr(relay_mod, "_proxy_request", fake_req)
        from fastapi.testclient import TestClient
        with TestClient(relay_mod.app) as tc:
            r = tc.post("/go/v1/chat/completions", json={"model": "m"})
        assert r.status_code == 200
        assert seen["go"] is True
        assert seen["path"] == "/v1/chat/completions"

    async def test_go_unconfigured_returns_503(self, relay_mod, fresh_pool, monkeypatch):
        monkeypatch.setattr(relay_mod, "GO_UPSTREAM_BASE", "")
        from fastapi.testclient import TestClient
        with TestClient(relay_mod.app) as tc:
            r = tc.post("/go/v1/chat/completions", json={"model": "m"})
        assert r.status_code == 503

    async def test_go_models_unconfigured_empty(self, relay_mod, monkeypatch):
        """/go/v1/models with GO_UPSTREAM_BASE empty returns an empty list."""
        monkeypatch.setattr(relay_mod, "GO_UPSTREAM_BASE", "")
        from fastapi.testclient import TestClient
        with TestClient(relay_mod.app) as tc:
            r = tc.get("/go/v1/models")
        assert r.status_code == 200
        assert r.json()["data"] == []

    async def test_go_proxy_all_route_wires_go_flag(self, relay_mod, fresh_pool, monkeypatch):
        """A generic /go/{path} request (e.g. embeddings) is routed with go=True."""
        seen = {}

        async def fake_req(method, path, body, headers, query, go=False):
            seen.update(method=method, path=path, go=go)
            return {"ok": True}

        monkeypatch.setattr(relay_mod, "_proxy_request", fake_req)
        from fastapi.testclient import TestClient
        with TestClient(relay_mod.app) as tc:
            r = tc.post("/go/v1/embeddings", json={"input": "hi"})
        assert r.status_code == 200
        assert seen["go"] is True
        assert seen["path"] == "/v1/embeddings"

    # ── Free-models filter ─────────────────────────────────────────

    async def test_models_free_only_filters(self, relay_mod, monkeypatch):
        monkeypatch.setattr(relay_mod, "MODELS_FREE_ONLY", True)
        relay_mod.MODELS_CACHE = [{"id": "deepseek-v4-flash-free"}, {"id": "gpt-5.6-sol"}]
        relay_mod.MODELS_CACHE_UPDATED = time.monotonic()  # fresh → cache hit
        req = MagicMock()
        req.url.path = "/v1/models"
        data = await relay_mod.list_models(req)
        assert [m["id"] for m in data["data"]] == ["deepseek-v4-flash-free"]

    # ── Model-exhaust sweep in _proxy_request ──────────────────────

    async def test_all_exhausted_returns_clean_429(self, relay_mod, fresh_pool):
        """Every proxy parked for the model → clean FreeUsageLimitError 429,
        and the proxies stay ACTIVE (not cooled — they serve other models)."""
        for e in relay_mod.pool._proxies:
            relay_mod.pool.mark_model_exhaust(e.url, "m1", 3600)
        body = b'{"model": "m1", "messages": [{"role": "user", "content": "hi"}]}'
        resp = await relay_mod._proxy_request(
            "POST", "/chat/completions", body, {"content-type": "application/json"}, "")
        assert resp.status_code == 429
        assert b"FreeUsageLimitError" in resp.body
        assert relay_mod.pool.available_count == relay_mod.pool.total  # no proxy cooled

    async def test_model_exhaust_429_sweeps_to_success(self, relay_mod, fresh_pool, monkeypatch):
        """FreeUsageLimitError from proxy A parks A for the model and the loop
        keeps sweeping — proxy B's valid 200 completes the request."""
        relay_mod.MAX_REQUEST_RETRIES = 5
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(429, json={"error": {"type": "FreeUsageLimitError"}})
            return httpx.Response(200, json={
                "choices": [{"message": {"role": "assistant", "content": "ok"}}]
            })

        mock_client = make_client(handler)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            resp = await relay_mod._proxy_request(
                "POST", "/chat/completions",
                b'{"model": "m1", "messages": [{"role": "user", "content": "hi"}]}',
                {"content-type": "application/json"}, "")
        assert resp.status_code == 200
        assert calls["n"] == 3  # swept past the exhausted proxy WITHOUT burning retries
        assert relay_mod.pool.exhausted_count_for("m1") == 1  # proxy A parked for m1
        assert relay_mod.pool.available_count == relay_mod.pool.total  # not cooled

    async def test_truncated_response_retried(self, relay_mod, fresh_pool, monkeypatch):
        """A 200 chat response without choices is treated as truncation and
        retried on the next proxy instead of being relayed as-is."""
        relay_mod.MAX_REQUEST_RETRIES = 5
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(200, json={"id": "truncated"})  # no choices
            return httpx.Response(200, json={
                "choices": [{"message": {"role": "assistant", "content": "ok"}}]
            })

        mock_client = make_client(handler)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            resp = await relay_mod._proxy_request(
                "POST", "/chat/completions",
                b'{"model": "m1", "messages": [{"role": "user", "content": "hi"}]}',
                {"content-type": "application/json"}, "")
        assert resp.status_code == 200
        assert calls["n"] == 2

    async def test_stream_model_exhaust_sweeps(self, relay_mod, fresh_pool, monkeypatch):
        """STREAM path: a FreeUsageLimitError 429 parks the proxy for the model
        and the generator sweeps on to a proxy that streams successfully."""
        relay_mod.MAX_REQUEST_RETRIES = 5
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(429, json={"error": {"type": "FreeUsageLimitError"}})
            return httpx.Response(
                200, content=b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\ndata: [DONE]\n\n',
                headers={"content-type": "text/event-stream"},
            )

        mock_client = make_client(handler)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            resp = await relay_mod._proxy_request(
                "POST", "/chat/completions",
                b'{"model": "m1", "stream": true, "messages": [{"role": "user", "content": "hi"}]}',
                {"content-type": "application/json"}, "")
        assert resp.status_code == 200
        assert calls["n"] == 2
        assert relay_mod.pool.exhausted_count_for("m1") == 1
        assert relay_mod.pool.available_count == relay_mod.pool.total

    async def test_stream_all_exhausted_clean_429(self, relay_mod, fresh_pool):
        """STREAM path: every proxy parked for the model → clean FreeUsageLimitError 429."""
        for e in relay_mod.pool._proxies:
            relay_mod.pool.mark_model_exhaust(e.url, "m1", 3600)
        resp = await relay_mod._proxy_request(
            "POST", "/chat/completions",
            b'{"model": "m1", "stream": true, "messages": [{"role": "user", "content": "hi"}]}',
            {"content-type": "application/json"}, "")
        assert resp.status_code == 429
        assert b"FreeUsageLimitError" in resp.body
        assert relay_mod.pool.available_count == relay_mod.pool.total

    async def test_alias_translated_end_to_end(self, relay_mod, fresh_pool, monkeypatch):
        """A request with an oc-* alias reaches the upstream with the REAL id."""
        seen = {"model": None}

        def handler(request):
            seen["model"] = json.loads(request.content).get("model")
            return httpx.Response(200, json={
                "choices": [{"message": {"role": "assistant", "content": "ok"}}]
            })

        mock_client = make_client(handler)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            resp = await relay_mod._proxy_request(
                "POST", "/chat/completions",
                b'{"model": "oc-deepseek-v4-flash", "messages": [{"role": "user", "content": "hi"}]}',
                {"content-type": "application/json"}, "")
        assert resp.status_code == 200
        assert seen["model"] == "deepseek-v4-flash-free"  # translated upstream

    # ── Stream idle timeout (STREAM_IDLE_TIMEOUT) ────────────────

    async def test_stream_idle_timeout_interrupts_stall(self, relay_mod, fresh_pool, monkeypatch):
        """A proxy that goes silent mid-stream must release its slot + client
        after STREAM_IDLE_TIMEOUT instead of holding them for the full
        UPSTREAM_READ_TIMEOUT (the disconnect-capable stall)."""
        import asyncio as _asyncio

        relay_mod.STREAM_IDLE_TIMEOUT = 0.2  # very short for the test
        relay_mod.HOLD_PERMIT_FOR_STREAM = True
        started = _asyncio.Event()

        async def streaming_body():
            # First chunk arrives quickly, then silence — the idle bound must
            # interrupt the wait for the second chunk.
            yield b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
            started.set()
            await _asyncio.sleep(5.0)  # longer than the idle timeout
            yield b'data: [DONE]\n\n'

        def handler(request):
            return httpx.Response(
                200,
                content=streaming_body(),
                headers={"content-type": "text/event-stream"},
            )

        mock_client = make_client(handler)
        sem = _asyncio.Semaphore(5)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            resp = await relay_mod._proxy_request(
                "POST", "/chat/completions",
                b'{"model": "m1", "stream": true, "messages": [{"role": "user", "content": "hi"}]}',
                {"content-type": "application/json"}, "")
        assert resp.status_code == 200
        chunks = []
        async for chunk in resp.body_iterator:
            chunks.append(chunk)
        joined = b"".join(chunks)
        # The stall was interrupted: we got the first chunk + an error chunk,
        # and the generator terminated WITHOUT waiting the full 5s sleep.
        assert b"hi" in joined
        assert b"Stream interrupted" in joined or b"stream_error" in joined

    async def test_stream_idle_timeout_zero_uses_read_timeout(self, relay_mod, fresh_pool, monkeypatch):
        """STREAM_IDLE_TIMEOUT=0 falls back to UPSTREAM_READ_TIMEOUT (no extra
        bound) — a long but alive stream is not killed."""
        relay_mod.STREAM_IDLE_TIMEOUT = 0
        relay_mod.UPSTREAM_READ_TIMEOUT = 120
        relay_mod.HOLD_PERMIT_FOR_STREAM = True

        def handler(request):
            return httpx.Response(
                200,
                content=b'data: {"choices":[{"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n',
                headers={"content-type": "text/event-stream"},
            )

        mock_client = make_client(handler)
        with patch.object(relay_mod, "_get_client", return_value=mock_client):
            resp = await relay_mod._proxy_request(
                "POST", "/chat/completions",
                b'{"model": "m1", "stream": true, "messages": [{"role": "user", "content": "hi"}]}',
                {"content-type": "application/json"}, "")
        assert resp.status_code == 200
        joined = b"".join([c async for c in resp.body_iterator])
        assert b"ok" in joined
        assert b"Stream interrupted" not in joined

    # ── Client pool cap auto-scales to proxy count ───────────────

    def test_client_pool_cap_scales_to_pool_size(self, relay_mod, fresh_pool):
        """_client_pool_cap() must never be below the proxy count — with 250
        proxies and a 100-client floor, round-robin traffic would otherwise
        pay a fresh SOCKS5+TLS handshake for every non-pooled proxy."""
        relay_mod.CLIENT_POOL_MAX = 100
        assert relay_mod._client_pool_cap() >= relay_mod.pool.total  # 3 proxies
        # Even a tiny configured floor must not undercut the pool size.
        relay_mod.CLIENT_POOL_MAX = 1
        assert relay_mod._client_pool_cap() == relay_mod.pool.total
        # A huge floor wins (operator opted into a bigger pool).
        relay_mod.CLIENT_POOL_MAX = 500
        assert relay_mod._client_pool_cap() == 500
        relay_mod.CLIENT_POOL_MAX = 100

    async def test_client_pool_holds_one_per_proxy_under_round_robin(self, relay_mod, fresh_pool, monkeypatch):
        """With the auto-scaled cap, borrowing a client for EVERY proxy in the
        pool must NOT evict earlier proxies (no handshake churn under round
        robin). Under the old fixed 100 cap this only worked below 100 proxies;
        the test proves the cap tracks the pool."""
        # A larger pool than the old fixed cap could hold.
        urls = [f"socks5://u{i}:p{i}@p{i}:1080" for i in range(105)]
        relay_mod.pool = relay_mod.CooldownPool(urls)
        relay_mod.CLIENT_POOL_MAX = 10  # tiny floor — pool size (105) dominates
        try:
            for url in urls:
                client = await relay_mod._get_client(url)
                assert client is not None
            assert len(relay_mod._client_pool) == 105  # no eviction — cap auto-scaled
        finally:
            await relay_mod._close_all_clients()
            relay_mod.CLIENT_POOL_MAX = 100
