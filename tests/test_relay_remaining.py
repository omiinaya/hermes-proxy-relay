"""Tests for the remaining uncovered relay features.

Features tested here:
- record_latency / stats with latency samples
- _update_models_cache + MODELS_CACHE TTL refresh
- _auto_star (GitHub auto-star — mocked)
- _proxy_health_check (background health checker — mocked)
- main() entrypoint --version and --help
"""

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestLatencyTracking:
    """record_latency() and stats() latency aggregation."""

    def test_record_latency_updates_proxy(self, cooldown_pool):
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_latency(proxy, 100.0)
        assert proxy.latency_samples == 1
        assert proxy.last_latency_ms == 100.0
        assert proxy.avg_latency_ms == 100.0

    def test_moving_average(self, cooldown_pool):
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_latency(proxy, 100.0)
        cooldown_pool.record_latency(proxy, 300.0)
        # (100 + 300) / 2 = 200
        assert proxy.avg_latency_ms == 200.0
        assert proxy.last_latency_ms == 300.0

    def test_multiple_proxies_avg(self, cooldown_pool):
        p1 = cooldown_pool.next()
        p2 = cooldown_pool.next()
        assert p1 is not None and p2 is not None
        cooldown_pool.record_latency(p1, 50.0)
        cooldown_pool.record_latency(p2, 150.0)
        stats = cooldown_pool.stats()
        # (50 + 150) / 2 = 100
        assert stats["avg_latency_ms"] == 100.0

    def test_stats_without_latency_samples(self, cooldown_pool):
        stats = cooldown_pool.stats()
        assert stats["avg_latency_ms"] == 0.0

    def test_stats_rounds_to_one_decimal(self, cooldown_pool):
        p1 = cooldown_pool.next()
        assert p1 is not None
        cooldown_pool.record_latency(p1, 33.33333)
        stats = cooldown_pool.stats()
        # 33.33333 rounds to 33.3
        assert stats["avg_latency_ms"] == 33.3


class TestModelsCache:
    """_update_models_cache() and cache freshness."""

    @pytest.fixture(autouse=True)
    def reset_cache(self):
        import relay.relay as relay_mod
        relay_mod.MODELS_CACHE = []
        relay_mod.MODELS_CACHE_UPDATED = 0.0

    def test_update_models_cache_sets_data(self):
        from relay.relay import _update_models_cache
        models = [{"id": "gpt-4"}, {"id": "gpt-4o"}]
        _update_models_cache(models)
        import relay.relay as relay_mod
        assert relay_mod.MODELS_CACHE == models
        assert relay_mod.MODELS_CACHE_UPDATED > 0

    def test_cache_freshness_check(self):
        import relay.relay as relay_mod
        models = [{"id": "claude-3"}]
        relay_mod._update_models_cache(models)
        now = time.monotonic()
        # Fresh cache — TTL not exceeded
        assert (now - relay_mod.MODELS_CACHE_UPDATED) < relay_mod.MODELS_CACHE_TTL
        assert relay_mod.MODELS_CACHE == models


class TestAutoStar:
    """_auto_star() — GitHub auto-star logic (mocked)."""

    @pytest.fixture(autouse=True)
    def no_token(self, monkeypatch):
        # Ensure no GITHUB_TOKEN so the function returns early
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    def test_no_token_returns_early(self):
        from relay.relay import _auto_star
        # Without token, should complete without any HTTP calls
        result = _auto_star()
        # It's an async function — must be awaited
        import asyncio
        asyncio.run(result)

    def test_token_owner_is_author_skips(self, monkeypatch):
        import relay.relay as relay_mod
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

        # Mock httpx to simulate token owner = repo author
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"login": "omiinaya"}

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = False

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=mock_client):
            import asyncio
            asyncio.run(relay_mod._auto_star())

        # Should have only called /user (to check owner), not starred
        assert mock_client.get.call_count == 1

    def test_already_starred_skips(self, monkeypatch):
        import relay.relay as relay_mod
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

        # First call returns user (not owner), second call returns 204 (already starred)
        mock_user = MagicMock()
        mock_user.status_code = 200
        mock_user.json.return_value = {"login": "somebody-else"}

        mock_starred = MagicMock()
        mock_starred.status_code = 204

        mock_client = AsyncMock()
        mock_client.get.side_effect = [mock_user, mock_starred]
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = False

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=mock_client):
            import asyncio
            asyncio.run(relay_mod._auto_star())

        # Two GETs: /user + /starred check. No PUT (already starred)
        assert mock_client.get.call_count == 2
        assert mock_client.put.call_count == 0

    def test_stars_repo_when_not_starred(self, monkeypatch):
        import relay.relay as relay_mod
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

        mock_user = MagicMock()
        mock_user.status_code = 200
        mock_user.json.return_value = {"login": "somebody-else"}

        mock_starred = MagicMock()
        mock_starred.status_code = 404  # not starred yet

        mock_put = MagicMock()
        mock_put.status_code = 204  # star succeeded

        mock_client = AsyncMock()
        mock_client.get.side_effect = [mock_user, mock_starred]
        mock_client.put.return_value = mock_put
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = False

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=mock_client):
            import asyncio
            asyncio.run(relay_mod._auto_star())

        assert mock_client.put.call_count == 1
        # Verify it starred the right repo
        put_url = mock_client.put.call_args[0][0]
        assert "omiinaya/hermes-proxy-relay" in put_url

    def test_api_failure_is_silent(self, monkeypatch):
        """Auto-star failures should not crash the relay (debug logged only)."""
        import relay.relay as relay_mod
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

        mock_client = AsyncMock()
        mock_client.get.side_effect = Exception("Network error")
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = False

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=mock_client):
            import asyncio
            # Should NOT raise
            asyncio.run(relay_mod._auto_star())


class TestHealthChecker:
    """_proxy_health_check() background task (mocked)."""

    @pytest.fixture(autouse=True)
    def patch_interval(self, monkeypatch):
        import relay.relay as relay_mod
        # Short interval so the loop runs quickly in tests
        monkeypatch.setattr(relay_mod, "PROXY_HEALTH_CHECK_INTERVAL", 0.01)

    async def test_marks_dead_proxy(self, cooldown_pool):
        """A failing proxy is marked permanently dead when another proxy
        in the same sweep succeeds (proves the health target is reachable)."""
        import relay.relay as relay_mod

        # Use a pool with multiple proxies — only one fails
        relay_mod.pool = cooldown_pool  # 4 proxies (SAMPLE_PROXIES)

        fail_client = AsyncMock()
        fail_client.__aenter__.side_effect = Exception("Connection refused")
        fail_client.__aexit__.return_value = False

        success_client = AsyncMock()
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_client.get.return_value = success_resp
        success_client.__aenter__.return_value = success_client
        success_client.__aexit__.return_value = False

        # First proxy fails, remaining 3 succeed
        with patch.object(relay_mod.httpx, "AsyncClient") as mock_ctor:
            mock_ctor.side_effect = [fail_client] + [success_client] * 3
            # Start health check and cancel after one iteration
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # At least one proxy should be permanently dead
        stats = relay_mod.pool.stats()
        assert stats["permanently_failed"] >= 1

    async def test_no_pool_skips(self, empty_pool):
        import relay.relay as relay_mod
        relay_mod.pool = empty_pool

        with patch.object(relay_mod.httpx, "AsyncClient") as mock_ctor:
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # With an empty pool, no clients should be created
        assert mock_ctor.call_count == 0


class TestMainEntrypoint:
    """main() — CLI entrypoint."""

    def test_version_flag(self):
        import relay.relay as relay_mod
        with patch.object(relay_mod.sys, "argv", ["relay.py", "--version"]):
            with pytest.raises(SystemExit) as exc_info:
                relay_mod.main()
            assert exc_info.value.code == 0

    def test_help_flag(self):
        import relay.relay as relay_mod
        with patch.object(relay_mod.sys, "argv", ["relay.py", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                relay_mod.main()
            # argparse --help exits with 0
            assert exc_info.value.code == 0

    def test_config_flag_passes(self, monkeypatch, tmp_path):
        """--config with a valid file should not crash (uvicorn would run, so
        we mock uvicorn.run)."""
        import relay.relay as relay_mod

        cfg = {
            "UPSTREAM_BASE": "https://test.example.com/v1",
            "UPSTREAM_API_KEY": "test-key",
            "UPSTREAM_AUTH_TYPE": "bearer",
            "RELAY_PORT": 9999,
            "MAX_CONCURRENT_UPSTREAM": 5,
            "MODEL_FILTER_PATTERN": ".*",
            "LOG_LEVEL": "CRITICAL",
            "PROXY_LIST": "",
            "PROXY_LIST_ENV": "",
        }
        cfg_path = tmp_path / "config.json"
        cfg_path.write_text(json.dumps(cfg))

        # main() does `import uvicorn` internally — patch sys.modules
        import sys as _sys
        mock_uvicorn = MagicMock()
        _sys.modules["uvicorn"] = mock_uvicorn

        try:
            with patch.object(relay_mod.sys, "argv", ["relay.py", "--config", str(cfg_path)]):
                relay_mod.main()
                # uvicorn.run should be called
                assert mock_uvicorn.run.call_count == 1
        finally:
            _sys.modules.pop("uvicorn", None)

    def test_default_args_runs_uvicorn(self):
        import relay.relay as relay_mod
        import sys as _sys
        mock_uvicorn = MagicMock()
        _sys.modules["uvicorn"] = mock_uvicorn

        try:
            with patch.object(relay_mod.sys, "argv", ["relay.py"]):
                relay_mod.main()
                assert mock_uvicorn.run.call_count == 1
        finally:
            _sys.modules.pop("uvicorn", None)
