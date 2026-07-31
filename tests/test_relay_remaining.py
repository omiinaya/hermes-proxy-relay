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

    def test_user_endpoint_non_200_skips(self, monkeypatch):
        """Auto-star with /user returning non-200 should skip silently."""
        import relay.relay as relay_mod
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

        mock_user = MagicMock()
        mock_user.status_code = 401  # auth failed

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_user
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = False

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=mock_client):
            import asyncio
            asyncio.run(relay_mod._auto_star())

        # Only the /user call — no star check, no put
        assert mock_client.get.call_count == 1
        assert mock_client.put.call_count == 0

    def test_star_put_non_204_is_silent(self, monkeypatch):
        """Auto-star PUT returning non-204 should log debug, not crash."""
        import relay.relay as relay_mod
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

        mock_user = MagicMock()
        mock_user.status_code = 200
        mock_user.json.return_value = {"login": "somebody-else"}

        mock_starred = MagicMock()
        mock_starred.status_code = 404  # not starred

        mock_put = MagicMock()
        mock_put.status_code = 500  # star API failed

        mock_client = AsyncMock()
        mock_client.get.side_effect = [mock_user, mock_starred]
        mock_client.put.return_value = mock_put
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = False

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=mock_client):
            import asyncio
            asyncio.run(relay_mod._auto_star())  # should not raise


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

    async def test_all_healthy_logs_info(self, cooldown_pool, caplog):
        """All proxies healthy → info log, no permanent failures recorded."""
        import logging
        import relay.relay as relay_mod
        relay_mod.pool = cooldown_pool

        success_client = AsyncMock()
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_client.get.return_value = success_resp
        success_client.__aenter__.return_value = success_client
        success_client.__aexit__.return_value = False

        with caplog.at_level(logging.INFO, logger="proxy-relay"):
            with patch.object(relay_mod.httpx, "AsyncClient", return_value=success_client):
                task = asyncio.create_task(relay_mod._proxy_health_check())
                await asyncio.sleep(0.15)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        stats = relay_mod.pool.stats()
        assert stats["permanently_failed"] == 0
        assert any("healthy" in r.message.lower() for r in caplog.records)

    async def test_permanently_dead_proxies_skipped(self, cooldown_pool, monkeypatch):
        """Permanently-dead proxies are skipped without a client connection."""
        import relay.relay as relay_mod
        relay_mod.pool = cooldown_pool

        # Mark the first proxy permanently dead
        relay_mod.pool._proxies[0].permanently_dead = True

        success_client = AsyncMock()
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_client.get.return_value = success_resp
        success_client.__aenter__.return_value = success_client
        success_client.__aexit__.return_value = False

        with patch.object(relay_mod.httpx, "AsyncClient", return_value=success_client) as mock_ctor:
            task = asyncio.create_task(relay_mod._proxy_health_check())
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # 3 live proxies checked (dead one skipped) — but AsyncClient is
        # called per-proxy so it should be exactly 3 (or fewer if loop raced)
        assert mock_ctor.call_count == 3

    async def test_health_check_loop_error_tolerated(self, cooldown_pool):
        """Unexpected exceptions inside the loop are caught and logged."""
        import relay.relay as relay_mod
        relay_mod.pool = cooldown_pool

        real_sleep = asyncio.sleep

        async def sleep_raises_once(delay):
            await real_sleep(delay)
            raise Exception("boom")

        calls = {"n": 0}

        async def flaky_sleep(delay):
            calls["n"] += 1
            if calls["n"] == 1:
                return await sleep_raises_once(delay)
            return await real_sleep(delay)

        with patch.object(relay_mod.asyncio, "sleep", side_effect=flaky_sleep):
            with patch.object(relay_mod, "logger") as mock_logger:
                task = asyncio.create_task(relay_mod._proxy_health_check())
                await real_sleep(0.15)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        assert any(
            "Health check error" in c.args[0]
            for c in mock_logger.error.call_args_list
        )


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

    def test_check_flag_exits_zero_with_valid_config(self, monkeypatch, tmp_path):
        """--check with valid config exits 0."""
        import relay.relay as relay_mod
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "https://test.example.com/v1")
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "test-key")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "socks5://u:p@h1:1080")

        with patch.object(relay_mod.sys, "argv", ["relay.py", "--check"]):
            with pytest.raises(SystemExit) as exc_info:
                relay_mod.main()
            assert exc_info.value.code == 0

    def test_check_flag_exits_nonzero_with_bad_config(self, monkeypatch):
        """--check with missing upstream exits 1 (fatal)."""
        import relay.relay as relay_mod
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "")

        with patch.object(relay_mod.sys, "argv", ["relay.py", "--check"]):
            with pytest.raises(SystemExit) as exc_info:
                relay_mod.main()
            assert exc_info.value.code == 1

    def test_check_flag_warns_but_ok_without_proxies(self, monkeypatch):
        """--check exits 0 with missing proxies (warning only)."""
        import relay.relay as relay_mod
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "https://test.example.com/v1")
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "test-key")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "")

        with patch.object(relay_mod.sys, "argv", ["relay.py", "--check"]):
            with pytest.raises(SystemExit) as exc_info:
                relay_mod.main()
            assert exc_info.value.code == 0

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


class TestRunConfigCheck:
    """_run_config_check() — --check mode validation report."""

    def test_reports_missing_api_key_warning(self, monkeypatch, capsys):
        """Missing UPSTREAM_API_KEY → warning (no error exit)."""
        import relay.relay as relay_mod
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "https://api.test.com/v1")
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "")
        monkeypatch.setattr(relay_mod, "UPSTREAM_AUTH_TYPE", "bearer")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "socks5://u:p@h1:1080")

        relay_mod._run_config_check()  # no SystemExit on warnings-only
        out = capsys.readouterr().out
        assert "UPSTREAM_API_KEY is empty" in out
        assert "Configuration OK." in out

    def test_reports_invalid_auth_type_error(self, monkeypatch, capsys):
        """Invalid UPSTREAM_AUTH_TYPE → error, exits 1."""
        import relay.relay as relay_mod
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "https://api.test.com/v1")
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "key")
        monkeypatch.setattr(relay_mod, "UPSTREAM_AUTH_TYPE", "digest")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "socks5://u:p@h1:1080")

        with pytest.raises(SystemExit) as exc_info:
            relay_mod._run_config_check()
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert "Invalid UPSTREAM_AUTH_TYPE" in out

    def test_reports_proxy_file_loaded(self, monkeypatch, capsys, tmp_path):
        """PROXY_LIST_FILE with proxies → check prints proxy count."""
        import relay.relay as relay_mod
        proxy_file = tmp_path / "proxies.txt"
        proxy_file.write_text("socks5://u:p@h1:1080\nsocks5://u:p@h2:1080\n")
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "https://api.test.com/v1")
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "key")
        monkeypatch.setattr(relay_mod, "UPSTREAM_AUTH_TYPE", "bearer")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", str(proxy_file))
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "")

        relay_mod._run_config_check()
        out = capsys.readouterr().out
        assert "Proxy file" in out
        assert "(2 proxies)" in out

    def test_ok_config_prints_success(self, monkeypatch, capsys):
        """Fully valid config → 'Configuration OK.' and exit 0."""
        import relay.relay as relay_mod
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "https://api.test.com/v1")
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "key")
        monkeypatch.setattr(relay_mod, "UPSTREAM_AUTH_TYPE", "bearer")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "socks5://u:p@h1:1080")

        relay_mod._run_config_check()
        assert "Configuration OK." in capsys.readouterr().out

    def test_reports_client_api_key_state(self, monkeypatch, capsys):
        """CLIENT_API_KEY set/unset shown in the check report."""
        import relay.relay as relay_mod
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "https://api.test.com/v1")
        monkeypatch.setattr(relay_mod, "UPSTREAM_API_KEY", "key")
        monkeypatch.setattr(relay_mod, "UPSTREAM_AUTH_TYPE", "bearer")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_FILE", "")
        monkeypatch.setattr(relay_mod, "PROXY_LIST_ENV", "socks5://u:p@h1:1080")

        monkeypatch.setattr(relay_mod, "CLIENT_API_KEY", "")
        relay_mod._run_config_check()
        out = capsys.readouterr().out
        assert "open proxy" in out  # warning shown when unset

        monkeypatch.setattr(relay_mod, "CLIENT_API_KEY", "s3cret")
        relay_mod._run_config_check()
        out = capsys.readouterr().out
        assert "CLIENT_API_KEY: set" in out  # confirmation when set


class TestPruneClientPool:
    """_prune_client_pool() — closes clients for removed proxies."""

    async def test_prunes_removed_proxies(self):
        import relay.relay as relay_mod
        # Reset pool state
        relay_mod._client_pool.clear()
        mock_client = AsyncMock()
        relay_mod._client_pool["socks5://old:1080"] = mock_client
        relay_mod._client_pool["socks5://keep:1080"] = AsyncMock()

        await relay_mod._prune_client_pool({"socks5://keep:1080"})

        assert "socks5://old:1080" not in relay_mod._client_pool
        assert "socks5://keep:1080" in relay_mod._client_pool
        mock_client.aclose.assert_awaited_once()

    async def test_prune_tolerates_aclose_error(self):
        """aclose() raising must not abort pruning of other clients."""
        import relay.relay as relay_mod
        relay_mod._client_pool.clear()
        bad_client = AsyncMock()
        bad_client.aclose.side_effect = Exception("close failed")
        relay_mod._client_pool["socks5://bad:1080"] = bad_client
        relay_mod._client_pool["socks5://also-bad:1080"] = AsyncMock()

        # Must not raise
        await relay_mod._prune_client_pool(set())

        assert "socks5://bad:1080" not in relay_mod._client_pool
        assert "socks5://also-bad:1080" not in relay_mod._client_pool


class TestMainGuard:
    """The `if __name__ == '__main__': main()` guard at the file bottom."""

    def test_main_guard_invokes_main(self, tmp_path, monkeypatch):
        """runpy execution of relay.py with no args runs main() (uvicorn patched)."""
        import runpy
        import sys as _sys
        import relay.relay as relay_mod

        mock_uvicorn = MagicMock()
        _sys.modules["uvicorn"] = mock_uvicorn
        # Patch argv so main() doesn't try to parse pytest's args
        monkeypatch.setattr(_sys, "argv", ["relay.py"])

        try:
            runpy.run_path(relay_mod.__file__, run_name="__main__")
            assert mock_uvicorn.run.call_count == 1
        finally:
            _sys.modules.pop("uvicorn", None)
