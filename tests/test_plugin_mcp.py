"""Tests for the Hermes plugin and MCP server.

Covers:
- Plugin: _read_custom_providers filtering, _infer_auth_type, _write_relay_config,
  _write_proxied_provider, _cmd_setup list/clone, _handle_slash routing
- MCP: tool_status, tool_models, tool_config, tool_request_stats, tool_health
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
# ═══════════════════════════════════════════════════════════════════
#  Plugin helpers
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def plugin_mod(tmp_path, monkeypatch):
    """Import plugin module with isolated HERMES_HOME."""
    import importlib
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("RELAY_PORT", "4002")
    import plugin as plugin_mod
    # Reset module-level paths to the isolated home
    importlib.reload(plugin_mod)
    return plugin_mod


class TestReadCustomProviders:
    def test_filters_proxied_and_relay(self, plugin_mod, tmp_path):
        """Entries pointing at the relay or named *-proxied are excluded."""
        cfg = {
            "custom_providers": [
                {"name": "openai", "base_url": "https://api.openai.com/v1", "api_key": "sk-123"},
                {"name": "relay", "base_url": "http://localhost:4002/v1", "api_key": "x"},
                {"name": "opencode-proxied", "base_url": "http://localhost:4002/v1", "api_key": "x"},
                {"name": "proxy-relay", "base_url": "http://localhost:4000/v1", "api_key": "x"},
            ],
        }
        config_path = tmp_path / "config.yaml"
        import yaml
        config_path.write_text(yaml.safe_dump(cfg))

        result = plugin_mod._read_custom_providers()
        names = [p["name"] for p in result]
        assert names == ["openai"]

    def test_empty_config_returns_empty(self, plugin_mod, tmp_path):
        result = plugin_mod._read_custom_providers()
        assert result == []

    def test_skips_non_dict_entries(self, plugin_mod, tmp_path):
        cfg = {
            "custom_providers": [
                {"name": "good", "base_url": "https://api.test.com/v1"},
                "not-a-dict",
                {"base_url": "https://no-name.com/v1"},  # missing name
            ],
        }
        import yaml
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
        result = plugin_mod._read_custom_providers()
        assert len(result) == 1
        assert result[0]["name"] == "good"


class TestInferAuthType:
    def test_opencode_hint(self):
        from plugin import _infer_auth_type
        assert _infer_auth_type({"name": "opencode-zen"}) == "x-api-key"
        assert _infer_auth_type({"name": "oc-zen"}) == "x-api-key"

    def test_public_key(self):
        from plugin import _infer_auth_type
        assert _infer_auth_type({"name": "anything", "api_key": "public"}) == "x-api-key"

    def test_default_bearer(self):
        from plugin import _infer_auth_type
        assert _infer_auth_type({"name": "openai", "api_key": "sk-123"}) == "bearer"


class TestMaskKey:
    """_mask_key must never reveal short API keys."""

    @pytest.fixture(autouse=True)
    def import_mask(self):
        from plugin._cmd_setup import _mask_key
        self._mask_key = _mask_key

    def test_empty(self):
        assert self._mask_key("") == "(none)"

    def test_short_key_fully_masked(self):
        """A 4-char key must never appear in output."""
        assert self._mask_key("abcd") == "****"
        assert "abcd" not in self._mask_key("abcd")

    def test_medium_key_partially_masked(self):
        """A 6-char key shows only 2 chars each side."""
        result = self._mask_key("abcdef")
        assert result == "ab...ef"
        assert "cde" not in result

    def test_long_key_masked(self):
        """A long key shows 6 prefix + 4 suffix."""
        result = self._mask_key("sk-abcdefghijklmnop")
        assert result == "sk-abc...mnop"
        assert "efghijk" not in result

    def test_list_does_not_leak_short_key(self, plugin_mod, tmp_path):
        """`setup list` must not print a short key in full."""
        import yaml
        cfg = {
            "custom_providers": [
                {"name": "shortkey-provider", "base_url": "http://localhost:4000/v1", "api_key": "secret"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
        result = plugin_mod._cmd_setup("setup list")
        assert "secret" not in result


class TestWriteRelayConfig:
    def test_writes_config_with_permissions(self, plugin_mod, tmp_path):
        path = plugin_mod._write_relay_config(
            "https://api.test.com/v1", "secret-key", "bearer", "/tmp/proxies.txt"
        )
        config_path = Path(path)
        assert config_path.exists()
        data = json.loads(config_path.read_text())
        assert data["UPSTREAM_BASE"] == "https://api.test.com/v1"
        assert data["UPSTREAM_API_KEY"] == "secret-key"
        assert data["UPSTREAM_AUTH_TYPE"] == "bearer"
        assert data["PROXY_LIST"] == "/tmp/proxies.txt"
        # Permissions must be 600 (secret file)
        assert config_path.stat().st_mode & 0o777 == 0o600


class TestConfigHelpers:
    """Plugin config/env helper functions."""

    def test_get_env_path_default(self, plugin_mod, tmp_path):
        """_get_env_path returns HERMES_HOME/.env by default."""
        result = plugin_mod._get_env_path()
        assert result == str(tmp_path / ".env")

    def test_env_val_reads_dotenv(self, plugin_mod, tmp_path):
        """.env values are read when env var not set."""
        (tmp_path / ".env").write_text('UPSTREAM_KEY="sk-from-dotenv"\n')
        result = plugin_mod._env_val("UPSTREAM_KEY")
        assert result == "sk-from-dotenv"

    def test_env_val_prefers_os_environ(self, plugin_mod, monkeypatch, tmp_path):
        """os.environ wins over .env file."""
        monkeypatch.setenv("MY_TEST_KEY", "from-env")
        (tmp_path / ".env").write_text("MY_TEST_KEY=from-file\n")
        assert plugin_mod._env_val("MY_TEST_KEY") == "from-env"

    def test_env_val_missing_returns_empty(self, plugin_mod, tmp_path):
        assert plugin_mod._env_val("NOPE_KEY") == ""

    def test_load_save_config_roundtrip(self, plugin_mod, tmp_path):
        """_save_config writes YAML that _load_config reads back."""
        cfg = {"custom_providers": [{"name": "test", "base_url": "https://x.com/v1"}]}
        plugin_mod._save_config(cfg)
        loaded = plugin_mod._load_config()
        assert loaded["custom_providers"][0]["name"] == "test"

    def test_load_config_missing_returns_empty(self, plugin_mod):
        assert plugin_mod._load_config() == {}

    def test_health_check_returns_none_on_failure(self, plugin_mod):
        import urllib.request as urlreq
        with patch.object(urlreq, "urlopen", side_effect=Exception("down")):
            assert plugin_mod._health_check() is None

    def test_relay_pid_returns_none_when_not_running(self, plugin_mod):
        import subprocess as sp
        with patch.object(sp, "run", return_value=MagicMock(returncode=1, stdout="")):
            assert plugin_mod._relay_pid() is None


class TestWriteProxiedProvider:
    def test_adds_proxied_entry(self, plugin_mod, tmp_path):
        entry = plugin_mod._write_proxied_provider("spacetimellm")
        assert entry["name"] == "spacetimellm-proxied"
        assert entry["base_url"] == "http://localhost:4002/v1"

        # Verify config.yaml was written
        import yaml
        cfg = yaml.safe_load((tmp_path / "config.yaml").read_text())
        providers = cfg["custom_providers"]
        assert len(providers) == 1
        assert providers[0]["name"] == "spacetimellm-proxied"

    def test_does_not_duplicate(self, plugin_mod, tmp_path):
        plugin_mod._write_proxied_provider("spacetimellm")
        plugin_mod._write_proxied_provider("spacetimellm")
        import yaml
        cfg = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert len(cfg["custom_providers"]) == 1

    def test_never_touches_original(self, plugin_mod, tmp_path):
        import yaml
        cfg = {
            "custom_providers": [
                {"name": "spacetimellm", "base_url": "http://localhost:4000/v1", "api_key": "orig"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))

        plugin_mod._write_proxied_provider("spacetimellm")
        cfg_after = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert len(cfg_after["custom_providers"]) == 2
        # Original untouched
        assert cfg_after["custom_providers"][0] == {
            "name": "spacetimellm", "base_url": "http://localhost:4000/v1", "api_key": "orig",
        }


# ═══════════════════════════════════════════════════════════════════
#  Plugin slash commands
# ═══════════════════════════════════════════════════════════════════


class TestCmdSetup:
    def test_list_empty(self, plugin_mod, tmp_path):
        result = plugin_mod._cmd_setup("setup list")
        assert "No `custom_providers` entries found" in result

    def test_list_with_providers(self, plugin_mod, tmp_path):
        import yaml
        cfg = {
            "custom_providers": [
                {"name": "spacetimellm", "base_url": "http://localhost:4000/v1", "api_key": "sk-abcdef1234567890"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
        result = plugin_mod._cmd_setup("setup list")
        assert "spacetimellm" in result
        assert "sk-abc" in result  # masked key

    def test_clone_invalid_index(self, plugin_mod, tmp_path):
        result = plugin_mod._cmd_setup("setup clone 99")
        assert "Invalid index" in result

    def test_clone_no_args(self, plugin_mod):
        result = plugin_mod._cmd_setup("setup clone")
        assert "Usage:" in result

    def test_clone_invalid_number(self, plugin_mod):
        result = plugin_mod._cmd_setup("setup clone abc")
        assert "Invalid number" in result

    def test_clone_success(self, plugin_mod, tmp_path):
        import yaml
        cfg = {
            "custom_providers": [
                {"name": "spacetimellm", "base_url": "http://localhost:4000/v1", "api_key": "sk-original"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
        result = plugin_mod._cmd_setup("setup clone 1")
        assert "Cloned: `spacetimellm` → `spacetimellm-proxied`" in result
        assert "config.json" in result
        # Verify config written
        config_path = tmp_path / "proxy-relay" / "config.json"
        assert config_path.exists()

    def test_overview_no_subcommand(self, plugin_mod, tmp_path):
        """/relay setup (no subcommand) shows the overview."""
        import sys
        # The `_cmd_setup` function shadows the submodule attribute on the
        # package — reach the module via sys.modules to patch its globals.
        cmd_setup_mod = sys.modules["plugin._cmd_setup"]
        with patch.object(cmd_setup_mod, "_health_check", return_value=None):
            with patch.object(cmd_setup_mod, "_relay_pid", return_value=None):
                result = plugin_mod._cmd_setup("setup")

        assert "Hermes Proxy Relay" in result
        assert "not running" in result
        assert "No `custom_providers`" in result


class TestHandleSlash:
    def test_unknown_command(self, plugin_mod):
        result = plugin_mod._handle_slash("frobnicate")
        assert "Unknown subcommand" in result

    def test_help_command(self, plugin_mod):
        result = plugin_mod._handle_slash("help")
        assert "Proxy Relay Commands" in result
        assert "/relay setup" in result

    def test_status_routes_to_cmd(self, plugin_mod):
        with patch.object(plugin_mod, "_health_check", return_value=None):
            with patch.object(plugin_mod, "_relay_pid", return_value=None):
                result = plugin_mod._handle_slash("status")
        assert "not running" in result

    def test_status_shows_version(self, plugin_mod):
        """_cmd_status includes relay version and uptime."""
        with patch.object(plugin_mod, "_health_check", return_value={
            "status": "ok",
            "version": "1.2.0",
            "uptime_seconds": 125,
            "upstream_base": "https://api.test.com/v1",
            "models_available": 5,
            "pool_stats": {
                "total": 3, "available": 2, "cooling": 1, "permanently_failed": 0,
                "cooling_details": [], "permanently_failed_details": [],
            },
            "request_stats": {"total": 10, "ok": 8, "errors": 2},
            "semaphore": {"used": 1, "max": 10},
        }):
            result = plugin_mod._cmd_status("status")

        assert "v1.2.0" in result
        assert "up 2m5s" in result

    def test_reset_all(self, plugin_mod):
        with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok", "proxies_total": 3}):
            result = plugin_mod._handle_slash("reset all")
        assert "All proxy cooldowns cleared" in result

    def test_reset_errors(self, plugin_mod):
        """`reset errors <threshold>` resets permanently-failed proxies."""
        with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok", "message": "Reset 2 proxies"}):
            result = plugin_mod._handle_slash("reset errors 5")
        assert "Reset permanently-failed proxies" in result

    def test_reset_errors_invalid_threshold(self, plugin_mod):
        result = plugin_mod._handle_slash("reset errors abc")
        assert "Invalid threshold" in result

    def test_reset_proxies(self, plugin_mod):
        """`reset proxies` reloads the proxy list."""
        with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok", "proxies_total": 4}):
            result = plugin_mod._handle_slash("reset proxies")
        assert "Proxy list reloaded" in result

    def test_reset_usage(self, plugin_mod):
        """`reset` with no args shows usage."""
        result = plugin_mod._handle_slash("reset")
        assert "Usage:" in result

    def test_switch_no_config(self, plugin_mod, tmp_path):
        """`switch upstream` with no config.json returns a helpful error."""
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            result = plugin_mod._cmd_switch("switch upstream https://x.com/v1")
        assert "No relay config found" in result

    def test_switch_unknown_subcommand(self, plugin_mod):
        result = plugin_mod._cmd_switch("switch nonsense")
        assert "Unknown subcommand" in result

    def test_admin_headers_with_key(self, plugin_mod, tmp_path):
        """_admin_headers includes X-Admin-Key when configured in config.json."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"ADMIN_API_KEY": "secret-admin"}))

        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            headers = plugin_mod._admin_headers()

        assert headers["X-Admin-Key"] == "secret-admin"

    def test_admin_headers_without_key(self, plugin_mod, tmp_path):
        """_admin_headers omits X-Admin-Key when not configured."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"UPSTREAM_BASE": "https://x.com/v1"}))

        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            headers = plugin_mod._admin_headers()

        assert "X-Admin-Key" not in headers

    def test_switch_upstream_writes_config(self, plugin_mod, tmp_path):
        """/relay switch upstream <url> should update config.json."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"UPSTREAM_BASE": "https://old.com/v1"}))

        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            result = plugin_mod._cmd_switch("switch upstream https://new.com/v1")

        assert "Upstream URL updated" in result
        data = json.loads(config_path.read_text())
        assert data["UPSTREAM_BASE"] == "https://new.com/v1"

    def test_switch_auth_writes_config(self, plugin_mod, tmp_path):
        """/relay switch auth x-api-key should update config.json."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"UPSTREAM_AUTH_TYPE": "bearer"}))

        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            result = plugin_mod._cmd_switch("switch auth x-api-key")

        assert "Auth type updated" in result
        data = json.loads(config_path.read_text())
        assert data["UPSTREAM_AUTH_TYPE"] == "x-api-key"

    def test_switch_auth_invalid_value(self, plugin_mod, tmp_path):
        """/relay switch auth <invalid> should reject the value."""
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            result = plugin_mod._cmd_switch("switch auth digest")

        assert "Invalid auth type" in result

    def test_logs_no_service(self, plugin_mod):
        """_cmd_logs returns a helpful message when no logs are found."""
        import subprocess as sp
        with patch.object(sp, "run", return_value=MagicMock(
            returncode=1, stdout="", stderr=""
        )):
            with patch.object(plugin_mod, "_relay_pid", return_value=None):
                result = plugin_mod._cmd_logs("logs")

        assert "No relay logs found" in result or "not running" in result.lower()

    def test_restart_no_systemd_no_pid(self, plugin_mod):
        """_cmd_restart falls back to manual instructions without systemd."""
        import subprocess as sp
        check = MagicMock(returncode=1, stdout="", stderr="")
        with patch.object(sp, "run", return_value=check):
            with patch.object(plugin_mod, "_relay_pid", return_value=None):
                result = plugin_mod._cmd_restart("restart")

        assert "not managed by systemd" in result or "Restart manually" in result


# ═══════════════════════════════════════════════════════════════════
#  MCP server tools
# ═══════════════════════════════════════════════════════════════════


class TestMcpTools:
    @pytest.fixture
    def mcp_mod(self, monkeypatch):
        import mcp.mcp_server as mcp_mod
        return mcp_mod

    def test_tool_status_unreachable(self, mcp_mod):
        with patch.object(mcp_mod, "_health_data", return_value=None):
            result = mcp_mod.tool_status()
        data = json.loads(result)
        assert data["status"] == "unreachable"

    def test_tool_status_ok(self, mcp_mod):
        with patch.object(mcp_mod, "_health_data", return_value={
            "status": "ok",
            "pool_stats": {"total": 3, "available": 2},
            "models_available": 5,
            "request_stats": {"total": 10, "ok": 8, "errors": 2},
            "semaphore": {"used": 1, "max": 10},
            "upstream_base": "https://api.test.com/v1",
            "uptime_seconds": 100,
            "version": "1.2.0",
        }):
            result = mcp_mod.tool_status()
        data = json.loads(result)
        assert data["status"] == "ok"
        assert data["pool"]["total"] == 3
        assert data["models"] == 5

    def test_tool_models(self, mcp_mod):
        with patch.object(mcp_mod, "_models_data", return_value={
            "object": "list",
            "data": [{"id": "gpt-4"}, {"id": "claude-3"}],
        }):
            result = mcp_mod.tool_models()
        data = json.loads(result)
        assert data["status"] == "ok"
        assert data["total"] == 2
        assert "claude-3" in data["models"]

    def test_tool_models_failure(self, mcp_mod):
        with patch.object(mcp_mod, "_models_data", return_value=None):
            result = mcp_mod.tool_models()
        data = json.loads(result)
        assert data["status"] == "error"

    def test_tool_request_stats_error_rate(self, mcp_mod):
        with patch.object(mcp_mod, "_health_data", return_value={
            "status": "ok",
            "request_stats": {"total": 100, "ok": 90, "errors": 10},
            "semaphore": {"used": 2, "max": 10},
        }):
            result = mcp_mod.tool_request_stats()
        data = json.loads(result)
        assert data["error_rate_pct"] == 10.0

    def test_tool_health_healthy(self, mcp_mod):
        with patch.object(mcp_mod, "_health_data", return_value={
            "status": "ok",
            "pool_stats": {"available": 2, "total": 3, "permanently_failed": 0},
            "request_stats": {"total": 5},
            "uptime_seconds": 60,
            "version": "1.2.0",
        }):
            result = mcp_mod.tool_health()
        data = json.loads(result)
        assert data["healthy"] is True
        assert data["available_proxies"] == 2

    def test_tool_health_unhealthy(self, mcp_mod):
        with patch.object(mcp_mod, "_health_data", return_value={
            "status": "degraded",
            "pool_stats": {"available": 0, "total": 3, "permanently_failed": 3},
            "request_stats": {"total": 5},
            "uptime_seconds": 60,
            "version": "1.2.0",
        }):
            result = mcp_mod.tool_health()
        data = json.loads(result)
        assert data["healthy"] is False
        assert data["permanently_failed"] == 3

    def test_tool_config(self, mcp_mod):
        with patch.object(mcp_mod, "_health_data", return_value={
            "status": "ok",
            "upstream_base": "https://api.test.com/v1",
            "pool_stats": {"total": 3, "available": 2, "cooling": 1, "permanently_failed": 0},
            "uptime_seconds": 120,
            "version": "1.2.0",
        }):
            result = mcp_mod.tool_config()
        data = json.loads(result)
        assert data["upstream"] == "https://api.test.com/v1"
        assert data["pool_total"] == 3

    def test_tool_clear_cooldowns(self, mcp_mod):
        with patch.object(mcp_mod, "_admin_post", return_value={"status": "ok", "proxies_total": 3}):
            result = mcp_mod.tool_clear_cooldowns()
        data = json.loads(result)
        assert data["status"] == "ok"

    def test_tool_reset_proxy(self, mcp_mod):
        with patch.object(mcp_mod, "_admin_post", return_value={"status": "ok"}):
            result = mcp_mod.tool_reset_proxy("socks5://test:1080")
        data = json.loads(result)
        assert data["status"] == "ok"

    def test_tool_upstream_health_ok(self, mcp_mod):
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"status":"ok","upstream":"https://x.com/v1"}'

        with patch.object(mcp_mod.urllib.request, "urlopen", return_value=mock_resp):
            result = mcp_mod.tool_upstream_health()
        data = json.loads(result)
        assert data["status"] == "ok"

    def test_tool_upstream_health_http_error(self, mcp_mod):
        err = mcp_mod.urllib.error.HTTPError("url", 503, "down", {}, None)
        err.read = lambda: b'{"status":"error","upstream_status":503}'

        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=err):
            result = mcp_mod.tool_upstream_health()
        data = json.loads(result)
        assert data["status"] == "error"

    def test_tool_reload_proxies(self, mcp_mod):
        with patch.object(mcp_mod, "_admin_post", return_value={"status": "ok", "proxies_total": 4}):
            result = mcp_mod.tool_reload_proxies()
        data = json.loads(result)
        assert data["status"] == "ok"
        assert data["proxies_total"] == 4

    def test_tool_reset_by_errors(self, mcp_mod):
        with patch.object(mcp_mod, "_admin_post", return_value={"status": "ok", "message": "Reset 2 proxies"}):
            result = mcp_mod.tool_reset_by_errors(5)
        data = json.loads(result)
        assert data["status"] == "ok"

    def test_admin_post_http_error(self, mcp_mod):
        """_admin_post should return the error body JSON on HTTPError."""
        err = mcp_mod.urllib.error.HTTPError("url", 403, "forbidden", {}, None)
        err.read = lambda: b'{"error":"Invalid or missing admin key"}'

        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=err):
            result = mcp_mod._admin_post("/admin/clear-cooldowns")
        assert result["error"] == "Invalid or missing admin key"

    def test_admin_post_connection_error(self, mcp_mod):
        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=ConnectionError("refused")):
            result = mcp_mod._admin_post("/admin/clear-cooldowns")
        assert result["status"] == "error"

    def test_health_data_returns_none_on_error(self, mcp_mod):
        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=Exception("down")):
            assert mcp_mod._health_data() is None


class TestPluginRegistration:
    def test_register_adds_command(self, plugin_mod):
        """register() should register the 'relay' command with the ctx."""
        ctx = MagicMock()
        plugin_mod.register(ctx)
        ctx.register_command.assert_called_once()
        call_args = ctx.register_command.call_args
        assert call_args[0][0] == "relay"  # positional: command name
        assert call_args[1]["handler"] == plugin_mod._handle_slash
        assert "proxy relay" in call_args[1]["description"].lower()
