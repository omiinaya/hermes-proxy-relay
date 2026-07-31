"""Tests for the Hermes plugin and MCP server.

Covers:
- Plugin: _read_custom_providers filtering, _infer_auth_type, _write_relay_config,
  _write_proxied_provider, _cmd_setup list/clone, _handle_slash routing
- MCP: tool_status, tool_models, tool_config, tool_request_stats, tool_health
"""

import json
from pathlib import Path
from unittest.mock import patch

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

    def test_reset_all(self, plugin_mod):
        with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok", "proxies_total": 3}):
            result = plugin_mod._handle_slash("reset all")
        assert "All proxy cooldowns cleared" in result

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
