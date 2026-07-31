"""Tests for the Hermes plugin and MCP server.

Covers:
- Plugin: _read_custom_providers filtering, _infer_auth_type, _write_relay_config,
  _write_proxied_provider, _cmd_setup list/clone, _handle_slash routing
- MCP: tool_status, tool_models, tool_config, tool_request_stats, tool_health
"""

import json
import urllib.error
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
        path, client_key = plugin_mod._write_relay_config(
            "https://api.test.com/v1", "secret-key", "bearer", "/tmp/proxies.txt"
        )
        config_path = Path(path)
        assert config_path.exists()
        data = json.loads(config_path.read_text())
        assert data["UPSTREAM_BASE"] == "https://api.test.com/v1"
        assert data["UPSTREAM_API_KEY"] == "secret-key"
        assert data["UPSTREAM_AUTH_TYPE"] == "bearer"
        assert data["PROXY_LIST"] == "/tmp/proxies.txt"
        # Client key generated and stored
        assert data["CLIENT_API_KEY"]
        assert len(client_key) == 32  # token_hex(16)
        assert data["CLIENT_API_KEY"] == client_key
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

    def test_relay_pid_returns_first_pid(self, plugin_mod):
        import subprocess as sp
        with patch.object(sp, "run", return_value=MagicMock(returncode=0, stdout="1234\n5678\n")):
            assert plugin_mod._relay_pid() == 1234

    def test_relay_pid_tolerates_subprocess_error(self, plugin_mod):
        import subprocess as sp
        with patch.object(sp, "run", side_effect=Exception("pgrep missing")):
            assert plugin_mod._relay_pid() is None

    def test_health_check_returns_json(self, plugin_mod):
        import urllib.request as urlreq
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"status":"ok"}'
        with patch.object(urlreq, "urlopen", return_value=mock_resp):
            assert plugin_mod._health_check() == {"status": "ok"}

    def test_admin_headers_tolerates_corrupt_config(self, plugin_mod, tmp_path):
        """Corrupt config.json must not raise in _admin_headers."""
        (tmp_path / "proxy-relay").mkdir(exist_ok=True)
        (tmp_path / "proxy-relay" / "config.json").write_text("{ broken !!!")
        result = plugin_mod._admin_headers()
        assert "X-Admin-Key" not in result

    def test_admin_post_success(self, plugin_mod):
        import urllib.request as urlreq
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"status":"ok"}'
        with patch.object(urlreq, "urlopen", return_value=mock_resp):
            assert plugin_mod._admin_post("/admin/clear-cooldowns") == {"status": "ok"}

    def test_admin_post_returns_none_on_error(self, plugin_mod):
        import urllib.request as urlreq
        with patch.object(urlreq, "urlopen", side_effect=Exception("down")):
            assert plugin_mod._admin_post("/admin/clear-cooldowns") is None

    def test_get_env_path_empty_string_falls_back(self, plugin_mod, monkeypatch):
        """HERMES_ENV_PATH='' (empty) falls back to HERMES_HOME/.env."""
        monkeypatch.setenv("HERMES_ENV_PATH", "''")
        assert plugin_mod._get_env_path() == str(plugin_mod.Path(plugin_mod.HERMES_HOME) / ".env")

    def test_env_val_ignores_comment_lines(self, plugin_mod, tmp_path):
        (tmp_path / ".env").write_text("# comment line\nREAL_KEY=val123\n")
        assert plugin_mod._env_val("REAL_KEY") == "val123"
        assert plugin_mod._env_val("# comment") == ""


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
        assert "Client auth enabled" in result  # relay-only key generated
        assert "switch clientkey" in result  # rotation documented
        # Verify config written
        config_path = tmp_path / "proxy-relay" / "config.json"
        assert config_path.exists()
        import json as _json
        relay_cfg = _json.loads(config_path.read_text())
        # Client key generated for relay auth
        assert relay_cfg["CLIENT_API_KEY"]

        # The proxied provider entry uses the same key — Hermes authenticates
        # to the relay with it
        cfg_after = yaml.safe_load((tmp_path / "config.yaml").read_text())
        proxied = [p for p in cfg_after["custom_providers"] if p["name"] == "spacetimellm-proxied"]
        assert len(proxied) == 1
        assert proxied[0]["api_key"] == relay_cfg["CLIENT_API_KEY"]

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

    def test_clone_with_auth_override(self, plugin_mod, tmp_path):
        """`setup clone <N> x-api-key` overrides the inferred auth type."""
        import yaml
        cfg = {
            "custom_providers": [
                {"name": "myprovider", "base_url": "https://api.example.com/v1", "api_key": "sk-abc"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
        result = plugin_mod._cmd_setup("setup clone 1 x-api-key")
        assert "Auth type: `x-api-key`" in result
        import json
        relay_cfg = json.loads((tmp_path / "proxy-relay" / "config.json").read_text())
        assert relay_cfg["UPSTREAM_AUTH_TYPE"] == "x-api-key"

    def test_clone_relay_config_write_error(self, plugin_mod, tmp_path):
        """Failure writing relay config → error message."""
        import sys
        import yaml
        cfg = {
            "custom_providers": [
                {"name": "myprovider", "base_url": "https://api.example.com/v1", "api_key": "sk-abc"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
        cmd_setup_mod = sys.modules["plugin._cmd_setup"]
        with patch.object(cmd_setup_mod, "_write_relay_config", side_effect=OSError("disk full")):
            result = plugin_mod._cmd_setup("setup clone 1")
        assert "Failed to write relay config" in result

    def test_clone_provider_entry_write_error(self, plugin_mod, tmp_path):
        """Failure writing the Hermes provider entry → error message."""
        import sys
        import yaml
        cfg = {
            "custom_providers": [
                {"name": "myprovider", "base_url": "https://api.example.com/v1", "api_key": "sk-abc"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
        cmd_setup_mod = sys.modules["plugin._cmd_setup"]
        with patch.object(cmd_setup_mod, "_write_proxied_provider", side_effect=OSError("permission denied")):
            result = plugin_mod._cmd_setup("setup clone 1")
        assert "Failed to write Hermes provider entry" in result

    def test_overview_relay_running(self, plugin_mod, tmp_path):
        """Overview shows relay running with pool stats when health check passes."""
        import sys
        cmd_setup_mod = sys.modules["plugin._cmd_setup"]
        with patch.object(cmd_setup_mod, "_health_check", return_value={
            "status": "ok",
            "pool_stats": {"total": 4, "available": 3},
        }):
            result = plugin_mod._cmd_setup("setup")
        assert "Relay **running** on :4002" in result
        assert "3/4 proxies available" in result

    def test_overview_pid_exists_but_health_down(self, plugin_mod, tmp_path):
        """Overview reports PID exists when health is unreachable."""
        import sys
        cmd_setup_mod = sys.modules["plugin._cmd_setup"]
        with patch.object(cmd_setup_mod, "_health_check", return_value=None):
            with patch.object(cmd_setup_mod, "_relay_pid", return_value=5555):
                result = plugin_mod._cmd_setup("setup")
        assert "PID 5555 exists but health unreachable" in result

    def test_overview_lists_providers_and_relay_config(self, plugin_mod, tmp_path):
        """Overview shows provider count, names, and relay upstream."""
        import yaml
        cfg = {
            "custom_providers": [
                {"name": "p1", "base_url": "https://api1.com/v1", "api_key": "sk-1"},
                {"name": "p2", "base_url": "https://api2.com/v1", "api_key": "sk-2"},
                {"name": "p3", "base_url": "https://api3.com/v1", "api_key": "sk-3"},
                {"name": "p4", "base_url": "https://api4.com/v1", "api_key": "sk-4"},
                {"name": "p5", "base_url": "https://api5.com/v1", "api_key": "sk-5"},
                {"name": "p6", "base_url": "https://api6.com/v1", "api_key": "sk-6"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
        relay_dir = tmp_path / "proxy-relay"
        relay_dir.mkdir(exist_ok=True)
        import json
        (relay_dir / "config.json").write_text(
            json.dumps({"UPSTREAM_BASE": "https://relay-target.com/v1", "UPSTREAM_API_KEY": "sk-1"})
        )
        import sys
        cmd_setup_mod = sys.modules["plugin._cmd_setup"]
        with patch.object(cmd_setup_mod, "_health_check", return_value=None):
            # The submodule caches RELAY_CONFIG_DIR at import — point it at the
            # isolated home so the relay-config-status branch executes.
            with patch.object(cmd_setup_mod, "RELAY_CONFIG_DIR", relay_dir):
                result = plugin_mod._cmd_setup("setup")

        assert "6 existing providers" in result
        assert "p1" in result
        assert "... and 1 more" in result
        # Relay config status line comes from config.json (distinct from the
        # provider-list URL, proving the branch ran)
        assert "https://relay-target.com/v1" in result

    def test_overview_corrupt_relay_config_tolerated(self, plugin_mod, tmp_path):
        """Corrupt relay config.json in overview → no crash."""
        import sys
        relay_dir = tmp_path / "proxy-relay"
        relay_dir.mkdir(exist_ok=True)
        (relay_dir / "config.json").write_text("{ nope !!!")
        cmd_setup_mod = sys.modules["plugin._cmd_setup"]
        with patch.object(cmd_setup_mod, "_health_check", return_value=None):
            with patch.object(cmd_setup_mod, "RELAY_CONFIG_DIR", relay_dir):
                result = plugin_mod._cmd_setup("setup")
        assert "Hermes Proxy Relay" in result


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

    def test_status_pid_exists_but_health_down(self, plugin_mod):
        """Health unreachable but PID exists → specific diagnostic message."""
        with patch.object(plugin_mod, "_health_check", return_value=None):
            with patch.object(plugin_mod, "_relay_pid", return_value=4321):
                result = plugin_mod._cmd_status("status")
        assert "PID 4321 exists but health endpoint unreachable" in result

    def test_status_shows_cooldown_and_failed_details(self, plugin_mod):
        """_cmd_status renders cooling + permanently-failed detail sections."""
        with patch.object(plugin_mod, "_health_check", return_value={
            "status": "ok",
            "version": "1.3.0",
            "uptime_seconds": 3600,
            "upstream_base": "https://api.test.com/v1",
            "models_available": 5,
            "pool_stats": {
                "total": 3, "available": 1, "cooling": 1, "permanently_failed": 1,
                "cooling_details": [
                    {"proxy": "socks5://user:pass@host1:1080", "remaining_s": 45},
                ],
                "permanently_failed_details": [
                    {"proxy": "socks5://user:pass@host2:1080", "last_error": "429 Too Many Requests", "total_429": 12},
                ],
            },
            "request_stats": {"total": 10, "ok": 8, "errors": 2},
            "semaphore": {"used": 1, "max": 10},
        }):
            result = plugin_mod._cmd_status("status")

        assert "1 proxies permanently failed" in result
        assert "Temporary cooling (1)" in result
        assert "45s remaining" in result
        assert "Permanently failed (1)" in result
        assert "429s: 12" in result
        # host part of URL is shown, credentials masked via split("@")
        assert "host1" in result
        assert "user:pass" not in result

    def test_status_shows_version_with_hour_uptime(self, plugin_mod):
        """Uptime formatting handles hours (h/m/s)."""
        with patch.object(plugin_mod, "_health_check", return_value={
            "status": "ok",
            "version": "1.0.0",
            "uptime_seconds": 3661,
            "upstream_base": "https://api.test.com/v1",
            "models_available": 1,
            "pool_stats": {"total": 1, "available": 1, "cooling": 0, "permanently_failed": 0},
            "request_stats": {"total": 1, "ok": 1, "errors": 0},
            "semaphore": {},
        }):
            result = plugin_mod._cmd_status("status")
        assert "up 1h1m1s" in result

    def test_status_shows_version(self, plugin_mod):
        """_cmd_status includes relay version and uptime."""
        with patch.object(plugin_mod, "_health_check", return_value={
            "status": "ok",
            "version": "1.3.0",
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

        assert "v1.3.0" in result
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

    def test_reset_all_failure(self, plugin_mod):
        """`reset all` when relay is down → failure message."""
        with patch.object(plugin_mod, "_admin_post", return_value=None):
            result = plugin_mod._handle_slash("reset all")
        assert "Failed to clear cooldowns" in result

    def test_reset_errors_failure(self, plugin_mod):
        """`reset errors` when relay is down → failure message."""
        with patch.object(plugin_mod, "_admin_post", return_value=None):
            result = plugin_mod._handle_slash("reset errors")
        assert "Failed to reset" in result

    def test_reset_proxies_failure(self, plugin_mod):
        """`reset proxies` when relay is down → failure message."""
        with patch.object(plugin_mod, "_admin_post", return_value=None):
            result = plugin_mod._handle_slash("reset proxies")
        assert "Failed to reload proxies" in result

    def test_reset_specific_proxy_failure(self, plugin_mod):
        """`reset <url>` when admin returns an error dict → surfaces error."""
        with patch.object(plugin_mod, "_admin_post", return_value={"status": "error", "error": "Proxy not found"}):
            result = plugin_mod._handle_slash("reset socks5://1.2.3.4:1080")
        assert "Proxy not found" in result

    def test_reset_specific_proxy_unreachable(self, plugin_mod):
        """`reset <url>` when relay is down → generic message."""
        with patch.object(plugin_mod, "_admin_post", return_value=None):
            result = plugin_mod._handle_slash("reset socks5://1.2.3.4:1080")
        assert "Relay unreachable" in result

    def test_reset_specific_proxy_success(self, plugin_mod):
        with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok"}):
            result = plugin_mod._handle_slash("reset socks5://1.2.3.4:1080")
        assert "Proxy reset" in result

    def test_switch_no_config(self, plugin_mod, tmp_path):
        """`switch upstream` with no config.json returns a helpful error."""
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            result = plugin_mod._cmd_switch("switch upstream https://x.com/v1")
        assert "No relay config found" in result

    def test_switch_unknown_subcommand(self, plugin_mod):
        result = plugin_mod._cmd_switch("switch nonsense")
        assert "Unknown subcommand" in result

    def test_handle_slash_aliases(self, plugin_mod):
        """Alias subcommands route to the same handlers."""
        with patch.object(plugin_mod, "_cmd_setup", return_value="setup-ok") as m_setup:
            assert plugin_mod._handle_slash("install list") == "setup-ok"
            assert plugin_mod._handle_slash("init list") == "setup-ok"
            assert plugin_mod._handle_slash("config list") == "setup-ok"
        assert m_setup.call_count == 3

        with patch.object(plugin_mod, "_cmd_switch", return_value="switch-ok") as m_switch:
            assert plugin_mod._handle_slash("change upstream https://x/v1") == "switch-ok"
        assert m_switch.call_count == 1

        with patch.object(plugin_mod, "_cmd_logs", return_value="logs-ok") as m_logs:
            assert plugin_mod._handle_slash("log") == "logs-ok"
            assert plugin_mod._handle_slash("journal") == "logs-ok"
        assert m_logs.call_count == 2

        with patch.object(plugin_mod, "_cmd_restart", return_value="restart-ok") as m_restart:
            assert plugin_mod._handle_slash("reboot") == "restart-ok"
        assert m_restart.call_count == 1

        with patch.object(plugin_mod, "_cmd_status", return_value="status-ok") as m_status:
            assert plugin_mod._handle_slash("health") == "status-ok"
            assert plugin_mod._handle_slash("info") == "status-ok"
        assert m_status.call_count == 2

        with patch.object(plugin_mod, "_cmd_reset", return_value="reset-ok") as m_reset:
            assert plugin_mod._handle_slash("clear all") == "reset-ok"
            assert plugin_mod._handle_slash("reload proxies") == "reset-ok"
        assert m_reset.call_count == 2

    def test_handle_slash_empty_routes_to_status(self, plugin_mod):
        with patch.object(plugin_mod, "_cmd_status", return_value="status-ok") as m_status:
            assert plugin_mod._handle_slash("") == "status-ok"
        assert m_status.call_count == 1

    def test_switch_auth_write_error(self, plugin_mod, tmp_path):
        """`switch auth` when config write fails → error message."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"UPSTREAM_AUTH_TYPE": "bearer"}))
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            with patch("json.dumps", side_effect=Exception("write failed")):
                result = plugin_mod._cmd_switch("switch auth x-api-key")
        assert "Failed to update config" in result

    def test_switch_usage(self, plugin_mod):
        """`switch` with no args shows usage."""
        result = plugin_mod._cmd_switch("switch")
        assert "Usage:" in result

    def test_switch_upstream_write_error(self, plugin_mod, tmp_path):
        """`switch upstream` when config write fails → error message."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"UPSTREAM_BASE": "https://old.com/v1"}))
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            with patch("json.dumps", side_effect=Exception("write failed")):
                result = plugin_mod._cmd_switch("switch upstream https://new.com/v1")
        assert "Failed to update config" in result

    def test_switch_auth_hot_reloads_when_running(self, plugin_mod, tmp_path):
        """When the relay is up, switch auth hot-reloads (no restart)."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"UPSTREAM_AUTH_TYPE": "bearer"}))
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok"}):
                result = plugin_mod._cmd_switch("switch auth x-api-key")
        assert "hot-reloaded" in result
        assert "no restart needed" in result

    def test_switch_auth_no_config(self, plugin_mod, tmp_path):
        """`switch auth` with no config.json → helpful error."""
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            result = plugin_mod._cmd_switch("switch auth x-api-key")
        assert "No relay config found" in result

    def test_switch_proxies_reloads(self, plugin_mod):
        """`switch proxies` reloads the proxy list from file."""
        with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok", "proxies_total": 6}):
            result = plugin_mod._cmd_switch("switch proxies")
        assert "Proxy list reloaded" in result
        assert "6" in result

    def test_switch_proxies_failure(self, plugin_mod):
        """`switch proxies` when relay is down → failure message."""
        with patch.object(plugin_mod, "_admin_post", return_value=None):
            result = plugin_mod._cmd_switch("switch proxies")
        assert "Failed to reload proxies" in result

    def test_switch_clientkey_rotates(self, plugin_mod, tmp_path):
        """`switch clientkey` rotates the key in config.json and the proxied entry."""
        import yaml
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"CLIENT_API_KEY": "old-key", "UPSTREAM_BASE": "https://x"}))

        cfg = {
            "custom_providers": [
                {"name": "myprovider-proxied", "base_url": "http://localhost:4002/v1", "api_key": "old-key"},
                {"name": "other", "base_url": "https://other.com/v1", "api_key": "keep-me"},
            ],
        }
        (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))

        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok"}):
                result = plugin_mod._cmd_switch("switch clientkey")

        assert "rotated" in result.lower()
        new_cfg = json.loads(config_path.read_text())
        assert new_cfg["CLIENT_API_KEY"] != "old-key"
        assert len(new_cfg["CLIENT_API_KEY"]) == 32
        # Proxied entry updated, unrelated entry untouched
        cfg_after = yaml.safe_load((tmp_path / "config.yaml").read_text())
        providers = {p["name"]: p for p in cfg_after["custom_providers"]}
        assert providers["myprovider-proxied"]["api_key"] == new_cfg["CLIENT_API_KEY"]
        assert providers["other"]["api_key"] == "keep-me"

    def test_switch_clientkey_no_config(self, plugin_mod, tmp_path):
        """`switch clientkey` with no config.json → helpful error."""
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            result = plugin_mod._cmd_switch("switch clientkey")
        assert "No relay config found" in result

    def test_switch_clientkey_error(self, plugin_mod, tmp_path):
        """`switch clientkey` write failure → error message."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"CLIENT_API_KEY": "old"}))
        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            with patch("json.dumps", side_effect=Exception("write failed")):
                result = plugin_mod._cmd_switch("switch clientkey")
        assert "Failed to rotate client key" in result

    def test_switch_clientkey_rotates_without_relay(self, plugin_mod, tmp_path):
        """`switch clientkey` when relay is down → still rotates, notes restart."""
        import yaml
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"CLIENT_API_KEY": "old-key"}))
        (tmp_path / "config.yaml").write_text(yaml.safe_dump({
            "custom_providers": [
                {"name": "p-proxied", "base_url": "http://localhost:4002/v1", "api_key": "old-key"},
            ],
        }))

        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            with patch.object(plugin_mod, "_admin_post", return_value=None):
                result = plugin_mod._cmd_switch("switch clientkey")

        assert "rotated" in result.lower()
        assert "Relay not running" in result

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

    def test_switch_upstream_hot_reloads_when_running(self, plugin_mod, tmp_path):
        """When the relay is up, switch upstream hot-reloads (no restart)."""
        config_path = tmp_path / "proxy-relay" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"UPSTREAM_BASE": "https://old.com/v1"}))

        with patch.object(plugin_mod, "RELAY_CONFIG_DIR", tmp_path / "proxy-relay"):
            with patch.object(plugin_mod, "_admin_post", return_value={"status": "ok", "upstream_base": "https://new.com/v1"}):
                result = plugin_mod._cmd_switch("switch upstream https://new.com/v1")

        assert "hot-reloaded" in result
        assert "no restart needed" in result

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

    def test_logs_success(self, plugin_mod):
        """_cmd_logs returns relay-relevant journalctl lines."""
        import subprocess as sp
        stdout = (
            "2026-07-30T10:00:00Z host proxy-relay[123]: started\n"
            "2026-07-30T10:00:05Z host python[123]: 429 Too Many Requests\n"
            "2026-07-30T10:00:10Z host python[123]: some other line\n"
        )
        with patch.object(sp, "run", return_value=MagicMock(returncode=0, stdout=stdout, stderr="")):
            result = plugin_mod._cmd_logs("logs")
        assert "Recent Relay Logs" in result
        assert "429 Too Many Requests" in result
        assert "some other line" not in result  # filtered out

    def test_logs_no_relevant_lines_falls_back(self, plugin_mod):
        """journalctl output with no relay-relevant lines → shows first 10."""
        import subprocess as sp
        stdout = "".join(f"line {i}\n" for i in range(15))
        with patch.object(sp, "run", return_value=MagicMock(returncode=0, stdout=stdout, stderr="")):
            result = plugin_mod._cmd_logs("logs")
        assert "Recent Relay Logs" in result
        assert "line 0" in result

    def test_logs_fallback_by_pid(self, plugin_mod):
        """When systemd unit lookup fails, _cmd_logs falls back to journalctl by PID."""
        import subprocess as sp
        first = MagicMock(returncode=1, stdout="", stderr="")  # unit lookup fails
        second = MagicMock(returncode=0, stdout="2026-07-30T10:00:00Z host python[777]: relay line\n", stderr="")
        with patch.object(sp, "run", side_effect=[first, second]):
            with patch.object(plugin_mod, "_relay_pid", return_value=777):
                result = plugin_mod._cmd_logs("logs")
        assert "Recent Relay Logs" in result
        assert "relay line" in result

    def test_logs_exception(self, plugin_mod):
        """_cmd_logs catches unexpected errors."""
        import subprocess as sp
        with patch.object(sp, "run", side_effect=Exception("boom")):
            result = plugin_mod._cmd_logs("logs")
        assert "Failed to read logs" in result

    def test_restart_no_systemd_no_pid(self, plugin_mod):
        """_cmd_restart falls back to manual instructions without systemd."""
        import subprocess as sp
        check = MagicMock(returncode=1, stdout="", stderr="")
        with patch.object(sp, "run", return_value=check):
            with patch.object(plugin_mod, "_relay_pid", return_value=None):
                result = plugin_mod._cmd_restart("restart")

        assert "not managed by systemd" in result or "Restart manually" in result

    def test_restart_kills_pid_when_no_systemd(self, plugin_mod):
        """_cmd_restart kills the bare relay process when no systemd service."""
        import subprocess as sp
        check = MagicMock(returncode=1, stdout="", stderr="")
        with patch.object(sp, "run", return_value=check):
            with patch.object(plugin_mod, "_relay_pid", return_value=9999):
                with patch.object(sp, "run", side_effect=check):
                    result = plugin_mod._cmd_restart("restart")
        assert "PID 9999" in result
        assert "killed" in result

    def test_restart_systemd_success_healthy(self, plugin_mod):
        """_cmd_restart via systemd with health check passing → success."""
        import subprocess as sp
        check = MagicMock(returncode=0, stdout="active", stderr="")
        restart = MagicMock(returncode=0, stdout="", stderr="")
        with patch.object(sp, "run", side_effect=[check, restart]):
            with patch.object(plugin_mod, "_health_check", return_value={"status": "ok"}):
                result = plugin_mod._cmd_restart("restart")
        assert "Relay restarted successfully" in result

    def test_restart_systemd_success_unhealthy(self, plugin_mod):
        """_cmd_restart via systemd when health not yet up → wait message."""
        import subprocess as sp
        check = MagicMock(returncode=0, stdout="active", stderr="")
        restart = MagicMock(returncode=0, stdout="", stderr="")
        with patch.object(sp, "run", side_effect=[check, restart]):
            with patch.object(plugin_mod, "_health_check", return_value=None):
                result = plugin_mod._cmd_restart("restart")
        assert "Restart command sent" in result

    def test_restart_systemd_failure(self, plugin_mod):
        """_cmd_restart when systemctl restart fails → error."""
        import subprocess as sp
        check = MagicMock(returncode=0, stdout="active", stderr="")
        restart = MagicMock(returncode=1, stdout="", stderr="unit not found")
        with patch.object(sp, "run", side_effect=[check, restart]):
            result = plugin_mod._cmd_restart("restart")
        assert "Failed to restart" in result
        assert "unit not found" in result

    def test_restart_timeout(self, plugin_mod):
        """_cmd_restart handles subprocess.TimeoutExpired."""
        import subprocess as sp
        with patch.object(sp, "run", side_effect=sp.TimeoutExpired("systemctl", 30)):
            result = plugin_mod._cmd_restart("restart")
        assert "timed out" in result

    def test_restart_unknown_error(self, plugin_mod):
        """_cmd_restart catches unexpected errors."""
        import subprocess as sp
        with patch.object(sp, "run", side_effect=Exception("boom")):
            result = plugin_mod._cmd_restart("restart")
        assert "Failed to restart" in result


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
            "version": "1.3.0",
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
            "version": "1.3.0",
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
            "version": "1.3.0",
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
            "version": "1.3.0",
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
        err = urllib.error.HTTPError("url", 502, "Bad Gateway", {}, None)
        err.read = lambda: b'{"status":"error","message":"upstream down"}'
        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=err):
            result = mcp_mod.tool_upstream_health()
        data = json.loads(result)
        assert data["status"] == "error"
        assert data["message"] == "upstream down"

    def test_tool_upstream_health_http_error_unparseable(self, mcp_mod):
        err = urllib.error.HTTPError("url", 503, "Service Unavailable", {}, None)
        err.read = lambda: b"<html>bad gateway</html>"
        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=err):
            result = mcp_mod.tool_upstream_health()
        data = json.loads(result)
        assert data["status"] == "error"
        assert data["message"] == "HTTP 503"

    def test_tool_upstream_health_connection_error(self, mcp_mod):
        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=TimeoutError("timed out")):
            result = mcp_mod.tool_upstream_health()
        data = json.loads(result)
        assert data["status"] == "unreachable"
        assert "timed out" in data["error"]

    def test_tool_config_unreachable(self, mcp_mod):
        with patch.object(mcp_mod, "_health_data", return_value=None):
            result = mcp_mod.tool_config()
        data = json.loads(result)
        assert data["status"] == "unreachable"

    def test_tool_request_stats_unreachable(self, mcp_mod):
        with patch.object(mcp_mod, "_health_data", return_value=None):
            result = mcp_mod.tool_request_stats()
        data = json.loads(result)
        assert data["status"] == "unreachable"

    def test_tool_health_unreachable_no_data(self, mcp_mod):
        """tool_health with _health_data() None → unhealthy + connection refused."""
        with patch.object(mcp_mod, "_health_data", return_value=None):
            result = mcp_mod.tool_health()
        data = json.loads(result)
        assert data["healthy"] is False
        assert "Connection refused" in data["error"]

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

    def test_admin_post_http_error_unparseable_body(self, mcp_mod):
        """HTTPError with non-JSON body → structured error with code+reason."""
        err = mcp_mod.urllib.error.HTTPError("url", 502, "Bad Gateway", {}, None)
        err.read = lambda: b"<html>bad gateway</html>"

        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=err):
            result = mcp_mod._admin_post("/admin/clear-cooldowns")
        assert result["status"] == "error"
        assert result["message"] == "HTTP 502: Bad Gateway"

    def test_admin_post_connection_error(self, mcp_mod):
        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=ConnectionError("refused")):
            result = mcp_mod._admin_post("/admin/clear-cooldowns")
        assert result["status"] == "error"

    def test_health_data_returns_none_on_error(self, mcp_mod):
        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=Exception("down")):
            assert mcp_mod._health_data() is None

    def test_health_data_parses_json(self, mcp_mod):
        """_health_data returns parsed JSON from the relay."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"status":"ok"}'
        with patch.object(mcp_mod.urllib.request, "urlopen", return_value=mock_resp):
            assert mcp_mod._health_data() == {"status": "ok"}

    def test_models_data_parses_json(self, mcp_mod):
        """_models_data returns parsed JSON from the relay."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"object":"list","data":[]}'
        with patch.object(mcp_mod.urllib.request, "urlopen", return_value=mock_resp):
            assert mcp_mod._models_data() == {"object": "list", "data": []}

    def test_models_data_returns_none_on_error(self, mcp_mod):
        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=Exception("down")):
            assert mcp_mod._models_data() is None

    def test_admin_post_uses_admin_key_from_config(self, mcp_mod, tmp_path, monkeypatch):
        """_admin_post reads ADMIN_API_KEY from the relay config and sends X-Admin-Key."""
        import os
        os.makedirs(tmp_path / ".hermes" / "proxy-relay", exist_ok=True)
        (tmp_path / ".hermes" / "proxy-relay" / "config.json").write_text(
            json.dumps({"ADMIN_API_KEY": "s3cret-key", "UPSTREAM_BASE": "https://x"})
        )
        monkeypatch.setenv("HOME", str(tmp_path))

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"status":"ok"}'
        sent = {}

        def fake_urlopen(req, timeout=None):
            sent["headers"] = dict(req.headers)
            return mock_resp

        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=fake_urlopen):
            mcp_mod._admin_post("/admin/clear-cooldowns")
        # urllib normalizes header casing; match case-insensitively
        assert any(k.lower() == "x-admin-key" and v == "s3cret-key" for k, v in sent["headers"].items())

    def test_admin_post_skips_header_when_no_config(self, mcp_mod, tmp_path, monkeypatch):
        """No config file → no X-Admin-Key header sent."""
        monkeypatch.setenv("HOME", str(tmp_path))
        sent = {}

        def fake_urlopen(req, timeout=None):
            sent["headers"] = dict(req.headers)
            return MagicMock()

        with patch.object(mcp_mod.urllib.request, "urlopen", side_effect=fake_urlopen):
            mcp_mod._admin_post("/admin/clear-cooldowns")
        assert not any(k.lower() == "x-admin-key" for k in sent["headers"])

    def test_admin_post_tolerates_corrupt_config(self, mcp_mod, tmp_path, monkeypatch):
        """Corrupt config.json must not raise — fall back to no header."""
        import os
        os.makedirs(tmp_path / ".hermes" / "proxy-relay", exist_ok=True)
        (tmp_path / ".hermes" / "proxy-relay" / "config.json").write_text("{ not json !!!")
        monkeypatch.setenv("HOME", str(tmp_path))

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"status":"ok"}'
        with patch.object(mcp_mod.urllib.request, "urlopen", return_value=mock_resp):
            result = mcp_mod._admin_post("/admin/clear-cooldowns")
        assert result["status"] == "ok"


class TestMcpRun:
    """The MCP server run() entrypoint — tool registration + stdio transport."""

    @pytest.fixture
    def mcp_mod(self):
        import mcp.mcp_server as mcp_mod
        return mcp_mod

    class FakeMcp:
        """Records @mcp.tool() registrations and run() calls."""

        def __init__(self):
            self.registered = {}
            self.run_kwargs = None

        def tool(self, *args, **kwargs):
            def deco(fn):
                self.registered[fn.__name__] = fn
                return fn
            return deco

        def run(self, **kwargs):
            self.run_kwargs = kwargs

    def test_run_registers_all_tools(self, mcp_mod, monkeypatch, capsys):
        fake = self.FakeMcp()
        monkeypatch.setattr(mcp_mod, "FastMCP", lambda name: fake)
        monkeypatch.setattr(mcp_mod.sys, "exit", lambda code: (_ for _ in ()).throw(SystemExit(code)))

        mcp_mod.run()

        expected = {
            "proxy_relay_status", "proxy_relay_health", "proxy_relay_upstream_health",
            "proxy_relay_config", "proxy_relay_models", "proxy_relay_request_stats",
            "proxy_relay_clear_cooldowns", "proxy_relay_reset_proxy",
            "proxy_relay_reset_by_errors", "proxy_relay_reload_proxies",
        }
        assert set(fake.registered) == expected
        assert fake.run_kwargs == {"transport": "stdio"}
        # Startup message goes to stderr (not stdout — stdout is the MCP protocol channel)
        assert "Starting Proxy Relay MCP server" in capsys.readouterr().err

    def test_run_registered_tools_call_through(self, mcp_mod, monkeypatch):
        """Each registered tool delegates to the corresponding module function."""
        fake = self.FakeMcp()
        monkeypatch.setattr(mcp_mod, "FastMCP", lambda name: fake)
        monkeypatch.setattr(mcp_mod.sys, "exit", lambda code: (_ for _ in ()).throw(SystemExit(code)))

        import asyncio
        with patch.object(mcp_mod, "tool_status", return_value="status-json"), \
             patch.object(mcp_mod, "tool_health", return_value="health-json"), \
             patch.object(mcp_mod, "tool_upstream_health", return_value="up-json"), \
             patch.object(mcp_mod, "tool_config", return_value="cfg-json"), \
             patch.object(mcp_mod, "tool_models", return_value="models-json"), \
             patch.object(mcp_mod, "tool_request_stats", return_value="stats-json"), \
             patch.object(mcp_mod, "tool_clear_cooldowns", return_value="clear-json"), \
             patch.object(mcp_mod, "tool_reset_proxy", return_value="reset-json"), \
             patch.object(mcp_mod, "tool_reset_by_errors", return_value="rbe-json"), \
             patch.object(mcp_mod, "tool_reload_proxies", return_value="reload-json"):
            mcp_mod.run()
            assert asyncio.run(fake.registered["proxy_relay_status"]()) == "status-json"
            assert asyncio.run(fake.registered["proxy_relay_health"]()) == "health-json"
            assert asyncio.run(fake.registered["proxy_relay_upstream_health"]()) == "up-json"
            assert asyncio.run(fake.registered["proxy_relay_config"]()) == "cfg-json"
            assert asyncio.run(fake.registered["proxy_relay_models"]()) == "models-json"
            assert asyncio.run(fake.registered["proxy_relay_request_stats"]()) == "stats-json"
            assert asyncio.run(fake.registered["proxy_relay_clear_cooldowns"]()) == "clear-json"
            assert asyncio.run(fake.registered["proxy_relay_reset_proxy"]("socks5://p:1")) == "reset-json"
            assert asyncio.run(fake.registered["proxy_relay_reset_by_errors"](2)) == "rbe-json"
            assert asyncio.run(fake.registered["proxy_relay_reload_proxies"]()) == "reload-json"

    def test_run_requires_mcp_sdk(self, mcp_mod, monkeypatch):
        """FastMCP is None → prints install hint and exits(1)."""
        monkeypatch.setattr(mcp_mod, "FastMCP", None)
        with pytest.raises(SystemExit) as excinfo:
            mcp_mod.run()
        assert excinfo.value.code == 1

    def test_main_guard_invokes_run(self, mcp_mod, monkeypatch, capsys):
        """The `if __name__ == "__main__": run()` guard at the file bottom executes run()."""
        import runpy
        # FastMCP is None in the test env → run() prints the hint and exits(1).
        # That SystemExit(1) proves the __main__ guard actually invoked run().
        with pytest.raises(SystemExit) as excinfo:
            runpy.run_path(str(mcp_mod.__file__), run_name="__main__")
        assert excinfo.value.code == 1
        assert "MCP SDK not installed" in capsys.readouterr().out


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
