"""Hermes Proxy Relay plugin — slash commands and auto-config.

from ._cmd_setup import _cmd_setup

Usage: /relay setup list               — show existing providers
       /relay setup clone <N> [auth]   — clone provider N with proxy routing
       /relay status                   — pool health
       /relay help                     — this message

Never replaces existing custom_providers entries. Always creates
a new entry with a `-proxied` suffix.
"""

import json
import os
import subprocess
from pathlib import Path


HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
PLUGIN_DIR = Path(__file__).resolve().parent
REPO_ROOT = PLUGIN_DIR.parent
RELAY_SCRIPT = REPO_ROOT / "relay" / "relay.py"
SETUP_SCRIPT = REPO_ROOT / "scripts" / "setup.sh"
RELAY_PORT = int(os.environ.get("RELAY_PORT", "4002"))
RELAY_CONFIG_DIR = Path(HERMES_HOME) / "proxy-relay"


# ── Helpers ────────────────────────────────────────────────────────

def _relay_pid() -> int | None:
    try:
        result = subprocess.run(
            ["pgrep", "-f", "relay/relay.py"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip().split("\n")[0])
    except Exception:
        pass
    return None


def _health_check() -> dict | None:
    try:
        import urllib.request
        resp = urllib.request.urlopen(f"http://localhost:{RELAY_PORT}/health", timeout=3)
        return json.loads(resp.read().decode())
    except Exception:
        return None


def _admin_headers() -> dict:
    """Headers for admin requests — include X-Admin-Key if configured.

    Reads ADMIN_API_KEY from the relay config file so plugin admin
    commands work even when the relay enforces admin auth.
    """
    headers = {"Content-Type": "application/json"}
    try:
        config_path = RELAY_CONFIG_DIR / "config.json"
        if config_path.exists():
            cfg = json.loads(config_path.read_text())
            key = cfg.get("ADMIN_API_KEY", "")
            if key:
                headers["X-Admin-Key"] = key
    except Exception:
        pass
    return headers


def _admin_post(path: str, body: dict | None = None) -> dict | None:
    """POST to a relay admin endpoint and return parsed JSON."""
    try:
        import urllib.request
        data = json.dumps(body).encode() if body else b"{}"
        req = urllib.request.Request(
            f"http://localhost:{RELAY_PORT}{path}",
            data=data,
            headers=_admin_headers(),
            method="POST",
        )
        resp = urllib.request.urlopen(req, timeout=5)
        return json.loads(resp.read().decode())
    except Exception:
        return None


def _get_env_path() -> str:
    dot_env = os.path.join(HERMES_HOME, ".env")
    env_path = os.environ.get("HERMES_ENV_PATH", dot_env)
    if not env_path or env_path == "''":
        env_path = dot_env
    return env_path


def _env_val(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if val:
        return val
    try:
        with open(_get_env_path()) as f:
            for line in f:
                if line.startswith(f"{key}="):
                    return line.split("=", 1)[1].strip().strip("\"'")
    except Exception:
        pass
    return ""


def _load_config() -> dict:
    """Read config.yaml and return parsed dict."""
    config_path = Path(HERMES_HOME) / "config.yaml"
    if not config_path.exists():
        return {}
    import yaml
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def _save_config(cfg: dict):
    """Write config.yaml from dict."""
    config_path = Path(HERMES_HOME) / "config.yaml"
    import yaml
    with open(config_path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)


def _read_custom_providers() -> list[dict]:
    """Return all custom_providers entries from config.yaml eligible for cloning.

    Excludes:
    - Entries already routing through the relay (base_url contains relay port)
    - The relay's own entry (named 'proxy-relay')
    - Already-proxied clones (name ends with '-proxied')
    """
    cfg = _load_config()
    providers = cfg.get("custom_providers", [])
    result = []
    for p in providers:
        if not isinstance(p, dict) or not p.get("name"):
            continue
        name = p.get("name", "")
        url = p.get("base_url", "")
        # Skip relay's own entries
        if name == "proxy-relay" or name.endswith("-proxied"):
            continue
        # Skip entries already pointing at the relay (would create a loop)
        if f":{RELAY_PORT}" in url:
            continue
        result.append(p)
    return result


def _infer_auth_type(provider: dict) -> str:
    """Try to guess the auth type from a custom_providers entry.

    Hermes custom_providers always send Authorization: Bearer *** default.
    Some upstreams expect x-api-key instead of bearer. We infer from:
    - Provider name hints that match known x-api-key patterns
    - The api_key value itself ("public" suggests x-api-key)
    """
    name = (provider.get("name") or "").lower()
    key = (provider.get("api_key") or "").strip()

    if "opencode" in name or "oc-zen" in name or "zen" in name:
        return "x-api-key"
    if key == "public":
        return "x-api-key"
    return "bearer"


def _write_relay_config(base_url: str, api_key: str, auth_type: str, proxy_list_path: str = ""):
    """Write relay config file at ~/.hermes/proxy-relay/config.json."""
    RELAY_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    config = {
        "UPSTREAM_BASE": base_url.rstrip("/"),
        "UPSTREAM_API_KEY": api_key,
        "UPSTREAM_AUTH_TYPE": auth_type,
    }
    if proxy_list_path:
        config["PROXY_LIST"] = proxy_list_path
    config_path = RELAY_CONFIG_DIR / "config.json"
    config_path.write_text(json.dumps(config, indent=2))
    # Guard permissions (secrets!)
    config_path.chmod(0o600)
    return str(config_path)


def _write_proxied_provider(original_name: str) -> dict:
    """Create a new custom_providers entry routing through the relay.

    Returns the entry dict. Writes it to config.yaml. Never touches the original.
    """
    new_name = f"{original_name}-proxied"
    entry = {
        "name": new_name,
        "base_url": f"http://localhost:{RELAY_PORT}/v1",
        "api_key": "relay-key",
        "model": "auto",
    }

    cfg = _load_config()
    providers = cfg.setdefault("custom_providers", [])

    # Check if already exists
    for p in providers:
        if isinstance(p, dict) and p.get("name") == new_name:
            return p  # already there

    providers.append(entry)
    _save_config(cfg)
    return entry


# ── Slash Commands ──────────────────────────────────────────────
def _cmd_status(raw_args: str) -> str:
    """/relay status — pool health and diagnostics."""
    health = _health_check()
    if not health:
        pid = _relay_pid()
        if pid:
            return f"⚠️ Relay PID {pid} exists but health endpoint unreachable on :{RELAY_PORT}."
        return f"❌ Relay is not running on :{RELAY_PORT}. Start it first."

    lines = ["📊 **Proxy Relay Status**\n"]
    pool = health.get("pool_stats", {})
    total = pool.get("total", "?")
    available = pool.get("available", "?")
    cooling_count = pool.get("cooling", 0)
    perm_failed = pool.get("permanently_failed", 0)
    lines.append(f"**Pool:** {available}/{total} proxies available")
    if perm_failed:
        lines.append(f"🪦 **{perm_failed} proxies permanently failed** (bandwidth exhausted / dead)")
    if cooling_count:
        lines.append(f"⏳ {cooling_count} proxies in temporary cooldown")

    upstream = health.get("upstream_base", "?")
    lines.append(f"**Upstream:** `{upstream}`")
    models = health.get("models_available", 0)
    lines.append(f"**Models:** {models}")

    stats = health.get("request_stats", {})
    total_reqs = stats.get("total", 0)
    ok_count = stats.get("ok", 0)
    err_count = stats.get("errors", 0)
    lines.append(f"**Requests:** {total_reqs} total ({ok_count} ok, {err_count} errors)")

    sem = health.get("semaphore", {})
    if sem:
        lines.append(f"**Concurrency:** {sem.get('used', 0)}/{sem.get('max', '?')} active")

    version = health.get("version", "")
    uptime = health.get("uptime_seconds", 0)
    if version:
        m, s = divmod(int(uptime), 60)
        h, m = divmod(m, 60)
        uptime_str = f"{h}h{m}m{s}s" if h else f"{m}m{s}s" if m else f"{s}s"
        lines.append(f"**Version:** v{version} (up {uptime_str})")

    cooling = pool.get("cooling_details", [])
    if cooling:
        lines.append(f"\n**Temporary cooling ({len(cooling)}):**")
        for c in cooling[:10]:
            remaining = c.get("remaining_s", 0)
            proxy = c.get("proxy", "?")
            m, s = divmod(int(remaining), 60)
            h, m = divmod(m, 60)
            time_str = f"{h}h{m}m{s}s" if h else f"{m}m{s}s" if m else f"{s}s"
            label = proxy.split("@")[-1] if "@" in proxy else proxy
            lines.append(f"   ⏳ {label} — {time_str} remaining")

    permanently_failed = pool.get("permanently_failed_details", [])
    if permanently_failed:
        lines.append(f"\n**Permanently failed ({len(permanently_failed)}):**")
        for c in permanently_failed[:10]:
            proxy = c.get("proxy", "?")
            err = c.get("last_error", "unknown")
            errs = c.get("total_429", 0)
            label = proxy.split("@")[-1] if "@" in proxy else proxy
            lines.append(f"   🪦 {label} — {err} (429s: {errs})")

    return "\n".join(lines)


def _cmd_reset(raw_args: str) -> str:
    """/relay reset <proxy-url|errors|all> — manage proxy cooldowns."""
    parts = raw_args.strip().split()
    sub = parts[1].lower() if len(parts) > 1 else ""

    if sub == "all":
        result = _admin_post("/admin/clear-cooldowns")
        if result and result.get("status") == "ok":
            return f"✅ **All proxy cooldowns cleared.** {result.get('proxies_total', '?')} proxies now available."
        return "❌ Failed to clear cooldowns. Is the relay running?"

    if sub == "errors":
        # Reset all permanently-failed proxies
        threshold = parts[2] if len(parts) > 2 else "3"
        try:
            min_errs = int(threshold)
        except ValueError:
            return f"Invalid threshold: {threshold}. Use a number."
        result = _admin_post("/admin/reset-by-errors", {"min_consecutive": min_errs})
        if result and result.get("status") == "ok":
            count = result.get("message", "0")
            return f"✅ **Reset permanently-failed proxies.** {count} re-enabled."
        return "❌ Failed to reset. Is the relay running?"

    if sub == "proxies":
        result = _admin_post("/admin/reload-proxies")
        if result and result.get("status") == "ok":
            return f"✅ **Proxy list reloaded.** {result.get('proxies_total', '?')} proxies in pool."
        return "❌ Failed to reload proxies. Is the relay running?"

    # Reset a specific proxy by URL
    proxy_url = sub
    if not proxy_url:
        return (
            "Usage: `/relay reset <proxy-url>`\n"
            "       `/relay reset all` — clear all cooldowns\n"
            "       `/relay reset errors [threshold]` — reset permanently-failed proxies\n"
            "       `/relay reset proxies` — reload proxy list from file\n"
        )
    result = _admin_post("/admin/reset-proxy", {"url": proxy_url})
    if result and result.get("status") == "ok":
        return f"✅ **Proxy reset:** `{proxy_url}`"
    error = result.get("error", "Unknown error") if result else "Relay unreachable"
    return f"❌ {error}"


def _cmd_switch(raw_args: str) -> str:
    """Switch upstream or reload proxies at runtime."""
    parts = raw_args.strip().split()
    if len(parts) < 2:
        return (
            "Usage: `/relay switch upstream <url>` or `/relay switch proxies`\n"
            "  `/relay switch upstream https://new-api.com/v1` — change upstream\n"
            "  `/relay switch proxies` — reload proxy list from file\n"
        )

    sub = parts[1].lower()
    if sub == "upstream" and len(parts) >= 3:
        new_url = parts[2].rstrip("/")
        # Update config.json
        config_path = RELAY_CONFIG_DIR / "config.json"
        if config_path.exists():
            try:
                import json
                cfg = json.loads(config_path.read_text())
                cfg["UPSTREAM_BASE"] = new_url
                config_path.write_text(json.dumps(cfg, indent=2))
                config_path.chmod(0o600)
                # Hot-reload if the relay is running
                result = _admin_post("/admin/reload-config")
                if result and result.get("status") == "ok":
                    return f"✅ **Upstream URL updated + hot-reloaded.**\n   New: `{new_url}`\n   (no restart needed)"
                return f"✅ **Upstream URL updated** in `{config_path}`\n   New: `{new_url}`\n\n⚠️  Relay not running — start it (or `/relay restart`) to apply."
            except Exception as e:
                return f"❌ Failed to update config: {e}"
        return "❌ No relay config found. Clone a provider first with `/relay setup clone <N>`."

    if sub == "auth" and len(parts) >= 3:
        new_auth = parts[2].lower()
        if new_auth not in ("bearer", "x-api-key"):
            return "❌ Invalid auth type. Use `bearer` or `x-api-key`."
        config_path = RELAY_CONFIG_DIR / "config.json"
        if config_path.exists():
            try:
                import json
                cfg = json.loads(config_path.read_text())
                cfg["UPSTREAM_AUTH_TYPE"] = new_auth
                config_path.write_text(json.dumps(cfg, indent=2))
                config_path.chmod(0o600)
                # Hot-reload if the relay is running
                result = _admin_post("/admin/reload-config")
                if result and result.get("status") == "ok":
                    return f"✅ **Auth type updated + hot-reloaded.**\n   New: `{new_auth}`\n   (no restart needed)"
                return f"✅ **Auth type updated** in `{config_path}`\n   New: `{new_auth}`\n\n⚠️  Relay not running — start it (or `/relay restart`) to apply."
            except Exception as e:
                return f"❌ Failed to update config: {e}"
        return "❌ No relay config found. Clone a provider first with `/relay setup clone <N>`."

    if sub == "proxies":
        result = _admin_post("/admin/reload-proxies")
        if result and result.get("status") == "ok":
            return f"✅ **Proxy list reloaded.** {result.get('proxies_total', '?')} proxies in pool."
        return "❌ Failed to reload proxies. Is the relay running?"

    return (
        "Unknown subcommand: `{sub}`. Available:\n"
        "  `/relay switch upstream <url>` — change upstream API URL\n"
        "  `/relay switch auth <bearer|x-api-key>` — change auth header type\n"
        "  `/relay switch proxies` — reload proxy list from file"
    )


def _cmd_logs(raw_args: str) -> str:
    """Show recent relay log entries."""
    try:
        import subprocess
        result = subprocess.run(
            ["journalctl", "--user", "-u", "hermes-proxy-relay",
             "--no-pager", "-n", "20", "--output", "short-iso"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split("\n")
            # Filter to just relay-relevant lines
            relevant = [line for line in lines if "proxy-relay" in line.lower() or "relay" in line.lower() or "429" in line or "error" in line.lower() or "started" in line.lower() or "shutting" in line.lower()]
            if not relevant:
                relevant = lines[:10]
            return "📋 **Recent Relay Logs**\n```\n" + "\n".join(relevant[-15:]) + "\n```"

        # Fallback: try pgrep + journalctl by PID
        pid = _relay_pid()
        if pid:
            result2 = subprocess.run(
                ["journalctl", "--user", f"_PID={pid}", "--no-pager", "-n", "15", "--output", "short-iso"],
                capture_output=True, text=True, timeout=10,
            )
            if result2.stdout.strip():
                return "📋 **Recent Relay Logs**\n```\n" + result2.stdout.strip() + "\n```"

        return "📋 No relay logs found. Is the systemd service running?"
    except Exception as e:
        return f"❌ Failed to read logs: {e}"


def _cmd_restart(raw_args: str) -> str:
    """Restart the relay via systemd."""
    try:
        import subprocess
        # Check if systemd service exists and is active
        check = subprocess.run(
            ["systemctl", "--user", "is-active", "hermes-proxy-relay.service"],
            capture_output=True, text=True, timeout=5,
        )
        if check.returncode != 0 and "inactive" not in check.stdout and "failed" not in check.stdout:
            # Try finding a running python process for the relay
            pid = _relay_pid()
            if pid:
                subprocess.run(["kill", str(pid)], timeout=5)
                return f"✅ **Relay process (PID {pid}) killed.** Auto-restart by systemd if service was running."
            return "⚠️  Relay is not managed by systemd. Restart manually:\n`python relay/relay.py`"

        result = subprocess.run(
            ["systemctl", "--user", "restart", "hermes-proxy-relay.service"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            # Wait briefly for startup
            import time as _time
            _time.sleep(2)
            if _health_check():
                return "✅ **Relay restarted successfully.** `/relay status` to verify."
            return "✅ **Restart command sent.** Check in a moment: `/relay status`"
        return f"❌ Failed to restart: {result.stderr.strip() or 'unknown error'}"
    except subprocess.TimeoutExpired:
        return "❌ Restart timed out. Try: `systemctl --user restart hermes-proxy-relay`"
    except Exception as e:
        return f"❌ Failed to restart: {e}"


def _handle_slash(raw_args: str) -> str:
    args = raw_args.strip().split()
    cmd = args[0].lower() if args else "status"

    if cmd in ("setup", "install", "init", "config"):
        return _cmd_setup(raw_args)
    elif cmd in ("status", "health", "info"):
        return _cmd_status(raw_args)
    elif cmd in ("switch", "change"):
        return _cmd_switch(raw_args)
    elif cmd in ("reset", "clear", "reload"):
        return _cmd_reset(raw_args)
    elif cmd in ("logs", "log", "journal"):
        return _cmd_logs(raw_args)
    elif cmd in ("restart", "reboot"):
        return _cmd_restart(raw_args)
    elif cmd in ("help", "?"):
        return (
            "**Proxy Relay Commands:**\n"
            "  `/relay setup` — Overview and quick start\n"
            "  `/relay setup list` — List existing providers to clone\n"
            "  `/relay setup clone <N>` — Clone a provider with proxy routing\n"
            "  `/relay status` — Pool health and diagnostics\n"
            "  `/relay reset <proxy-url>` — Reset a specific proxy's cooldown\n"
            "  `/relay reset all` — Clear all cooldowns (re-enable every proxy)\n"
            "  `/relay reset errors [threshold]` — Reset permanently-failed proxies\n"
            "  `/relay reset proxies` — Reload proxy list from file\n"
            "  `/relay switch upstream <url>` — Change upstream API URL\n"
            "  `/relay switch auth <bearer|x-api-key>` — Change auth header type\n"
            "  `/relay switch proxies` — Reload proxy list from file\n"
            "  `/relay restart` — Restart the relay service\n"
            "  `/relay logs` — Show recent relay log entries\n"
            "  `/relay help` — This message\n"
            "\n**Quick start:**\n"
            "1. `/relay setup list` — see what providers you have\n"
            "2. `/relay setup clone 1` — clone the first one with proxy\n"
            "3. Create a proxy list file\n"
            "4. Start the relay: `python relay/relay.py`\n"
            "5. `/model <name>-proxied` — switch to the proxied provider"
        )
    return f"Unknown subcommand: `{cmd}`. Use `/relay help`."


# ── Plugin Registration ────────────────────────────────────────

# Import AFTER all helpers are defined — _cmd_setup.py imports names from
# this module, so importing it earlier would cause a circular import.
from ._cmd_setup import _cmd_setup  # noqa: E402


def register(ctx) -> None:
    ctx.register_command(
        "relay",
        handler=_handle_slash,
        description="Proxy relay: clone any custom provider with SOCKS5 proxy rotation.",
        args_hint="<setup|status|switch|help>",
    )
