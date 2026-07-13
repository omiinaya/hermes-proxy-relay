"""Hermes Proxy Relay plugin — slash commands and auto-config."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path


HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
PLUGIN_DIR = Path(__file__).resolve().parent
REPO_ROOT = PLUGIN_DIR.parent
RELAY_SCRIPT = REPO_ROOT / "relay" / "relay.py"
SETUP_SCRIPT = REPO_ROOT / "scripts" / "setup.sh"
RELAY_PORT = 4002


def _relay_pid() -> int | None:
    """Find the relay process PID if running."""
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
    """Quick health check on the relay."""
    try:
        import urllib.request
        resp = urllib.request.urlopen(f"http://localhost:{RELAY_PORT}/health", timeout=3)
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
    """Read a value from environment or ~/.hermes/.env."""
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


def _custom_providers_entry() -> dict:
    """Build the custom_providers entry for config.yaml."""
    return {
        "name": "proxy-relay",
        "base_url": f"http://localhost:{RELAY_PORT}/v1",
        "api_key": "relay-key",  # local relay doesn't auth; Hermes needs something here
        "model": "default",
    }


def _ensure_custom_provider() -> str | None:
    """Ensure the custom_providers entry exists in config.yaml. Returns error or None."""
    config_path = Path(HERMES_HOME) / "config.yaml"
    if not config_path.exists():
        return "No config.yaml found at {config_path}"

    import yaml
    with open(config_path) as f:
        cfg = yaml.safe_load(f) or {}

    existing = cfg.get("custom_providers", [])
    for entry in existing:
        if isinstance(entry, dict) and entry.get("name") == "proxy-relay":
            return None  # already configured

    existing.append(_custom_providers_entry())
    cfg["custom_providers"] = existing
    with open(config_path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
    return None


# ── Slash Commands ──────────────────────────────────────────────

def _cmd_setup(raw_args: str) -> str:
    """/relay setup — guided configuration."""
    lines = []
    lines.append("🔧 **Hermes Proxy Relay Setup**\n")

    # Check if relay is already running
    health = _health_check()
    if health:
        lines.append(f"✅ Relay is **running** on port {RELAY_PORT}")
        lines.append(f"   Pool: {health.get('pool_stats', {}).get('total', '?')} proxies "
                     f"({health.get('pool_stats', {}).get('available', '?')} available)")
    else:
        lines.append("⚠️  Relay is **not running**")
        pid = _relay_pid()
        if pid:
            lines.append(f"   (PID {pid} exists but health endpoint unreachable)")

    # Check if custom_providers entry exists
    config_path = Path(HERMES_HOME) / "config.yaml"
    if config_path.exists():
        import yaml
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}
        providers = cfg.get("custom_providers", [])
        has_entry = any(
            isinstance(e, dict) and e.get("name") == "proxy-relay"
            for e in providers
        )
        if has_entry:
            lines.append("✅ `custom_providers` entry `proxy-relay` is configured")
        else:
            lines.append("❌ `custom_providers` entry `proxy-relay` is **missing**")
            lines.append("   Run `hermes config edit` and add the entry, or use the MCP tools.")
    else:
        lines.append("❌ No config.yaml found")

    # Check required env vars
    upstream = _env_val("UPSTREAM_BASE") or _env_val("PROXY_RELAY_UPSTREAM_BASE")
    api_key = _env_val("UPSTREAM_API_KEY") or _env_val("PROXY_RELAY_UPSTREAM_API_KEY")
    proxy_list = _env_val("PROXY_LIST") or _env_val("PROXY_RELAY_PROXY_LIST")

    lines.append("")
    if upstream:
        lines.append(f"✅ `UPSTREAM_BASE` = `{upstream}`")
    else:
        lines.append("❌ `UPSTREAM_BASE` not set — add to ~/.hermes/.env")
        lines.append("   ```")
        lines.append("   UPSTREAM_BASE=https://api.openai.com/v1")
        lines.append("   UPSTREAM_API_KEY=sk-...")
        lines.append("   PROXY_LIST=/home/user/proxies.txt")
        lines.append("   ```")

    if api_key:
        lines.append(f"✅ `UPSTREAM_API_KEY` = `{api_key[:8]}...{api_key[-4:]}`")
    else:
        lines.append("❌ `UPSTREAM_API_KEY` not set")

    if proxy_list:
        lines.append(f"✅ `PROXY_LIST` = `{proxy_list}`")
    else:
        lines.append("❌ `PROXY_LIST` not set — need a file with SOCKS5 URLs")

    # Quick start
    lines.append("\n**Quick start guide:**")
    lines.append("```bash")
    lines.append("# 1. Set env vars in ~/.hermes/.env")
    lines.append("echo 'UPSTREAM_BASE=https://api.opencode-zen.com/v1' >> ~/.hermes/.env")
    lines.append("echo 'UPSTREAM_API_KEY=public' >> ~/.hermes/.env")
    lines.append("echo 'UPSTREAM_AUTH_TYPE=x-api-key' >> ~/.hermes/.env")
    lines.append("")
    lines.append("# 2. Create proxy list file")
    lines.append("mkdir -p ~/.hermes/proxy-relay")
    lines.append("echo 'socks5://user:pass@proxy1:1080' > ~/.hermes/proxy-relay/proxies.txt")
    lines.append("")
    lines.append("# 3. Start the relay")
    lines.append(f"cd {REPO_ROOT}/relay")
    lines.append("pip install -r ../requirements.txt")
    lines.append("python relay.py &")
    lines.append("")
    lines.append("# 4. Verify")
    lines.append("curl -s http://localhost:4002/health")
    lines.append("```")

    return "\n".join(lines)


def _cmd_status(raw_args: str) -> str:
    """/relay status — pool health and diagnostics."""
    health = _health_check()
    if not health:
        pid = _relay_pid()
        if pid:
            return f"⚠️ Relay PID {pid} exists but health endpoint unreachable on :{RELAY_PORT}."
        return f"❌ Relay is not running on :{RELAY_PORT}. Start it first."

    lines = ["📊 **Proxy Relay Status**\n"]

    # Pool stats
    pool = health.get("pool_stats", {})
    total = pool.get("total", "?")
    available = pool.get("available", "?")
    cooling_count = pool.get("cooling", 0)
    lines.append(f"**Pool:** {available}/{total} proxies available")
    if cooling_count:
        lines.append(f"⏳ {cooling_count} proxies in cooldown")

    # Upstream
    upstream = health.get("upstream_base", "?")
    lines.append(f"**Upstream:** `{upstream}`")

    # Model count
    models = health.get("models_available", 0)
    lines.append(f"**Models:** {models}")

    # Request stats
    stats = health.get("request_stats", {})
    total_reqs = stats.get("total", 0)
    ok_count = stats.get("ok", 0)
    err_count = stats.get("errors", 0)
    lines.append(f"**Requests:** {total_reqs} total ({ok_count} ok, {err_count} errors)")

    # Semaphore
    sem = health.get("semaphore", {})
    if sem:
        lines.append(f"**Concurrency:** {sem.get('used', 0)}/{sem.get('max', '?')} active")

    # Cooling details
    cooling = health.get("cooling_details", [])
    if cooling:
        lines.append(f"\n**Cooling proxies ({len(cooling)}):**")
        for c in cooling[:10]:  # show first 10
            remaining = c.get("remaining_s", 0)
            proxy = c.get("proxy", "?")
            m, s = divmod(int(remaining), 60)
            h, m = divmod(m, 60)
            if h:
                time_str = f"{h}h{m}m"
            elif m:
                time_str = f"{m}m{s}s"
            else:
                time_str = f"{s}s"
            label = proxy.split("@")[-1] if "@" in proxy else proxy
            lines.append(f"   ⏳ {label} — {time_str} remaining")

    return "\n".join(lines)


def _cmd_switch(raw_args: str) -> str:
    """/relay switch — change upstream or reload proxies."""
    args = raw_args.strip().lower()
    if not args:
        return (
            "Usage: `/relay switch upstream <url>` or `/relay switch proxies`\n"
            "  `/relay switch upstream https://new-api.com/v1`\n"
            "  `/relay switch proxies` — reload proxy list from file"
        )

    # In a real implementation, this would hit the relay's management endpoint
    return (
        "🔁 **Switch commands:**\n"
        "To change these values, edit ~/.hermes/.env and restart the relay:\n"
        "- `UPSTREAM_BASE` — upstream API endpoint\n"
        "- `UPSTREAM_API_KEY` — upstream API key\n"
        "- `PROXY_LIST` — path to proxy list file\n"
        "\nManagement endpoint support coming soon."
    )


def _handle_slash(raw_args: str) -> str:
    args = raw_args.strip().split()
    cmd = args[0].lower() if args else "status"

    if cmd in ("setup", "install", "init", "config"):
        return _cmd_setup(raw_args)
    elif cmd in ("status", "health", "info"):
        return _cmd_status(raw_args)
    elif cmd in ("switch", "change"):
        return _cmd_switch(raw_args)
    elif cmd in ("help", "?"):
        return (
            "**Proxy Relay Commands:**\n"
            "  `/relay setup` — Guided configuration\n"
            "  `/relay status` — Pool health and diagnostics\n"
            "  `/relay switch <upstream|proxies>` — Change config\n"
            "  `/relay help` — This message\n"
            "\n**Setup quick reference:**\n"
            "1. Set `UPSTREAM_BASE`, `UPSTREAM_API_KEY`, `PROXY_LIST` in ~/.hermes/.env\n"
            "2. Create your proxy list file\n"
            "3. Start the relay: `python relay/relay.py &`\n"
            "4. Check: `/relay status`"
        )
    return f"Unknown subcommand: `{cmd}`. Use `/relay help`."


# ── Plugin Registration ────────────────────────────────────────

def register(ctx) -> None:
    ctx.register_command(
        "relay",
        handler=_handle_slash,
        description="Proxy relay management (setup, status, switch).",
        args_hint="<setup|status|switch|help>",
    )

    # Auto-configure custom_providers entry if missing
    try:
        _ensure_custom_provider()
    except Exception:
        pass
