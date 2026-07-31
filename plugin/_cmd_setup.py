
# Import helpers from the parent plugin package.
# NOTE: these are defined in plugin/__init__.py AFTER the `from ._cmd_setup
# import _cmd_setup` line, so the parent package is already populated when
# this module is loaded (no circular-import issue).
import json

from plugin import (
    RELAY_CONFIG_DIR,
    RELAY_PORT,
    RELAY_SCRIPT,
    _health_check,
    _infer_auth_type,
    _read_custom_providers,
    _relay_pid,
    _write_proxied_provider,
    _write_relay_config,
)


def _mask_key(key: str) -> str:
    """Mask an API key for display — never reveal short keys.

    - empty → "(none)"
    - len <= 4 → "****"
    - len <= 8 → "ab...xy" (2 chars each side)
    - otherwise → "abcdef...wxyz" (6 + 4)
    """
    if not key:
        return "(none)"
    n = len(key)
    if n <= 4:
        return "****"
    if n <= 8:
        return f"{key[:2]}...{key[-2:]}"
    return f"{key[:6]}...{key[-4:]}"


def _cmd_setup(raw_args: str) -> str:
    """/relay setup [list|clone <N> [auth-type]] — clone an existing provider with proxy routing."""
    parts = raw_args.strip().split()
    sub = parts[1].lower() if len(parts) > 1 else ""

    # ── list ─────────────────────────────────────────────────────
    if sub == "list":
        providers = _read_custom_providers()
        if not providers:
            return (
                "📋 **Existing Custom Providers**\n\n"
                "No `custom_providers` entries found in config.yaml.\n"
                "Add one in `hermes config edit`, then run `/relay setup list`."
            )

        lines = ["📋 **Existing Custom Providers**\n"]
        for i, p in enumerate(providers, 1):
            name = p.get("name", "?")
            url = p.get("base_url", "?")
            key = p.get("api_key", "")
            key_display = _mask_key(key)
            model = p.get("model", "?")
            lines.append(f"  **{i}.** `{name}`")
            lines.append(f"      URL: {url}")
            lines.append(f"      Key: {key_display}")
            lines.append(f"      Model: {model}")
        lines.append("")
        lines.append("**Clone one with proxy:** `/relay setup clone <N>`")
        lines.append("**Override auth:** `/relay setup clone <N> x-api-key`")
        return "\n".join(lines)

    # ── clone <N> [auth] ─────────────────────────────────────────
    if sub == "clone":
        if len(parts) < 3:
            return (
                "Usage: `/relay setup clone <N>`\n"
                "  N = the number from `/relay setup list`\n"
                "  `/relay setup clone 2` — clones provider #2 with proxy"
            )

        try:
            idx = int(parts[2]) - 1  # 1-indexed from user
        except ValueError:
            return f"Invalid number: `{parts[2]}`. Use the number from `/relay setup list`."

        providers = _read_custom_providers()
        if idx < 0 or idx >= len(providers):
            return f"Invalid index `{parts[2]}`. Run `/relay setup list` to see available providers."

        original = providers[idx]
        orig_name = original["name"]
        orig_url = original.get("base_url", "")
        orig_key = original.get("api_key", "")

        # Infer or override auth type
        if len(parts) >= 4:
            auth_type = parts[3].lower()
        else:
            auth_type = _infer_auth_type(original)

        # Write relay config
        try:
            relay_config_path = _write_relay_config(orig_url, orig_key, auth_type)
        except Exception as e:
            return f"❌ Failed to write relay config: {e}"

        # Write proxied provider entry (NEVER touches original)
        try:
            new_entry = _write_proxied_provider(orig_name)
            new_name = new_entry["name"]
        except Exception as e:
            return f"❌ Failed to write Hermes provider entry: {e}"

        lines = [f"✅ **Cloned: `{orig_name}` → `{new_name}`**\n"]
        lines.append("**Original** (untouched):")
        lines.append(f"  URL: {orig_url}")
        lines.append(f"  Key: {_mask_key(orig_key)}")
        lines.append("")
        lines.append("**Proxied entry created:**")
        lines.append(f"  Name: `{new_name}`")
        lines.append(f"  Routes through: `http://localhost:{RELAY_PORT}/v1`")
        lines.append(f"  Auth type: `{auth_type}`")
        lines.append("")
        lines.append(f"**Relay config saved to:** `{relay_config_path}`")
        lines.append("")
        lines.append("**Next steps:**")
        lines.append("  1. Create your SOCKS5 proxy list file:")
        lines.append("     ```")
        lines.append("     mkdir -p ~/.hermes/proxy-relay")
        lines.append("     echo 'socks5://user:pass@proxy:1080' > ~/.hermes/proxy-relay/proxies.txt")
        lines.append("     ```")
        lines.append("  2. Start the relay:")
        lines.append("     ```")
        lines.append("     PROXY_LIST=~/.hermes/proxy-relay/proxies.txt \\")
        lines.append(f"       python {RELAY_SCRIPT}")
        lines.append("     ```")
        lines.append("  3. In Hermes, switch to the proxied provider:")
        lines.append(f"     `/model {new_name}`")
        lines.append("     Or set it as default:")
        lines.append(f"     `hermes config set model.default {new_name}`")
        lines.append(f"     `hermes config set model.provider custom:{new_name}`")
        lines.append("  4. Verify: `/relay status`")
        return "\n".join(lines)

    # ── no subcommand (or unknown) ───────────────────────────────
    # Show overview (existing behaviour)
    lines = ["🔧 **Hermes Proxy Relay**\n"]

    health = _health_check()
    if health:
        pool = health.get("pool_stats", {})
        lines.append(f"✅ Relay **running** on :{RELAY_PORT} "
                     f"({pool.get('available', '?')}/{pool.get('total', '?')} proxies available)")
    else:
        lines.append("⚠️  Relay **not running**")
        pid = _relay_pid()
        if pid:
            lines.append(f"   (PID {pid} exists but health unreachable)")

    # Count existing providers available to clone
    providers = _read_custom_providers()
    if providers:
        lines.append(f"📋 **{len(providers)} existing providers** available to clone:")
        for i, p in enumerate(providers[:5], 1):
            lines.append(f"   {i}. `{p.get('name', '?')}` — {p.get('base_url', '?')}")
        if len(providers) > 5:
            lines.append(f"   ... and {len(providers) - 5} more")
        lines.append("")
        lines.append("  👉 `/relay setup list` — see all providers with details")
        lines.append("  👉 `/relay setup clone <N>` — clone one with proxy routing")
    else:
        lines.append("📋 No `custom_providers` entries found in config.yaml.")
        lines.append("   Add one via `hermes config edit`, then run `/relay setup list`.")

    # Relay config status
    relay_cfg = RELAY_CONFIG_DIR / "config.json"
    if relay_cfg.exists():
        try:
            rc = json.loads(relay_cfg.read_text())
            upstream = rc.get("UPSTREAM_BASE", "?")
            lines.append(f"📄 Relay configured to forward to: `{upstream}`")
        except Exception:
            pass

    return "\n".join(lines)

