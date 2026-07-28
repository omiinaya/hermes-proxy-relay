"""MCP server — exposes relay management as agent-callable tools.

Run as a companion to the Hermes Plugin for tool-level pool inspection
and management.
"""

import json
import os
import sys
import urllib.request
import urllib.error
import time

# MCP SDK
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    FastMCP = None

RELAY_PORT = int(os.environ.get("RELAY_PORT", "4002"))
RELAY_BASE = f"http://localhost:{RELAY_PORT}"
RELAY_HEALTH_URL = f"{RELAY_BASE}/health"


# ── Helpers ──────────────────────────────────────────────────────────

def _health_data() -> dict | None:
    """Fetch /health from the relay."""
    try:
        resp = urllib.request.urlopen(RELAY_HEALTH_URL, timeout=5)
        return json.loads(resp.read().decode())
    except Exception:
        return None


def _admin_post(path: str, body: dict | None = None) -> dict:
    """POST to an admin endpoint and return parsed JSON."""
    data = json.dumps(body).encode() if body else b"{}"
    req = urllib.request.Request(
        f"{RELAY_BASE}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode())
        except Exception:
            return {"status": "error", "message": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── Tools ──────────────────────────────────────────────────────────

def tool_status() -> str:
    """Get relay pool status, model counts, and cooling details."""
    health = _health_data()
    if not health:
        return json.dumps({
            "status": "unreachable",
            "relay_url": RELAY_BASE,
        })

    result = {
        "status": health.get("status", "unknown"),
        "pool": health.get("pool_stats", {}),
        "models": health.get("models_available", 0),
        "requests": health.get("request_stats", {}),
        "semaphore": health.get("semaphore", {}),
        "upstream": health.get("upstream_base", ""),
    }
    return json.dumps(result, indent=2)


def tool_clear_cooldowns() -> str:
    """Clear all proxy cooldowns (force all proxies available)."""
    result = _admin_post("/admin/clear-cooldowns")
    return json.dumps(result, indent=2)


def tool_reset_proxy(proxy_url: str) -> str:
    """Reset a single proxy by URL (clear its cooldown and error state)."""
    result = _admin_post("/admin/reset-proxy", {"url": proxy_url})
    return json.dumps(result, indent=2)


def tool_reset_by_errors(min_consecutive: int = 3) -> str:
    """Reset all proxies that have been permanently failed with >= min_consecutive errors."""
    result = _admin_post("/admin/reset-by-errors", {"min_consecutive": min_consecutive})
    return json.dumps(result, indent=2)


def tool_reload_proxies() -> str:
    """Reload the proxy list from the configured file/env."""
    result = _admin_post("/admin/reload-proxies")
    return json.dumps(result, indent=2)


def tool_health() -> str:
    """Basic health check — returns status and latency."""
    start = time.time()
    health = _health_data()
    latency_ms = int((time.time() - start) * 1000)
    if not health:
        return json.dumps({
            "healthy": False,
            "latency_ms": latency_ms,
            "error": "Connection refused",
        })
    return json.dumps({
        "healthy": health.get("status") == "ok",
        "status": health.get("status"),
        "latency_ms": latency_ms,
        "available_proxies": health.get("pool_stats", {}).get("available", 0),
        "total_proxies": health.get("pool_stats", {}).get("total", 0),
        "permanently_failed": health.get("pool_stats", {}).get("permanently_failed", 0),
    })


# ── MCP Server ─────────────────────────────────────────────────────

def run():
    """Start the MCP server."""
    if FastMCP is None:
        print("MCP SDK not installed. Install with: pip install mcp")
        sys.exit(1)

    mcp = FastMCP("proxy-relay-mcp")

    @mcp.tool()
    async def proxy_relay_status() -> str:
        """Get relay pool status, model counts, cooling and permanently-failed details."""
        return tool_status()

    @mcp.tool()
    async def proxy_relay_health() -> str:
        """Quick health check — returns status, latency, and proxy availability."""
        return tool_health()

    @mcp.tool()
    async def proxy_relay_clear_cooldowns() -> str:
        """Clear ALL proxy cooldowns — resets both temporary and permanently-failed proxies."""
        return tool_clear_cooldowns()

    @mcp.tool()
    async def proxy_relay_reset_proxy(proxy_url: str) -> str:
        """Reset a single proxy by URL — clears its cooldown and error state.
        Use this to re-enable a specific proxy that was permanently marked.
        """
        return tool_reset_proxy(proxy_url)

    @mcp.tool()
    async def proxy_relay_reset_by_errors(min_consecutive: int = 3) -> str:
        """Reset all proxies that were permanently failed with at least min_consecutive errors.
        Default 3 (the default threshold). Pass a lower number to be more aggressive.
        """
        return tool_reset_by_errors(min_consecutive)

    @mcp.tool()
    async def proxy_relay_reload_proxies() -> str:
        """Reload the proxy list from the configured file or env var. Keeps existing cooldowns."""
        return tool_reload_proxies()

    print("Starting Proxy Relay MCP server...", file=sys.stderr)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    run()
