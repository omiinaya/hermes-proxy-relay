"""MCP server — exposes relay management as agent-callable tools.

Run as a companion to the Hermes Plugin for tool-level pool inspection
and management.
"""

import json
import os
import sys

# MCP SDK
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    FastMCP = None

RELAY_PORT = int(os.environ.get("RELAY_PORT", "4002"))
RELAY_HEALTH_URL = f"http://localhost:{RELAY_PORT}/health"

# ── Tools ──────────────────────────────────────────────────────────

def _health_data() -> dict | None:
    """Fetch /health from the relay."""
    try:
        import urllib.request
        resp = urllib.request.urlopen(RELAY_HEALTH_URL, timeout=5)
        return json.loads(resp.read().decode())
    except Exception:
        return None


def tool_status() -> str:
    """Get relay pool status, model counts, and cooling details."""
    health = _health_data()
    if not health:
        return json.dumps({
            "status": "unreachable",
            "relay_url": f"http://localhost:{RELAY_PORT}",
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
    # This requires exposing an admin endpoint on the relay or sending a signal
    return json.dumps({
        "success": False,
        "note": "Not yet implemented — restart the relay or implement /admin/clear-cooling",
    })


def tool_health() -> str:
    """Basic health check — returns status and latency."""
    start = __import__("time").time()
    health = _health_data()
    latency_ms = int((__import__("time").time() - start) * 1000)
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
        """Get relay pool status, model counts, and cooling details."""
        return tool_status()

    @mcp.tool()
    async def proxy_relay_health() -> str:
        """Quick health check — returns status and latency."""
        return tool_health()

    @mcp.tool()
    async def proxy_relay_clear_cooldowns() -> str:
        """Clear all proxy cooldowns (force all proxies available)."""
        return tool_clear_cooldowns()

    print("Starting Proxy Relay MCP server...", file=sys.stderr)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    run()
