"""MCP server — exposes relay management as agent-callable tools.

Run as a companion to the Hermes Plugin for tool-level pool inspection
and management.

Usage:
    python -m mcp.mcp_server         # run via module
    python mcp/mcp_server.py          # run directly
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

# MCP SDK
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    FastMCP = None

RELAY_PORT = int(os.environ.get("RELAY_PORT", "4002"))
RELAY_BASE = f"http://localhost:{RELAY_PORT}"


# ── Helpers ──────────────────────────────────────────────────────────

def _health_data() -> dict | None:
    """Fetch /health from the relay."""
    try:
        resp = urllib.request.urlopen(f"{RELAY_BASE}/health", timeout=5)
        return json.loads(resp.read().decode())
    except Exception:
        return None


def _models_data() -> dict | None:
    """Fetch /v1/models from the relay."""
    try:
        resp = urllib.request.urlopen(f"{RELAY_BASE}/v1/models", timeout=10)
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


def _format_tool_result(data: dict, label: str) -> str:
    """Pretty-print a tool result."""
    return json.dumps(data, indent=2)


# ── Tool implementations ────────────────────────────────────────────

def tool_status() -> str:
    """Get relay pool status, model counts, and cooling details."""
    health = _health_data()
    if not health:
        return json.dumps({
            "status": "unreachable",
            "relay_url": RELAY_BASE,
            "error": "Connection refused — is the relay running?",
        })

    result = {
        "status": health.get("status", "unknown"),
        "pool": health.get("pool_stats", {}),
        "models": health.get("models_available", 0),
        "requests": health.get("request_stats", {}),
        "semaphore": health.get("semaphore", {}),
        "upstream": health.get("upstream_base", ""),
        "uptime_seconds": health.get("uptime_seconds", 0),
        "version": health.get("version", "unknown"),
    }
    return _format_tool_result(result, "relay status")


def tool_models() -> str:
    """List models available from the upstream API (via relay cache)."""
    models = _models_data()
    if not models:
        return json.dumps({
            "status": "error",
            "message": "Failed to fetch models from relay",
        })

    data = models.get("data", [])
    model_names = [m.get("id", "") for m in data if m.get("id")]
    return json.dumps({
        "status": "ok",
        "total": len(model_names),
        "models": sorted(model_names),
    }, indent=2)


def tool_config() -> str:
    """Show the relay's current upstream configuration."""
    health = _health_data()
    if not health:
        return _format_tool_result({
            "status": "unreachable",
            "relay_url": RELAY_BASE,
        }, "relay config")

    return _format_tool_result({
        "status": health.get("status"),
        "upstream": health.get("upstream_base", ""),
        "port": RELAY_PORT,
        "version": health.get("version", "unknown"),
        "uptime_seconds": health.get("uptime_seconds", 0),
        "pool_total": health.get("pool_stats", {}).get("total", 0),
        "pool_available": health.get("pool_stats", {}).get("available", 0),
        "pool_cooling": health.get("pool_stats", {}).get("cooling", 0),
        "pool_permanently_failed": health.get("pool_stats", {}).get("permanently_failed", 0),
    }, "relay config")


def tool_request_stats() -> str:
    """Show request counters (total, ok, errors)."""
    health = _health_data()
    if not health:
        return _format_tool_result({"status": "unreachable"}, "request stats")

    stats = health.get("request_stats", {})
    return _format_tool_result({
        "status": "ok",
        "total": stats.get("total", 0),
        "ok": stats.get("ok", 0),
        "errors": stats.get("errors", 0),
        "error_rate_pct": round(
            (stats.get("errors", 0) / max(stats.get("total", 1), 1)) * 100, 1
        ),
        "concurrency": health.get("semaphore", {}),
    }, "request stats")


def tool_clear_cooldowns() -> str:
    """Clear all proxy cooldowns (force all proxies available)."""
    result = _admin_post("/admin/clear-cooldowns")
    return _format_tool_result(result, "clear cooldowns")


def tool_reset_proxy(proxy_url: str) -> str:
    """Reset a single proxy by URL (clear its cooldown and error state)."""
    result = _admin_post("/admin/reset-proxy", {"url": proxy_url})
    return _format_tool_result(result, "reset proxy")


def tool_reset_by_errors(min_consecutive: int = 3) -> str:
    """Reset all permanently-failed proxies with >= min_consecutive errors."""
    result = _admin_post("/admin/reset-by-errors", {"min_consecutive": min_consecutive})
    return _format_tool_result(result, "reset by errors")


def tool_reload_proxies() -> str:
    """Reload the proxy list from the configured file/env."""
    result = _admin_post("/admin/reload-proxies")
    return _format_tool_result(result, "reload proxies")


def tool_health() -> str:
    """Quick health check — returns status, latency, and proxy availability."""
    start = time.time()
    health = _health_data()
    latency_ms = int((time.time() - start) * 1000)
    if not health:
        return json.dumps({
            "healthy": False,
            "latency_ms": latency_ms,
            "error": "Connection refused",
            "relay_url": RELAY_BASE,
        })
    return json.dumps({
        "healthy": health.get("status") == "ok",
        "status": health.get("status"),
        "latency_ms": latency_ms,
        "uptime_seconds": health.get("uptime_seconds", 0),
        "version": health.get("version", "unknown"),
        "available_proxies": health.get("pool_stats", {}).get("available", 0),
        "total_proxies": health.get("pool_stats", {}).get("total", 0),
        "permanently_failed": health.get("pool_stats", {}).get("permanently_failed", 0),
        "total_requests": health.get("request_stats", {}).get("total", 0),
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
    async def proxy_relay_config() -> str:
        """Show the relay's current upstream config and pool summary."""
        return tool_config()

    @mcp.tool()
    async def proxy_relay_models() -> str:
        """List models available from the upstream API (via relay cache)."""
        return tool_models()

    @mcp.tool()
    async def proxy_relay_request_stats() -> str:
        """Show request counters (total, ok, errors) and error rate."""
        return tool_request_stats()

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
