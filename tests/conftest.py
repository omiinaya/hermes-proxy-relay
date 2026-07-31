"""Pytest fixtures for Hermes Proxy Relay tests."""

import pytest
import sys
from pathlib import Path

# Ensure project root is on sys.path for relay imports
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


# ═════════════════════════════════════════════════════════════════
#   CooldownPool Fixtures
# ═════════════════════════════════════════════════════════════════

SAMPLE_PROXIES = [
    "socks5://user1:pass1@192.168.1.10:1080",
    "socks5://user2:pass2@192.168.1.11:1080",
    "socks5://user3:pass3@192.168.1.12:1080",
    "http://user4:pass4@proxy.example.com:3128",
]


def _import_pool():
    """Lazy import relay — must happen after env is patched."""
    from relay.relay import CooldownPool
    return CooldownPool


@pytest.fixture
def cooldown_pool():
    """Fresh CooldownPool with 4 sample proxies."""
    CP = _import_pool()
    return CP(SAMPLE_PROXIES)


@pytest.fixture
def empty_pool():
    """Fresh CooldownPool with no proxies."""
    CP = _import_pool()
    return CP()


@pytest.fixture
def mock_proxy_pool():
    """CooldownPool with 3 proxies for rotation tests."""
    CP = _import_pool()
    return CP(SAMPLE_PROXIES[:3])


# ── Config fixture ────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def patch_env(monkeypatch):
    """Set test-safe env vars before each test so relay config is clean."""
    monkeypatch.setenv("UPSTREAM_BASE", "https://test-api.example.com/v1")
    monkeypatch.setenv("UPSTREAM_API_KEY", "test-key-12345")
    monkeypatch.setenv("UPSTREAM_AUTH_TYPE", "bearer")
    monkeypatch.setenv("RELAY_PORT", "9999")
    monkeypatch.setenv("MAX_CONCURRENT_UPSTREAM", "10")
    monkeypatch.setenv("MODEL_FILTER_PATTERN", ".*")
    monkeypatch.setenv("LOG_LEVEL", "CRITICAL")
    monkeypatch.setenv("CONSECUTIVE_ERROR_THRESHOLD", "3")
    monkeypatch.setenv("PERMANENT_COOLDOWN_SECONDS", "86400")
    monkeypatch.setenv("RELAY_SHUTDOWN_DRAIN_SECONDS", "0")
