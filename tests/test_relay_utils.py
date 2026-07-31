"""Tests for relay utility functions: model filtering, header building, retry-after parsing.

These tests exercise the pure functions in relay.py without needing async infrastructure.
"""

import re
import pytest


# ── Model filtering ───────────────────────────────────────────────


class TestModelFilter:
    """_model_allowed() checks model names against MODEL_FILTER_PATTERN regex."""

    @pytest.fixture(autouse=True)
    def setup_filter(self):
        """Import the actual _model_allowed function and compile the default pattern."""
        from relay.relay import _model_allowed
        # _model_filter_re is compiled at import time from MODEL_FILTER_PATTERN
        self._model_allowed = _model_allowed

    def test_default_pattern_matches_all(self):
        """Default .* should match any model name."""
        assert self._model_allowed("gpt-4") is True
        assert self._model_allowed("gpt-4o-mini") is True
        assert self._model_allowed("claude-sonnet-4") is True
        assert self._model_allowed("") is True

    def test_custom_pattern_match(self):
        """Free models filter should only match -free suffixed names."""

        # Temporarily override filter re — we test the function with a custom pattern
        import relay.relay as relay_mod
        original_re = relay_mod._model_filter_re
        try:
            relay_mod._model_filter_re = re.compile(r"-free$")
            assert relay_mod._model_allowed("gpt-4o-free") is True
            assert relay_mod._model_allowed("gpt-4o") is False
            assert relay_mod._model_allowed("claude-sonnet-free") is True
            assert relay_mod._model_allowed("claude-sonnet-4") is False
        finally:
            relay_mod._model_filter_re = original_re

    def test_filter_rejects_empty_pattern(self):
        """Pattern that matches nothing should reject all."""
        from relay.relay import _model_allowed
        import relay.relay as relay_mod
        original_re = relay_mod._model_filter_re
        try:
            relay_mod._model_filter_re = re.compile(r"(?!x)x")  # never matches
            assert _model_allowed("gpt-4") is False
        finally:
            relay_mod._model_filter_re = original_re


# ── Header building ────────────────────────────────────────────────


class TestBuildHeaders:
    """_build_headers() should strip managed headers and inject upstream auth."""

    @pytest.fixture(autouse=True)
    def setup(self):
        from relay.relay import _build_headers
        self._build_headers = _build_headers

    def test_strips_authorization(self, monkeypatch):
        """Client Authorization header should be replaced with upstream key."""
        monkeypatch.setattr("relay.relay.UPSTREAM_API_KEY", "sk-upstream")
        monkeypatch.setattr("relay.relay.UPSTREAM_AUTH_TYPE", "bearer")

        headers = {
            "Authorization": "Bearer sk-original",
            "Content-Type": "application/json",
        }
        result = self._build_headers(headers)
        # Original Bearer value is stripped, replaced by upstream key
        assert result.get("Authorization") == "Bearer sk-upstream"
        assert result["Authorization"] != "Bearer sk-original"

    def test_strips_accept_encoding(self, monkeypatch):
        """Accept-Encoding must be stripped to prevent zstd codec issues."""
        monkeypatch.setattr("relay.relay.UPSTREAM_API_KEY", "sk-upstream")
        monkeypatch.setattr("relay.relay.UPSTREAM_AUTH_TYPE", "bearer")

        headers = {
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Content-Type": "application/json",
        }
        result = self._build_headers(headers)
        assert "accept-encoding" not in {k.lower() for k in result}

    def test_strips_content_length_host_connection(self, monkeypatch):
        """Content-Length, Host, and Connection should be stripped."""
        monkeypatch.setattr("relay.relay.UPSTREAM_API_KEY", "sk-upstream")
        monkeypatch.setattr("relay.relay.UPSTREAM_AUTH_TYPE", "bearer")

        headers = {
            "Content-Length": "123",
            "Host": "localhost:4002",
            "Connection": "keep-alive",
        }
        result = self._build_headers(headers)
        lowered = {k.lower() for k in result}
        assert "content-length" not in lowered
        assert "host" not in lowered
        assert "connection" not in lowered

    def test_passes_other_headers(self, monkeypatch):
        """Content-Type, User-Agent, custom headers should pass through."""
        monkeypatch.setattr("relay.relay.UPSTREAM_API_KEY", "sk-upstream")
        monkeypatch.setattr("relay.relay.UPSTREAM_AUTH_TYPE", "bearer")

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "test-agent/1.0",
            "X-Custom-Header": "custom-value",
        }
        result = self._build_headers(headers)
        assert result.get("Content-Type") == "application/json"
        assert result.get("User-Agent") == "test-agent/1.0"
        assert result.get("X-Custom-Header") == "custom-value"

    def test_x_api_key_auth_type(self, monkeypatch):
        """When UPSTREAM_AUTH_TYPE is x-api-key, use that header instead of Bearer."""
        monkeypatch.setattr("relay.relay.UPSTREAM_API_KEY", "public")
        monkeypatch.setattr("relay.relay.UPSTREAM_AUTH_TYPE", "x-api-key")

        result = self._build_headers({})
        assert "Authorization" not in result
        assert result.get("x-api-key") == "public"

    def test_strips_admin_key_header(self, monkeypatch):
        """X-Admin-Key (relay's own admin auth) must never reach upstream."""
        monkeypatch.setattr("relay.relay.UPSTREAM_API_KEY", "sk-upstream")
        monkeypatch.setattr("relay.relay.UPSTREAM_AUTH_TYPE", "bearer")

        headers = {"X-Admin-Key": "super-secret-admin-key"}
        result = self._build_headers(headers)
        lowered = {k.lower() for k in result}
        assert "x-admin-key" not in lowered
        assert "super-secret-admin-key" not in str(result)

    def test_strips_transfer_encoding(self, monkeypatch):
        """Transfer-Encoding must be stripped (httpx re-frames the body)."""
        monkeypatch.setattr("relay.relay.UPSTREAM_API_KEY", "sk-upstream")
        monkeypatch.setattr("relay.relay.UPSTREAM_AUTH_TYPE", "bearer")

        headers = {"Transfer-Encoding": "chunked"}
        result = self._build_headers(headers)
        lowered = {k.lower() for k in result}
        assert "transfer-encoding" not in lowered


# ── Retry-After parsing ────────────────────────────────────────────


class TestRetryAfter:
    """_parse_retry_after() should handle seconds and HTTP-date formats."""

    @pytest.fixture(autouse=True)
    def setup(self):
        from relay.relay import _parse_retry_after
        self._parse_retry_after = _parse_retry_after

    def test_integer_retry_after(self):
        result = self._parse_retry_after({"retry-after": "120"})
        assert result == 120

    def test_missing_retry_after_default(self):
        result = self._parse_retry_after({})
        assert result == 60

    def test_empty_retry_after_default(self):
        result = self._parse_retry_after({"retry-after": ""})
        assert result == 60

    def test_http_date_format(self):
        """Parse RFC 2822 date format for Retry-After."""
        from datetime import datetime, timezone, timedelta
        future = datetime.now(timezone.utc) + timedelta(seconds=45)
        date_str = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
        result = self._parse_retry_after({"retry-after": date_str})
        assert 40 <= result <= 50  # close to 45s

    def test_invalid_value_returns_default(self):
        """Garbage values should fall back to default 60."""
        result = self._parse_retry_after({"retry-after": "not-a-number"})
        assert result == 60

    def test_negative_seconds_clamped_to_minimum(self):
        """Negative Retry-After (past HTTP-date) clamps to a sane minimum."""
        from datetime import datetime, timezone, timedelta
        past = datetime.now(timezone.utc) - timedelta(seconds=300)
        date_str = past.strftime("%a, %d %b %Y %H:%M:%S GMT")
        result = self._parse_retry_after({"retry-after": date_str})
        assert result >= 10

    def test_zero_seconds_clamped_to_minimum(self):
        """Retry-After: 0 clamps to the 10s minimum."""
        result = self._parse_retry_after({"retry-after": "0"})
        assert result == 10

    def test_negative_integer_clamped_to_minimum(self):
        """Retry-After: -5 clamps to the 10s minimum."""
        result = self._parse_retry_after({"retry-after": "-5"})
        assert result == 10

    def test_naive_http_date_treated_as_utc(self):
        """HTTP-date without timezone suffix is parsed as UTC (RFC 2822)."""
        from datetime import datetime, timezone, timedelta
        future = datetime.now(timezone.utc) + timedelta(seconds=45)
        date_str = future.strftime("%a, %d %b %Y %H:%M:%S")  # no "GMT"
        result = self._parse_retry_after({"retry-after": date_str})
        assert 40 <= result <= 50
