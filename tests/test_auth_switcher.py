"""Tests for the AuthSwitcher — smart upstream auth-type fallback.

Covers the failure-classification logic (only 401 is an auth signal),
the probe/switch state machine, anti-flap rails, persistence, and the
end-to-end hook in _proxy_request (a mock upstream that flips its auth
method mid-stream).
"""

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest


def make_switcher(**kw):
    from relay.relay import AuthSwitcher
    defaults = dict(
        candidates=("bearer", "x-api-key"),
        trigger_threshold=3,
        probe_successes=2,
        cooldown_s=300,
        max_per_window=3,
        window_s=3600,
        state_path="",
        enabled=True,
    )
    defaults.update(kw)
    return AuthSwitcher(**defaults)


# ═══════════════════════════════════════════════════════════════════
#  observe() — failure classification
# ═══════════════════════════════════════════════════════════════════


class TestObserve:
    def test_401_increments_consecutive(self):
        s = make_switcher()
        s.observe(401)
        s.observe(401)
        assert s._consecutive_401 == 2
        assert s._total_401 == 2

    def test_success_resets(self):
        s = make_switcher()
        s.observe(401)
        s.observe(401)
        s.observe(200)
        assert s._consecutive_401 == 0

    def test_5xx_neither_counts_nor_resets(self):
        s = make_switcher()
        s.observe(401)
        s.observe(500)
        assert s._consecutive_401 == 1  # streak survives — auth may still be broken

    def test_429_neither_counts_nor_resets(self):
        s = make_switcher()
        s.observe(401)
        s.observe(429)
        assert s._consecutive_401 == 1

    def test_403_neither_counts_nor_resets(self):
        s = make_switcher()
        s.observe(401)
        s.observe(403)
        assert s._consecutive_401 == 1

    def test_disabled_is_noop(self):
        s = make_switcher(enabled=False)
        s.observe(401)
        assert s._consecutive_401 == 0


# ═══════════════════════════════════════════════════════════════════
#  should_probe() — trigger threshold + anti-flap rails
# ═══════════════════════════════════════════════════════════════════


class TestShouldProbe:
    def test_below_threshold(self):
        s = make_switcher(trigger_threshold=3)
        s.observe(401)
        s.observe(401)
        assert not s.should_probe()

    def test_at_threshold(self):
        s = make_switcher(trigger_threshold=3)
        s.observe(401)
        s.observe(401)
        s.observe(401)
        assert s.should_probe()

    def test_cooldown_blocks(self):
        s = make_switcher(cooldown_s=300)
        s._last_probe_ts = time.monotonic()
        s.observe(401)
        s.observe(401)
        s.observe(401)
        assert not s.should_probe()

    def test_max_per_window_blocks_and_alerts(self):
        s = make_switcher(max_per_window=2, window_s=3600)
        now = time.monotonic()
        s._switch_ts.extend([now - 1, now - 2])  # 2 switches already in window
        s.observe(401)
        s.observe(401)
        s.observe(401)
        assert not s.should_probe()
        assert s._alert == "flapping"

    def test_old_switches_pruned_outside_window(self):
        s = make_switcher(max_per_window=2, window_s=3600)
        now = time.monotonic()
        s._switch_ts.extend([now - 4000, now - 4001])  # both outside window
        s.observe(401)
        s.observe(401)
        s.observe(401)
        assert s.should_probe()

    def test_disabled_never_probes(self):
        s = make_switcher(enabled=False)
        s.observe(401)
        s.observe(401)
        s.observe(401)
        assert not s.should_probe()


# ═══════════════════════════════════════════════════════════════════
#  probe_and_switch() — the state machine
# ═══════════════════════════════════════════════════════════════════


class TestProbeAndSwitch:
    @pytest.fixture(autouse=True)
    def fresh_pool(self, monkeypatch):
        import relay.relay as relay_mod
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        relay_mod.UPSTREAM_BASE = "https://upstream.example.com/v1"
        relay_mod.UPSTREAM_API_KEY = "test-key"
        relay_mod.UPSTREAM_AUTH_TYPE = "bearer"
        monkeypatch.setattr(relay_mod, "AUTH_SWITCH_ENABLED", True)
        yield relay_mod
        relay_mod.auth_switcher.reset()
        relay_mod.UPSTREAM_AUTH_TYPE = "bearer"

    async def test_switches_to_verified_candidate(self):
        import relay.relay as relay_mod

        async def fake_probe(auth_type):
            return "ok" if auth_type == "x-api-key" else "inconclusive"

        s = make_switcher()
        with patch.object(s, "_probe_auth", side_effect=fake_probe):
            assert await s.probe_and_switch() is True
        assert relay_mod.UPSTREAM_AUTH_TYPE == "x-api-key"
        assert s._switch_history[-1]["from"] == "bearer"
        assert s._switch_history[-1]["to"] == "x-api-key"
        assert s._switches_done == 1

    async def test_all_rejected_sets_key_revoked_alert(self):
        import relay.relay as relay_mod

        async def fake_probe(auth_type):
            return "rejected"

        s = make_switcher()
        with patch.object(s, "_probe_auth", side_effect=fake_probe):
            assert await s.probe_and_switch() is False
        assert relay_mod.UPSTREAM_AUTH_TYPE == "bearer"  # untouched
        assert s._alert == "key_revoked"

    async def test_inconclusive_does_not_switch(self):
        import relay.relay as relay_mod

        async def fake_probe(auth_type):
            return "inconclusive"

        s = make_switcher()
        with patch.object(s, "_probe_auth", side_effect=fake_probe):
            assert await s.probe_and_switch() is False
        assert relay_mod.UPSTREAM_AUTH_TYPE == "bearer"
        assert s._alert is None  # NOT an auth problem — no key_revoked alarm

    async def test_candidate_skips_current_auth_type(self):
        import relay.relay as relay_mod
        relay_mod.UPSTREAM_AUTH_TYPE = "x-api-key"

        s = make_switcher()
        probed = []
        async def fake_probe(auth_type):
            probed.append(auth_type)
            return "ok"

        with patch.object(s, "_probe_auth", side_effect=fake_probe):
            await s.probe_and_switch()
        # Should never probe the CURRENT type — only alternates
        assert "x-api-key" not in probed
        assert probed == ["bearer"]

    async def test_no_candidates_returns_false(self):
        s = make_switcher(candidates=("bearer",))
        assert await s.probe_and_switch() is False

    async def test_probe_running_lock_prevents_duplicate(self):
        s = make_switcher()
        s._probe_running = True
        assert await s.probe_and_switch() is False

    async def test_probe_sets_last_probe_timestamp(self):
        s = make_switcher()
        async def fake_probe(auth_type):
            return "inconclusive"
        with patch.object(s, "_probe_auth", side_effect=fake_probe):
            await s.probe_and_switch()
        assert s._last_probe_ts > 0


# ═══════════════════════════════════════════════════════════════════
#  _probe_auth() — real probe through the pool
# ═══════════════════════════════════════════════════════════════════


class TestProbeAuth:
    @pytest.fixture(autouse=True)
    def fresh_pool(self, monkeypatch):
        import relay.relay as relay_mod
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
            "socks5://u2:p2@192.168.1.11:1080",
        ])
        relay_mod.UPSTREAM_BASE = "https://upstream.example.com/v1"
        relay_mod.UPSTREAM_API_KEY = "test-key"
        yield relay_mod

    def make_client(self, handler):
        transport = httpx.MockTransport(handler)
        return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(5.0))

    @staticmethod
    def fake_borrow_for(handler):
        """Return an async-context-manager _borrow_client backed by MockTransport."""
        from contextlib import asynccontextmanager

        def build_client():
            transport = httpx.MockTransport(handler)
            return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(5.0))

        @asynccontextmanager
        async def fake_borrow(url):
            yield build_client()

        return fake_borrow

    async def test_probe_returns_ok_on_200s(self, monkeypatch):
        import relay.relay as relay_mod
        s = make_switcher(probe_successes=2)

        def handler(request):
            return httpx.Response(200, json={"data": []})

        monkeypatch.setattr(relay_mod, "_borrow_client", self.fake_borrow_for(handler))
        assert await s._probe_auth("x-api-key") == "ok"

    async def test_probe_returns_rejected_on_401(self, monkeypatch):
        import relay.relay as relay_mod
        s = make_switcher(probe_successes=2)

        def handler(request):
            return httpx.Response(401, json={"error": {"message": "invalid api key"}})

        monkeypatch.setattr(relay_mod, "_borrow_client", self.fake_borrow_for(handler))
        assert await s._probe_auth("x-api-key") == "rejected"

    async def test_probe_inconclusive_on_5xx(self, monkeypatch):
        import relay.relay as relay_mod
        s = make_switcher(probe_successes=2)

        def handler(request):
            return httpx.Response(503, json={"error": "upstream down"})

        monkeypatch.setattr(relay_mod, "_borrow_client", self.fake_borrow_for(handler))
        assert await s._probe_auth("x-api-key") == "inconclusive"

    async def test_probe_inconclusive_on_connect_error(self, monkeypatch):
        import relay.relay as relay_mod
        s = make_switcher(probe_successes=2)

        def handler(request):
            raise httpx.ConnectError("proxy refused")

        monkeypatch.setattr(relay_mod, "_borrow_client", self.fake_borrow_for(handler))
        assert await s._probe_auth("x-api-key") == "inconclusive"

    async def test_probe_inconclusive_without_upstream(self, monkeypatch):
        import relay.relay as relay_mod
        monkeypatch.setattr(relay_mod, "UPSTREAM_BASE", "")
        s = make_switcher()
        assert await s._probe_auth("x-api-key") == "inconclusive"

    async def test_probe_uses_explicit_auth_header(self, monkeypatch):
        import relay.relay as relay_mod
        s = make_switcher(probe_successes=1)
        seen = {}

        def handler(request):
            seen["auth"] = request.headers.get("authorization")
            seen["x-api-key"] = request.headers.get("x-api-key")
            return httpx.Response(200, json={"data": []})

        monkeypatch.setattr(relay_mod, "_borrow_client", self.fake_borrow_for(handler))
        # Even though the CURRENT auth type is bearer, probing x-api-key
        # must send X-API-Key (the override is what makes the probe valid).
        relay_mod.UPSTREAM_AUTH_TYPE = "bearer"
        await s._probe_auth("x-api-key")
        assert seen["auth"] is None
        assert seen["x-api-key"] == "test-key"


# ═══════════════════════════════════════════════════════════════════
#  persistence
# ═══════════════════════════════════════════════════════════════════


class TestPersistence:
    def test_save_and_load_roundtrip(self, tmp_path):
        import relay.relay as relay_mod
        state_path = str(tmp_path / "auth_state.json")
        s = make_switcher(state_path=state_path)
        relay_mod.UPSTREAM_AUTH_TYPE = "x-api-key"
        s._switches_done = 4
        s._save_state()
        s2 = make_switcher(state_path=state_path)
        assert s2.load_state() == "x-api-key"

    def test_load_state_missing_file(self, tmp_path):
        s = make_switcher(state_path=str(tmp_path / "nope.json"))
        assert s.load_state() is None

    def test_load_state_rejects_unknown_type(self, tmp_path):
        import relay.relay as relay_mod
        state_path = str(tmp_path / "auth_state.json")
        state_path_abs = str(tmp_path / "auth_state.json")
        with open(state_path_abs, "w") as f:
            json.dump({"auth_type": "spooky-method"}, f)
        s = make_switcher(state_path=state_path)
        assert s.load_state() is None

    def test_load_state_corrupt_file(self, tmp_path):
        state_path = str(tmp_path / "auth_state.json")
        with open(state_path, "w") as f:
            f.write("{not json")
        s = make_switcher(state_path=state_path)
        assert s.load_state() is None

    def test_save_state_no_path_is_silent(self):
        s = make_switcher(state_path="")
        s._save_state()  # must not raise


# ═══════════════════════════════════════════════════════════════════
#  status() / reconfigure() / reset()
# ═══════════════════════════════════════════════════════════════════


class TestStatus:
    def test_status_shape(self):
        import relay.relay as relay_mod
        s = make_switcher()
        st = s.status()
        for key in ("enabled", "current_auth_type", "consecutive_401s",
                    "total_401s", "probes_run", "switches", "alert",
                    "candidates", "switch_history"):
            assert key in st

    def test_reconfigure_updates_knobs(self):
        s = make_switcher()
        s.reconfigure(candidates=["query"], trigger_threshold=5, enabled=False)
        assert s.candidates == ["query"]
        assert s.trigger_threshold == 5
        assert not s.enabled

    def test_reset_clears_state(self):
        s = make_switcher()
        s._consecutive_401 = 7
        s._switches_done = 3
        s._switch_history.append({"ts": "x", "from": "a", "to": "b"})
        s._alert = "key_revoked"
        s.reset()
        assert s._consecutive_401 == 0
        assert s._switches_done == 0
        assert s._switch_history == []
        assert s._alert is None


# ═══════════════════════════════════════════════════════════════════
#  end-to-end: _proxy_request with an upstream that flips auth
# ═══════════════════════════════════════════════════════════════════


class TestProxyRequestAuthSwitch:
    """The money test — a mock upstream that rejects Bearer but accepts
    X-API-Key. Three consecutive requests should: 401, 401, then trigger
    the probe, switch to x-api-key, and the third request's retry succeeds."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch):
        import relay.relay as relay_mod
        relay_mod.pool = relay_mod.CooldownPool([
            "socks5://u1:p1@192.168.1.10:1080",
        ])
        relay_mod.UPSTREAM_BASE = "https://upstream.example.com/v1"
        relay_mod.UPSTREAM_API_KEY = "test-key"
        relay_mod.UPSTREAM_AUTH_TYPE = "bearer"
        relay_mod.MAX_REQUEST_RETRIES = 1
        monkeypatch.setattr(relay_mod, "AUTH_SWITCH_ENABLED", True)
        relay_mod.auth_switcher.reset()
        relay_mod.auth_switcher.enabled = True
        relay_mod.auth_switcher.candidates = ["bearer", "x-api-key"]
        relay_mod.auth_switcher.trigger_threshold = 3
        relay_mod.auth_switcher.probe_successes = 1
        relay_mod.auth_switcher.cooldown_s = 0
        yield relay_mod
        relay_mod.auth_switcher.reset()
        relay_mod.UPSTREAM_AUTH_TYPE = "bearer"
        relay_mod.auth_switcher.enabled = True

    def make_client(self, handler):
        transport = httpx.MockTransport(handler)
        return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(5.0))

    @staticmethod
    def fake_borrow_for(handler):
        """Return an async-context-manager _borrow_client backed by MockTransport."""
        from contextlib import asynccontextmanager

        def build_client():
            transport = httpx.MockTransport(handler)
            return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(5.0))

        @asynccontextmanager
        async def fake_borrow(url):
            yield build_client()

        return fake_borrow

    async def test_upstream_auth_flip_self_heals(self, monkeypatch):
        import relay.relay as relay_mod
        requests = []

        def handler(request):
            requests.append({
                "path": request.url.path,
                "auth": request.headers.get("authorization"),
                "x-api-key": request.headers.get("x-api-key"),
            })
            if request.headers.get("x-api-key") == "test-key":
                return httpx.Response(200, json={"data": [], "ok": True})
            return httpx.Response(401, json={"error": {"message": "invalid api key"}})

        monkeypatch.setattr(relay_mod, "_borrow_client", self.fake_borrow_for(handler))
        monkeypatch.setattr(relay_mod, "_make_streaming_client", self.fake_borrow_for(handler))

        body = b'{"model": "m1", "messages": [{"role": "user", "content": "hi"}]}'

        # Request 1: Bearer → 401
        r1 = await relay_mod._proxy_request("POST", "/chat/completions", body,
                                            {"content-type": "application/json"}, "")
        assert r1.status_code == 401

        # Request 2: still Bearer → 401 (threshold 3, only 2 seen)
        r2 = await relay_mod._proxy_request("POST", "/chat/completions", body,
                                            {"content-type": "application/json"}, "")
        assert r2.status_code == 401

        # Request 3: third 401 → probe verifies x-api-key → switch → retry → 200
        r3 = await relay_mod._proxy_request("POST", "/chat/completions", body,
                                            {"content-type": "application/json"}, "")
        assert r3.status_code == 200
        assert relay_mod.UPSTREAM_AUTH_TYPE == "x-api-key"
        assert relay_mod.auth_switcher._switches_done == 1

        # The retried request must have gone out with X-API-Key
        assert any(req["x-api-key"] == "test-key" for req in requests)

    async def test_key_revoked_all_methods_401_no_switch(self, monkeypatch):
        import relay.relay as relay_mod

        def handler(request):
            return httpx.Response(401, json={"error": {"message": "nope"}})

        monkeypatch.setattr(relay_mod, "_borrow_client", self.fake_borrow_for(handler))
        monkeypatch.setattr(relay_mod, "_make_streaming_client", self.fake_borrow_for(handler))
        relay_mod.auth_switcher.probe_successes = 1

        body = b'{"model": "m1", "messages": [{"role": "user", "content": "hi"}]}'

        for _ in range(3):
            await relay_mod._proxy_request("POST", "/chat/completions", body,
                                           {"content-type": "application/json"}, "")
        # Third request crossed the threshold, probed, found nothing → no switch
        assert relay_mod.UPSTREAM_AUTH_TYPE == "bearer"
        assert relay_mod.auth_switcher._alert == "key_revoked"
        assert relay_mod.auth_switcher._switches_done == 0

    async def test_success_resets_streak_no_switch(self, monkeypatch):
        import relay.relay as relay_mod
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] <= 2:
                return httpx.Response(401, json={"error": {"message": "bad"}})
            return httpx.Response(200, json={"data": []})

        monkeypatch.setattr(relay_mod, "_borrow_client", self.fake_borrow_for(handler))
        monkeypatch.setattr(relay_mod, "_make_streaming_client", self.fake_borrow_for(handler))
        body = b'{"model": "m1", "messages": [{"role": "user", "content": "hi"}]}'

        await relay_mod._proxy_request("POST", "/chat/completions", body, {}, "")
        await relay_mod._proxy_request("POST", "/chat/completions", body, {}, "")
        await relay_mod._proxy_request("POST", "/chat/completions", body, {}, "")
        # 401, 401, 200 → success resets the streak → no switch, no probe
        assert relay_mod.UPSTREAM_AUTH_TYPE == "bearer"
        assert relay_mod.auth_switcher._switches_done == 0
        assert relay_mod.auth_switcher._consecutive_401 == 0
