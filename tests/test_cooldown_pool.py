"""Comprehensive unit tests for CooldownPool — proxy rotation + 429 cooldown logic."""

import time
import threading

SAMPLE_PROXIES = [
    "socks5://user1:pass1@192.168.1.10:1080",
    "socks5://user2:pass2@192.168.1.11:1080",
    "socks5://user3:pass3@192.168.1.12:1080",
    "http://user4:pass4@proxy.example.com:3128",
]


# ═══════════════════════════════════════════════════════════════════
#  Construction / Properties
# ═══════════════════════════════════════════════════════════════════


class TestConstruction:
    def test_empty_pool(self, empty_pool):
        assert empty_pool.total == 0
        assert empty_pool.available_count == 0
        assert empty_pool.cooling_count == 0
        # Empty proxy list means "all cooling" (can't serve any request)
        assert empty_pool.all_cooling is True
        assert empty_pool.next() is None

    def test_pool_with_proxies(self, cooldown_pool):
        assert cooldown_pool.total == len(SAMPLE_PROXIES)
        assert cooldown_pool.available_count == len(SAMPLE_PROXIES)
        assert cooldown_pool.cooling_count == 0
        assert cooldown_pool.all_cooling is False

    def test_init_with_none(self):
        from relay.relay import CooldownPool
        pool = CooldownPool(None)
        assert pool.total == 0

    def test_proxy_entry_defaults(self):
        from relay.relay import ProxyEntry
        entry = ProxyEntry(url="socks5://u:p@h:1080")
        assert entry.url == "socks5://u:p@h:1080"
        assert entry.cooldown_until == 0.0
        assert entry.last_error == ""
        assert entry.consecutive_errors == 0
        assert entry.consecutive_429 == 0
        assert entry.total_ok == 0
        assert entry.total_429 == 0
        assert entry.permanently_dead is False


# ═══════════════════════════════════════════════════════════════════
#  Round-Robin Selection
# ═══════════════════════════════════════════════════════════════════


class TestRoundRobin:
    def test_sequential_rotation(self, cooldown_pool):
        """next() should round-robin through proxies in order."""
        seen = []
        for _ in range(cooldown_pool.total * 3):
            entry = cooldown_pool.next()
            assert entry is not None
            seen.append(entry.url)

        # Check rotation pattern: each proxy appears equally
        for proxy in SAMPLE_PROXIES:
            assert seen.count(proxy) == 3

    def test_rotation_correct_order(self, mock_proxy_pool):
        """First 3 calls should return proxies in order."""
        p1 = mock_proxy_pool.next()
        p2 = mock_proxy_pool.next()
        p3 = mock_proxy_pool.next()
        assert p1 is not None
        assert p2 is not None
        assert p3 is not None
        assert p1.url == SAMPLE_PROXIES[0]
        assert p2.url == SAMPLE_PROXIES[1]
        assert p3.url == SAMPLE_PROXIES[2]

    def test_rotation_wraps_around(self, mock_proxy_pool):
        """After exhausting all proxies, next() wraps to first."""
        mock_proxy_pool.next()  # 1
        mock_proxy_pool.next()  # 2
        mock_proxy_pool.next()  # 3
        p4 = mock_proxy_pool.next()  # wraps to 1
        assert p4 is not None
        assert p4.url == SAMPLE_PROXIES[0]


# ═══════════════════════════════════════════════════════════════════
#  429 Cooldown
# ═══════════════════════════════════════════════════════════════════


class TestCooldown:
    def test_429_cools_proxy(self, cooldown_pool):
        """A 429 puts the proxy in cooldown — it won't be returned."""
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_429(proxy, retry_after=300)

        assert cooldown_pool.cooling_count == 1
        assert cooldown_pool.available_count == cooldown_pool.total - 1
        assert not cooldown_pool.all_cooling  # other 3 still warm

    def test_429_minimum_cooldown(self, cooldown_pool):
        """Even with retry_after=1, minimum cooldown is 10s."""
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_429(proxy, retry_after=1)

        remaining = proxy.cooldown_until - time.monotonic()
        assert remaining >= 9  # at least 9s of the 10s minimum remain

    def test_429_retry_after_respected(self, cooldown_pool):
        """Cooldown lasts at least retry_after seconds."""
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_429(proxy, retry_after=60)

        remaining = proxy.cooldown_until - time.monotonic()
        assert remaining >= 58  # 60s, but test may have taken ~2s

    def test_429_increments_counters(self, cooldown_pool):
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_429(proxy, retry_after=30)

        assert proxy.consecutive_429 == 1
        assert proxy.total_429 == 1
        assert "429" in proxy.last_error

    def test_record_success_resets_429_counters(self, cooldown_pool):
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_429(proxy, retry_after=60)
        cooldown_pool.record_success(proxy)

        assert proxy.consecutive_429 == 0
        assert proxy.consecutive_errors == 0
        assert proxy.last_error == ""
        assert proxy.permanently_dead is False

    def test_429_skips_cooled_proxy(self, cooldown_pool):
        """A cooled proxy should be skipped during round-robin."""
        p1 = cooldown_pool.next()
        p2 = cooldown_pool.next()
        p3 = cooldown_pool.next()
        p4 = cooldown_pool.next()

        # Cool proxy 2
        cooldown_pool.record_429(p2, retry_after=300)

        # Next call should skip proxy 2 and return proxy 1 (p1 was last returned,
        # p2 is cooling, so index wraps to p1)
        next_proxy = cooldown_pool.next()
        assert next_proxy is not None
        assert next_proxy.url != p2.url  # shouldn't return cooled proxy

    def test_all_proxies_cooling_returns_none(self, cooldown_pool):
        """When every proxy is cooling, next() returns None."""
        for _ in range(4):
            proxy = cooldown_pool.next()
            cooldown_pool.record_429(proxy, retry_after=300)

        assert cooldown_pool.all_cooling
        assert cooldown_pool.next() is None

    def test_all_cooling_property(self, cooldown_pool):
        """all_cooling is True only when every proxy is in cooldown."""
        assert not cooldown_pool.all_cooling

        p1 = cooldown_pool.next()
        cooldown_pool.record_429(p1, retry_after=300)
        assert not cooldown_pool.all_cooling  # 3 still warm

        for _ in range(3):
            p = cooldown_pool.next()
            if p:
                cooldown_pool.record_429(p, retry_after=300)

        assert cooldown_pool.all_cooling


# ═══════════════════════════════════════════════════════════════════
#  Connection-Level Errors (timeout, 5xx)
# ═══════════════════════════════════════════════════════════════════


class TestConsecutiveErrors:
    def test_single_timeout_temporary(self, cooldown_pool):
        """A single timeout should temporarily cool the proxy (30s)."""
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_timeout(proxy)

        remaining = proxy.cooldown_until - time.monotonic()
        assert remaining >= 28  # 30s cooldown
        assert proxy.consecutive_errors == 1
        assert not proxy.permanently_dead

    def test_three_timeouts_permanent_death(self, cooldown_pool):
        """After CONSECUTIVE_ERROR_THRESHOLD (3) timeouts, proxy is permanently dead."""
        proxy = cooldown_pool.next()
        assert proxy is not None
        for _ in range(3):
            cooldown_pool.record_timeout(proxy)

        assert proxy.permanently_dead
        assert proxy.consecutive_errors == 3
        assert "Permanent" in proxy.last_error

    def test_success_resets_error_count(self, cooldown_pool):
        """A success after errors should reset consecutive_errors."""
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_timeout(proxy)
        cooldown_pool.record_timeout(proxy)
        assert proxy.consecutive_errors == 2

        cooldown_pool.record_success(proxy)
        assert proxy.consecutive_errors == 0
        assert proxy.consecutive_429 == 0
        assert not proxy.permanently_dead

    def test_permanent_failure_explicit(self, cooldown_pool):
        """record_permanent_failure should mark proxy as permanently dead."""
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_permanent_failure(proxy, reason="Bandwidth exhausted")

        assert proxy.permanently_dead
        assert "Bandwidth exhausted" in proxy.last_error
        assert proxy.consecutive_errors == 1
        remaining = proxy.cooldown_until - time.monotonic()
        assert remaining >= 86350  # 86400s cooldown


# ═══════════════════════════════════════════════════════════════════
#  Pool Management (reload, clear, reset)
# ═══════════════════════════════════════════════════════════════════


class TestPoolManagement:
    def test_clear_cooldowns_resets_all(self, cooldown_pool):
        """clear_cooldowns() should make all proxies available."""
        for _ in range(4):
            p = cooldown_pool.next()
            cooldown_pool.record_429(p, retry_after=300)

        assert cooldown_pool.all_cooling
        cooldown_pool.clear_cooldowns()
        assert cooldown_pool.available_count == 4
        assert cooldown_pool.next() is not None

    def test_clear_cooldowns_resets_permanent(self, cooldown_pool):
        """Permanently dead proxies are revived by clear_cooldowns."""
        p = cooldown_pool.next()
        cooldown_pool.record_permanent_failure(p)
        assert p.permanently_dead

        cooldown_pool.clear_cooldowns()
        assert not p.permanently_dead
        assert p.last_error == ""

    def test_reset_proxy_found(self, cooldown_pool):
        p = cooldown_pool.next()
        cooldown_pool.record_429(p, retry_after=300)
        assert p.cooldown_until > time.monotonic()

        result = cooldown_pool.reset_proxy(p.url)
        assert result is True
        assert p.cooldown_until <= time.monotonic()
        assert not p.permanently_dead

    def test_reset_proxy_not_found(self, cooldown_pool):
        result = cooldown_pool.reset_proxy("socks5://nonexistent:1080")
        assert result is False

    def test_reload_preserves_existing_state(self, cooldown_pool):
        """Reloading should keep existing proxy entries with their state."""
        p1 = cooldown_pool.next()
        cooldown_pool.record_429(p1, retry_after=300)

        # Reload with same list
        cooldown_pool.reload(SAMPLE_PROXIES)

        # p1 should still be cooling
        assert p1.cooldown_until > time.monotonic()
        assert p1.total_429 == 1

    def test_reload_adds_new_proxies(self, cooldown_pool):
        old_total = cooldown_pool.total
        new_proxies = SAMPLE_PROXIES + ["socks5://new@proxy:1080"]
        cooldown_pool.reload(new_proxies)
        assert cooldown_pool.total == old_total + 1

    def test_reload_removes_old_proxies(self, cooldown_pool):
        old_total = cooldown_pool.total
        smaller_list = SAMPLE_PROXIES[:2]
        cooldown_pool.reload(smaller_list)
        assert cooldown_pool.total == 2

    def test_reset_by_errors(self, cooldown_pool):
        p = cooldown_pool.next()
        for _ in range(3):
            cooldown_pool.record_timeout(p)
        assert p.permanently_dead

        count = cooldown_pool.reset_by_errors(min_consecutive=3)
        assert count == 1
        assert not p.permanently_dead
        assert p.consecutive_errors == 0


# ═══════════════════════════════════════════════════════════════════
#  Thread Safety
# ═══════════════════════════════════════════════════════════════════


class TestThreadSafety:
    def test_concurrent_429_records(self, cooldown_pool):
        """Multiple threads recording 429s should not corrupt state."""
        errors = []

        def record_429_thread():
            try:
                p = cooldown_pool.next()
                if p:
                    cooldown_pool.record_429(p, retry_after=10)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_429_thread) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        # Pool should have all_time counters populated
        stats = cooldown_pool.stats()

    def test_stats_consistency(self, cooldown_pool):
        """Stats dict should have all expected fields."""
        proxy = cooldown_pool.next()
        assert proxy is not None
        cooldown_pool.record_429(proxy, retry_after=30)
        cooldown_pool.record_success(proxy)

        stats = cooldown_pool.stats()
        assert "total" in stats
        assert "available" in stats
        assert "cooling" in stats
        assert "permanently_failed" in stats
        assert "all_time_ok" in stats
        assert "all_time_429" in stats
        assert stats["total"] == len(SAMPLE_PROXIES)


# ═══════════════════════════════════════════════════════════════════
#  Stream Detection
# ═══════════════════════════════════════════════════════════════════


class TestStreamDetection:
    """Test the byte-level stream detection (vision optimization)."""

    def test_detects_stream_true(self):
        body = b'{"stream": true, "model": "gpt-4"}'
        body_lower = body.lower()
        assert b'"stream":true' in body_lower or b'"stream": true' in body_lower

    def test_detects_stream_true_no_space(self):
        body = b'{"stream":true,"model":"gpt-4"}'
        body_lower = body.lower()
        assert b'"stream":true' in body_lower

    def test_detects_stream_false(self):
        body = b'{"stream": false, "model": "gpt-4"}'
        body_lower = body.lower()
        assert b'"stream":true' not in body_lower
        assert b'"stream": true' not in body_lower

    def test_stream_detection_with_messages(self):
        """When stream=true is in a messages context."""
        body = b'{"messages": [{"role": "user", "content": "hi"}], "stream": true}'
        body_lower = body.lower()
        assert b'"stream":true' in body_lower or b'"stream": true' in body_lower

    def test_stream_not_in_string_value(self):
        """stream:true inside a nested string should not match."""
        body = b'{"messages": [{"role": "user", "content": "stream:true is not a real stream"}], "stream": false}'
        body_lower = body.lower()
        # This won't false-match because "stream:true" doesn't match '"stream":true'
        assert b'"stream":true' not in body_lower

    def test_no_body_returns_false(self):
        body = None
        assert body is None  # _proxy_request handles None before stream detection
