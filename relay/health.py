"""Module: health.

The background proxy health checker — a long-running task that periodically
probes proxies (reviving dead ones, marking persistently-failing ones dead).

Relocated verbatim from relay.relay (2026-09-01, G4 extraction). Relay
module-globals AND module objects (asyncio, time, httpx, logger) are
dereferenced through the live ``_relay_globals`` seam (installed by relay.relay
via ``set_relay_globals``) — the same pattern as relay/pool.py,
relay/auth_switcher.py, and the route modules — so monkeypatching
``relay_mod.asyncio``/``relay_mod.logger``/``relay_mod.httpx``/``relay_mod.time``
or any relay global at call time is honored.
"""

from relay.pool import ProxyEntry


# Live relay globals seam (same contract as relay/pool.py, relay/auth_switcher.py).
_relay_globals: dict = {}


def set_relay_globals(globals_dict: dict) -> None:
    """Install the LIVE relay module globals dict (not a copy)."""
    global _relay_globals
    _relay_globals = globals_dict


def _G(name: str):
    """Dereference a relay module-global (or module object) by name at call time."""
    return _relay_globals[name]



async def _proxy_health_check():
    """Background task: periodically test each proxy's connectivity.

    Attempts a connection through each proxy to verify it's alive.
    Dead proxies are marked as permanently failed — BUT only if at
    least one other proxy succeeded in the same sweep. If every proxy
    fails at once, the health target is likely down or the network is
    partitioned — marking all proxies dead would be wrong, so they're
    left alive and a warning is logged.

    A single partial-sweep failure does NOT kill a proxy: some proxy
    networks whitelist only API domains and block generic targets like
    httpbin.org. A proxy is only marked permanently dead after
    HEALTH_FAIL_THRESHOLD consecutive failures in separate sweeps.
    When UPSTREAM_BASE is configured, the health check targets it
    (the endpoint proxies are actually used for), falling back to
    PROXY_HEALTH_CHECK_URL.
    """
    if _G('PROXY_HEALTH_CHECK_INTERVAL') <= 0:
        _G('logger').info("Proxy health checker disabled (PROXY_HEALTH_CHECK_INTERVAL=0)")
        return
    # Consecutive health-check failures per proxy URL. Reset on success.
    health_fail_count: dict[str, int] = {}
    while True:
        # A hot-reload can set PROXY_HEALTH_CHECK_INTERVAL to 0 mid-run —
        # guard INSIDE the loop so the checker backs off instead of
        # spinning on asyncio.sleep(0) and hammering the target.
        if _G('PROXY_HEALTH_CHECK_INTERVAL') <= 0:
            await _G('asyncio').sleep(60)
            continue
        try:
            await _G('asyncio').sleep(_G('PROXY_HEALTH_CHECK_INTERVAL'))
            if _G('pool').total == 0:
                continue

            # Prefer checking the real upstream — it's the endpoint the
            # proxies are actually used for. Fall back to the configured
            # target when UPSTREAM_BASE is empty.
            check_url = _G('PROXY_HEALTH_CHECK_URL')
            if _G('UPSTREAM_BASE'):
                check_url = f"{_G('UPSTREAM_BASE')}/models"

            healthy = 0
            failures: list[tuple[ProxyEntry, str]] = []
            now = _G('time').monotonic()
            # Probe only proxies that need attention: permanently-dead (for
            # revival), cooling (verify recovery), or never-used (new/untested).
            # Healthy, recently-successful proxies are NOT hammered every sweep
            # — real traffic validates them, and this cuts upstream load to
            # ~zero when the pool is healthy (the old code probed every proxy
            # in the pool on every sweep, which was ~N requests/min of load
            # on the real upstream for no benefit).
            entries = [
                e for e in _G('pool')._proxies
                if e.permanently_dead or e.cooldown_until > now or e.total_ok == 0
            ]
            if not entries:
                # Fully healthy pool — clear any lingering failure counters and
                # skip the sweep (no upstream load when nothing needs checking).
                health_fail_count.clear()
                continue

            # Bounded-concurrency sweep. The old serial loop awaited each
            # proxy in turn — a 250-proxy pool took ~N × probe-time per
            # sweep (minutes when proxies stall). Probing ALL at once is
            # worse (250 concurrent upstream requests = rate-limit bait).
            # HEALTH_CHECK_CONCURRENCY (default 20) caps in-flight probes
            # so a sweep wall-time is ~N/concurrency × probe-time.
            probe_sem = _G('asyncio').Semaphore(max(1, _G('HEALTH_CHECK_CONCURRENCY')))

            async def _probe(entry: ProxyEntry):
                nonlocal healthy
                async with probe_sem:
                    try:
                        # Reuse a warm pooled client when one is cached for this
                        # proxy (it still has its client from real traffic), so
                        # the revival probe avoids a fresh SOCKS5+TLS handshake
                        # and keeps probing off the fast path. Fall back to a
                        # dedicated fresh client otherwise — this is also the
                        # path the health tests drive (they seed no pool).
                        pooled_client = _G('_client_pool').get(entry.url)
                        if pooled_client is not None:
                            async with pooled_client.stream(
                                "GET", check_url, timeout=_G('httpx').Timeout(10.0)
                            ) as sresp:
                                status_code = sresp.status_code
                        else:
                            transport = _G('httpx').AsyncHTTPTransport(proxy=entry.url)
                            async with _G('httpx').AsyncClient(
                                transport=transport, timeout=_G('httpx').Timeout(10.0)
                            ) as fresh_client:
                                fresp = await fresh_client.get(check_url, timeout=10.0)
                                status_code = fresp.status_code
                        # Revival bar: require a genuine 2xx/3xx success
                        # (<400), NOT just "the proxy answered something".
                        # A 401/403/redirect from a blocking or irrelevant
                        # health target is not proof the proxy serves real
                        # traffic — reviving a dead proxy on one such probe
                        # would put it back in rotation for a request that
                        # then fails. Symmetric with HEALTH_FAIL_THRESHOLD
                        # for death, a single <400 success revives (only
                        # permanent-death proxies need any revival at all).
                        if status_code < 400:
                            # A previously-dead proxy that now responds is
                            # revived. next() skips permanently_dead
                            # proxies, so the health checker is the only
                            # automated verifier — "permanently dead"
                            # means dead until verified otherwise, not
                            # dead forever.
                            healthy += 1
                            if entry.permanently_dead:
                                _G('pool').record_success(entry)
                                _G('logger').info(
                                    f"Health check: proxy {_G('_mask_proxy_url')(entry.url)} "
                                    f"recovered — revived"
                                )
                        else:
                            failures.append((entry, f"Health check returned {status_code}"))
                    except Exception:
                        failures.append((entry, "Health check connection failed"))

            # healthy/failures are only touched in no-await critical
            # sections (single increments/append + no awaits between
            # read and write), so they are atomic under the asyncio loop.
            await _G('asyncio').gather(*(_probe(e) for e in entries))

            if failures and healthy == 0:
                # Everything failed — the health target is probably down,
                # not the proxies. Don't nuke the pool.
                _G('logger').warning(
                    f"Health check: ALL {len(failures)} proxies failed — "
                    f"health target may be unreachable; leaving proxies alive"
                )
                # Reset counters — this was a target problem, not proxy problems
                for entry, _ in failures:
                    health_fail_count.pop(entry.url, None)
            elif failures:
                for entry, reason in failures:
                    count = health_fail_count.get(entry.url, 0) + 1
                    health_fail_count[entry.url] = count
                    if count >= _G('HEALTH_FAIL_THRESHOLD'):
                        _G('pool').record_permanent_failure(entry, reason=reason)
                        _G('logger').warning(
                            f"Health check: proxy {_G('_mask_proxy_url')(entry.url)} — "
                            f"marked permanently unavailable after {count} "
                            f"consecutive failures ({reason})"
                        )
                        health_fail_count.pop(entry.url, None)
                    else:
                        _G('logger').warning(
                            f"Health check: proxy {_G('_mask_proxy_url')(entry.url)} "
                            f"failed ({count}/{_G('HEALTH_FAIL_THRESHOLD')} consecutive) — "
                            f"not yet marked dead ({reason})"
                        )
                _G('logger').info(
                    f"Health check: {healthy} healthy, {len(failures)} failed "
                    f"({_G('pool').available_count}/{_G('pool').total} available)"
                )
            elif healthy:
                # Any proxy that now succeeds resets its failure counter
                for url in list(health_fail_count):
                    health_fail_count.pop(url, None)
                _G('logger').info(
                    f"Health check: {healthy} healthy "
                    f"({_G('pool').available_count}/{_G('pool').total} available)"
                )
        except _G('asyncio').CancelledError:
            break
        except Exception as e:
            _G('logger').error(f"Health check error: {e}")

