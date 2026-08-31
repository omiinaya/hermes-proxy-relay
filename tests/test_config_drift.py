"""Config drift regression tests (Phase F capstone).

PROVES the refactored config subsystem has no drifted enumeration: the
module globals relay binds (``_CFG_GLOBALS``) must ALWAYS equal what the
config pipeline derives (``_load_config_file -> _merge_config -> build``)
for the same environment. Any future edit that makes the bind list
disagree with the derivation (the historic drift bug class) fails here
instead of silently misconfiguring the relay.

Three invariants under test:

1. IMPORT PARITY  -- after ``importlib.reload(relay)``, every config global
   relay binds equals the matching key in a freshly-derived snapshot built
   from the same ambient env.
2. ENV PRECEDENCE -- empty-string env vars keep behaving as UNSET, so file
   config is not silently clobbered (the ADMIN_API_KEY="" footgun).
3. RELOAD PARITY  -- ``config.reload(path)`` produces the same snapshot as
   building from the same file + ambient env, so hot reload cannot drift
   from a cold start.
"""

import importlib
import logging

import pytest

# Selection of config globals whose derived types are meaningful to check
# (int/float/bool/list coercion happens in build(), not at the call site).
_TYPED_KEYS = [
    "RELAY_PORT",
    "MAX_CONCURRENT_UPSTREAM",
    "DYNAMIC_CAP_ENABLED",
    "DYNAMIC_CAP_CPU_TARGET_PCT",
    "DYNAMIC_CAP_MIN",
    "DYNAMIC_CAP_MAX",
    "MAX_QUEUED_REQUESTS",
    "UPSTREAM_READ_TIMEOUT",
    "STREAM_IDLE_TIMEOUT",
    "CONSECUTIVE_ERROR_THRESHOLD",
    "PERMANENT_COOLDOWN_SECONDS",
    "MAX_REQUEST_RETRIES",
    "RETRY_BACKOFF_BASE",
    "RETRY_BACKOFF_MAX",
    "AUTH_SWITCH_ENABLED",
    "AUTH_SWITCH_CANDIDATES",
    "AUTH_SWITCH_TRIGGER_THRESHOLD",
    "AUTH_SWITCH_WINDOW_S",
    "CLIENT_POOL_MAX",
    "MODEL_FILTER_PATTERN",
]


def _derive(path: str) -> dict:
    """Reproduce the config pipeline exactly as _Config._build_from does."""
    from relay.config import _load_config_file, _merge_config

    file_cfg = _load_config_file(path) if path else {}
    return _merge_config(file_cfg)


def _build_from(path: str) -> dict:
    from relay.config import build

    return build(_derive(path))


class TestConfigDrift:
    @pytest.fixture()
    def relay(self, monkeypatch, tmp_path):
        """Freshly-imported relay module bound to ambient env, deterministic.

        Points AUTH_STATE_PATH at an EMPTY file so the auth-switcher's
        persisted-state override cannot fire (it would legitimately change
        some config globals post-build, e.g. UPSTREAM_AUTH_TYPE) and the
        config-truth parity assertion stays exact for every bound global.
        """
        empty_state = tmp_path / "empty-auth-state.json"
        empty_state.write_text("{}")
        monkeypatch.setenv("AUTH_STATE_PATH", str(empty_state))

        import relay.relay as r

        previous_level = r.logger.level
        importlib.reload(r)
        # NOTE: this is a PERSISTENT module mutation — restore it on teardown
        # or every later caplog/log-level test in the suite goes quiet.
        r.logger.setLevel(logging.CRITICAL)
        yield r
        r.logger.setLevel(previous_level)

    def test_import_time_globals_match_fresh_pipeline(self, relay):
        """Every bound global equals the pipeline derivation for same env."""
        path = relay._CONFIG_PATH
        expected = _build_from(path)
        for name in relay._CFG_GLOBALS:
            if name not in expected:
                raise AssertionError(f"derived snapshot missing key {name!r}")
            assert getattr(relay, name) == expected[name], (
                f"drift: relay.{name} != config pipeline under same env"
            )

    def test_typed_values_are_derived(self, relay):
        """build() coerces types; relay globals agree in kind and value."""
        expected = _build_from(relay._CONFIG_PATH)
        for name in _TYPED_KEYS:
            bound = getattr(relay, name)
            derived = expected[name]
            assert type(bound) is type(derived), (
                f"type drift on {name}: relay={type(bound)}, build={type(derived)}"
            )
            assert bound == derived

    def test_empty_env_does_not_override_file(self, relay, monkeypatch, tmp_path):
        """Empty-string env behaves as UNSET (historic env-wins-or contract)."""
        cfg = tmp_path / "drift.json"
        cfg.write_text('{"UPSTREAM_BASE": "https://file.example.com/v1"}')
        monkeypatch.setenv("RELAY_CONFIG", str(cfg))
        monkeypatch.setenv("UPSTREAM_BASE", "")
        importlib.reload(relay)
        # Empty env must NOT clobber the file's upstream base.
        assert relay.UPSTREAM_BASE == "https://file.example.com/v1"

    def test_reload_matches_fresh_pipeline(self, relay, tmp_path):
        """config.reload(path) == building from the same file + ambient env."""
        from relay.config import config as cfg

        cfg_file = tmp_path / "drift-reload.json"
        cfg_file.write_text(
            '{"UPSTREAM_BASE": "https://reload.example.com/v1",'
            ' "MAX_CONCURRENT_UPSTREAM": 41}'
        )
        cfg.reload(str(cfg_file))
        snap = cfg.snapshot()
        fresh = _build_from(str(cfg_file))
        for k in snap:
            assert snap[k] == fresh[k], f"reload drift on {k}: {snap[k]} != {fresh[k]}"

    def test_build_keys_cover_all_binding_globals(self, relay):
        """The binding list and the derivation agree on the key SET."""
        expected = _build_from(relay._CONFIG_PATH)
        missing_in_binding = sorted(set(expected) - set(relay._CFG_GLOBALS))
        missing_in_derive = sorted(set(relay._CFG_GLOBALS) - set(expected))
        # Internal/derived-only keys exist in the snapshot by design; the
        # contract is that every binding global exists in the derivation.
        assert not missing_in_derive, (
            "binding globals not derivable: " + ", ".join(missing_in_derive)
        )
        # Noise keys (derived-but-not-bound) are allowed, list them for info.
        noise = [k for k in missing_in_binding if k not in ("_model_filter_re",)]
        assert not noise, "derived keys never bound: " + ", ".join(noise)
