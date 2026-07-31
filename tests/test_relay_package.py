"""Tests for relay/__init__.py — lazy VERSION exposure and package exports."""



class TestRelayPackage:
    def test_version_exported_lazily(self):
        """__getattr__ exposes VERSION/__version__ via relay.relay import."""
        import relay as relay_pkg
        assert relay_pkg.VERSION
        assert relay_pkg.__version__ == relay_pkg.VERSION

    def test_version_matches_relay_module(self):
        """Package VERSION equals relay.relay.VERSION."""
        import relay as relay_pkg
        import relay.relay as relay_module
        assert relay_pkg.VERSION == relay_module.VERSION

    def test_unknown_attribute_raises(self):
        """__getattr__ raises AttributeError for anything else."""
        import relay as relay_pkg
        try:
            relay_pkg.no_such_attribute
        except AttributeError as e:
            assert "no_such_attribute" in str(e)
        else:
            raise AssertionError("expected AttributeError")

    def test_all_exports(self):
        """__all__ contains VERSION and __version__."""
        import relay as relay_pkg
        assert relay_pkg.__all__ == ["VERSION", "__version__"]
