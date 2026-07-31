"""Hermes Proxy Relay — lightweight SOCKS5 rotation relay with dynamic 429 cooldown."""

__all__ = ["VERSION", "__version__"]


def __getattr__(name):
    """Lazily expose VERSION — avoids importing relay.relay at package
    import time (which would break `python -m relay.relay` and slow
    down any consumer that only wants the package docstring)."""
    if name in ("VERSION", "__version__"):
        from relay.relay import VERSION
        return VERSION
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
