"""Hermes Proxy Relay — lightweight SOCKS5 rotation relay with dynamic 429 cooldown."""

from relay.relay import VERSION, __doc__ as relay_doc  # noqa: F401

__version__ = VERSION
__all__ = ["VERSION", "__version__"]
