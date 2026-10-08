"""Remote harness connector application for Okto Nexus.

This package is an application: it imports canonical agent credentials, binds
identity/harness/project, administers managed runtimes through
``okto-nexus-connector-core`` and maintains an outbound WSS control channel. It
contains no MCP server, proxy or relay of any transport, no Nexus user login
and no copy of the native adapters.
"""

__version__ = "0.0.7"

from .errors import ConnectorError

__all__ = ["ConnectorError", "__version__"]
