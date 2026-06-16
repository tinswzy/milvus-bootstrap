"""milvus-bootstrap — Milvus install/ops tool.

Thin CLI client + core daemon. All real logic lives in the core
(`milvus_bootstrap.core`); the CLI (`milvus_bootstrap.cli`) is a thin client
that manages the daemon and forwards commands over a local socket.

Architecture (see prototype/design-*.html):
  L0 Clients (cli)
  L1 Command / API   (server)
  L2 Orchestration   (core.tasks)
  L3 ServiceDriver   (core.drivers)   — per-component polymorphism
  L4 PlatformAdapter (core.platform)  — per-platform polymorphism
  L5 Access + State  (core.state)
"""

__version__ = "0.0.1"
