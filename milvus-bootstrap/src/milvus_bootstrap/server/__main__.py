"""Run the daemon over a Unix domain socket: python -m milvus_bootstrap.server --uds PATH"""
from __future__ import annotations

import argparse

import uvicorn


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uds", required=True, help="unix domain socket path")
    args = ap.parse_args()
    uvicorn.run("milvus_bootstrap.server.app:app", uds=args.uds, log_level="warning")


if __name__ == "__main__":
    main()
