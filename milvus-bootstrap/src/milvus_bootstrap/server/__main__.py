"""Run the daemon over a Unix domain socket: python -m milvus_bootstrap.server --uds PATH"""
from __future__ import annotations

import argparse

import uvicorn


def run_web(host: str, port: int) -> None:
    if host == "0.0.0.0":
        print(f"[警告] 绑定 {host}:{port} 会把包含 install/delete 等可变更操作的 API 暴露到网络。")
    print(f"WebUI: http://{host}:{port}/")
    uvicorn.run("milvus_bootstrap.server.app:app", host=host, port=port, log_level="warning")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uds", required=True, help="unix domain socket path")
    args = ap.parse_args()
    uvicorn.run("milvus_bootstrap.server.app:app", uds=args.uds, log_level="warning")


if __name__ == "__main__":
    main()
