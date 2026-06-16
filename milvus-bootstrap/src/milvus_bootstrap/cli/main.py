"""`mb` — thin CLI client.

It does NOT contain business logic; it manages the core daemon (install / start
/ stop / update) and forwards commands over the local socket.
"""
from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from ..client import DaemonClient

app = typer.Typer(
    help="milvus-bootstrap — 瘦 CLI 客户端（真正的逻辑都在 core daemon）",
    no_args_is_help=True,
    add_completion=False,
)
core_app = typer.Typer(help="管理 core daemon：启停 / 状态 / 更新", no_args_is_help=True)
app.add_typer(core_app, name="core")

console = Console()
client = DaemonClient()


# ---- core lifecycle (the part the CLI actually owns) ----
@core_app.command("start")
def core_start() -> None:
    """启动 core daemon（若已运行则跳过）。"""
    client.ensure_running()
    console.print(f"[green]core daemon 运行中[/]  sock={client.sock}")


@core_app.command("stop")
def core_stop() -> None:
    """停止 core daemon。"""
    ok = client.stop()
    console.print("[green]已停止[/]" if ok else "[yellow]未在运行 / 无法定位 pid[/]")


@core_app.command("status")
def core_status() -> None:
    """core daemon 本地状态。"""
    console.print_json(data=client.local_status())


@core_app.command("update")
def core_update() -> None:
    """自更新 core（stub）。"""
    console.print("[dim]自更新（stub）：后续用 pip / registry 更新 core 包，像 claude code / codex。[/]")


# ---- forwarded commands (logic runs in the daemon) ----
@app.command()
def status() -> None:
    """core 运行时状态（已加载 profiles / adapter / 实例）。"""
    console.print_json(data=client.request("GET", "/status"))


@app.command()
def discover() -> None:
    """发现集群里疑似的组件（只读，发现→待确认）。"""
    cands = client.request("POST", "/discover")["candidates"]
    table = Table(title="发现的候选（pending · 待确认才接管）")
    for col in ("组件", "名称", "平台", "归属", "装法", "说明"):
        table.add_column(col)
    for c in cands:
        own = c["ownership"] + ("（排除·永不接管）" if c["excluded"] else "")
        style = "red" if c["excluded"] else None
        table.add_row(c["kind"], c["name"], c["platform"], own,
                      c.get("install_method") or "-", c["reason"], style=style)
    console.print(table)


@app.command()
def install(
    kind: str = typer.Argument(..., help="组件类型，如 etcd"),
    name: str = typer.Option(..., "--name", "-n", help="实例名"),
    method: str | None = typer.Option(None, "--method", help="安装方式 id；默认用 profile 默认"),
    namespace: str = typer.Option("default", "--namespace", "-N"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply", help="默认 dry-run 仅出计划；--apply 真正执行"),
) -> None:
    """安装一个组件（默认 dry-run 预演）。"""
    body = {"kind": kind, "name": name, "method": method, "namespace": namespace, "dry_run": dry_run}
    _print_task(client.request("POST", "/install", json=body))


def _print_task(task: dict) -> None:
    mode = "DRY-RUN · 仅计划" if task["dry_run"] else "执行"
    color = "cyan" if task["dry_run"] else "green"
    console.print(
        f"[bold]Task {task['id']}[/]  {task['type']} → {task['target']}  "
        f"[[{color}]{mode}[/]]  状态={task['status']}"
    )
    table = Table(show_header=True, header_style="bold")
    for col in ("#", "步骤", "状态", "计划 / 输出"):
        table.add_column(col)
    for i, s in enumerate(task["steps"], 1):
        table.add_row(str(i), s["name"], s["status"], s["detail"] or s["plan"])
    console.print(table)
    for a in task.get("audit", []):
        console.print(f"  · {a}", style="dim")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
