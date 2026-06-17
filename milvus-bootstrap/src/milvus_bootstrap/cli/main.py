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
config_app = typer.Typer(help="配置管理：get / set / restart", no_args_is_help=True)
app.add_typer(config_app, name="config")

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
    set_: list[str] = typer.Option(None, "--set", help="覆盖安装参数 key=val（可重复）"),
    chart: str | None = typer.Option(None, "--chart", help="覆盖 chart 源（如本地 .tgz 路径）"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply", help="默认 dry-run 仅出计划；--apply 真正执行"),
) -> None:
    """安装一个组件（默认 dry-run 预演）。"""
    params: dict[str, str] = {}
    for item in set_ or []:
        if "=" in item:
            k, v = item.split("=", 1)
            params[k] = v
    body = {"kind": kind, "name": name, "method": method, "namespace": namespace,
            "params": params, "chart_override": chart, "dry_run": dry_run}
    _print_task(client.request("POST", "/install", json=body, timeout=600))


@app.command()
def adopt(
    kind: str = typer.Argument(..., help="组件类型，如 minio"),
    name: str = typer.Option(..., "--name", "-n", help="要接管的实例名"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply"),
) -> None:
    """接管一个已存在的（Adoptable）实例为 Managed。"""
    _print_task(client.request("POST", "/adopt", json={"kind": kind, "name": name, "dry_run": dry_run}, timeout=600))


@app.command()
def scale(
    instance: str = typer.Argument(..., help="实例名"),
    replicas: int = typer.Argument(..., help="目标副本数"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply"),
) -> None:
    """扩缩容（按组件护栏）。"""
    _print_task(client.request("POST", "/scale", json={"instance": instance, "replicas": replicas, "dry_run": dry_run}, timeout=600))


@app.command()
def upgrade(
    instance: str = typer.Argument(..., help="实例名"),
    image: str = typer.Option(..., "--image", help="目标镜像"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply"),
) -> None:
    """升级镜像（重新渲染并应用；权威态先备份）。"""
    _print_task(client.request("POST", "/upgrade", json={"instance": instance, "image": image, "dry_run": dry_run}, timeout=600))


@app.command()
def delete(
    instance: str = typer.Argument(..., help="实例名"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply"),
) -> None:
    """删除实例（state-class 护栏；PVC 默认保留）。"""
    _print_task(client.request("POST", "/delete", json={"instance": instance, "dry_run": dry_run}, timeout=600))


@app.command("mq-options")
def mq_options(
    milvus_version: str = typer.Option(..., "--milvus-version", "-v",
                                       help="milvus 版本或镜像，如 v2.6.3 或 milvusdb/milvus:v3.0.0"),
    mode: str = typer.Option("standalone", "--mode", help="standalone / cluster"),
) -> None:
    """查看某 milvus 版本可选 / 不可选的 MQ（版本兼容矩阵）。"""
    data = client.request("POST", "/mq-options",
                          json={"milvus_version": milvus_version, "mode": mode})
    table = Table(title=f"milvus {milvus_version} ({mode}) 的 MQ 选项")
    for col in ("MQ 选项", "WAL", "依赖组件", "可选", "说明"):
        table.add_column(col)
    for o in data["options"]:
        sel = "[green]✓ 可选[/]" if o["supported"] else "[red]✗ 不可选[/]"
        table.add_row(o["label"], o["wal"], o.get("dep_kind") or "嵌入", sel,
                      o["reason"] or o["note"], style=None if o["supported"] else "dim")
    console.print(table)


@app.command("switch-mq")
def switch_mq(
    instance: str = typer.Argument(..., help="milvus 实例名"),
    to: str = typer.Option(..., "--to", help="目标 MQ/WAL：woodpecker / kafka / pulsar"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply"),
) -> None:
    """切换 Milvus 的 MQ/WAL（管理 API wal/alter）。"""
    _print_task(client.request("POST", "/switch-mq",
                               json={"instance": instance, "target_wal": to, "dry_run": dry_run}, timeout=600))


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


@config_app.command("get")
def config_get(instance: str = typer.Argument(..., help="实例名")) -> None:
    """查看实例的有效配置。"""
    data = client.request("POST", "/config/get", json={"instance": instance})
    console.print_json(data=data["config"])


@config_app.command("set")
def config_set(
    instance: str = typer.Argument(..., help="实例名"),
    kv: list[str] = typer.Option(None, "--set", help="配置项 key=val（可重复）"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply"),
) -> None:
    """改配置（milvus→CR spec.conf；其它→helm values），随后自动滚动。"""
    overrides: dict[str, str] = {}
    for item in kv or []:
        if "=" in item:
            k, v = item.split("=", 1)
            overrides[k] = v
    _print_task(client.request("POST", "/config/set",
                               json={"instance": instance, "kv": overrides, "dry_run": dry_run}, timeout=600))


@config_app.command("restart")
def config_restart(
    instance: str = typer.Argument(..., help="实例名"),
    dry_run: bool = typer.Option(True, "--dry-run/--apply"),
) -> None:
    """滚动重启实例的工作负载。"""
    _print_task(client.request("POST", "/config/restart",
                               json={"instance": instance, "dry_run": dry_run}, timeout=600))


def main() -> None:
    app()


if __name__ == "__main__":
    main()
