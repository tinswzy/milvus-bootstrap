# milvus-bootstrap

Milvus 安装/运维工具 —— **瘦 CLI 客户端 + core daemon**。所有真实逻辑都在 core daemon
里（六引擎 / ServiceDriver / PlatformAdapter / StateStore / Task 引擎）；CLI 只负责
管理 core（启停/更新）并把命令转发给它。

设计文档见上级目录 `prototype/design-*.html` 与 `prototype/phase1-plan.html`。

## 当前状态：阶段一垂直切片

打通整条链路（**无需 k8s 集群**，用 `FakeAdapter` + dry-run）：

```
CLI(mb) → core daemon(UDS) → DiscoveryEngine/Provisioner
        → ServiceDriver(etcd, 按 kind 分发) → PlatformAdapter(fake/k8s)
        → StateStore(file)
```

- `ServiceDriver` 接口 + `BaseServiceDriver`（吃 profile）+ `EtcdDriver`（override 特殊语义）
- `PlatformAdapter` 接口 + `FakeAdapter`（可跑）+ `K8sAdapter`（stub，待接 helm/kubectl）
- profile 驱动（`profiles/etcd.yaml`）：检测签名 / 安装方式 / state-class / 健康 / 扩缩 / connect
- Task 引擎：Step 四件套（precheck/do/verify/compensate）+ dry-run + 回滚
- 硬护栏：发现时排除控制面 etcd（`tier=control-plane`），永不接管

## 目录

```
src/milvus_bootstrap/
  cli/        瘦客户端（mb）
  client/     daemon 传输 + 生命周期（UDS）
  server/     core daemon（FastAPI over UDS）
  core/
    models.py / profile.py / registry.py / context.py
    drivers/   ServiceDriver（base + etcd）   ← L3 按组件多态
    platform/  PlatformAdapter（base/fake/k8s）← L4 按平台多态
    state/     StateStore（base/file）
    tasks/     Task 引擎
    engines/   discovery / provisioner
  profiles/   服务知识库（etcd.yaml）
```

## 跑起来（uv）

```bash
cd milvus-bootstrap
uv sync --extra dev          # 建 venv + 装依赖

uv run mb core start         # 启动 core daemon
uv run mb status             # core 运行时状态
uv run mb discover           # 发现候选（含被排除的控制面 etcd）
uv run mb install etcd -n etcd-dev          # dry-run：只出计划
uv run mb install etcd -n etcd-dev --apply  # 执行（FakeAdapter）
uv run mb core stop

uv run pytest -q             # 跑垂直切片测试
```

`MB_ADAPTER=k8s` 切到真集群适配器（目前是 stub）；`MB_HOME` 改状态/socket 落点。

## 下一步

- K8sAdapter 接 helm（子进程）+ 动态客户端（CR）；真集群验证 etcd 安装
- 逐组件接入：minio(MinIO Operator) → woodpecker(Woodpecker Operator) → milvus(external) → pulsar/kafka
- Lifecycle（升级/扩缩/配置/删）、Ownership 接管、后台 poller
