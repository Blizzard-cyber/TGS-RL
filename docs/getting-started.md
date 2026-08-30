# 快速上手

本指南在一台主机上启动 TGS-RL 的六个组件，并完成健康检查与 Job 生命周期操作。
默认方案使用 CPU Mock Provider 和 fake Operator backend，不需要 GPU、CUDA 或
Kubernetes。该方案运行完整控制链，但不会创建真实基础设施资源或执行真实训练。

## 1. 选择启动方式

### Docker Compose

主机要求：Docker Engine 和 Docker Compose v2。Compose 负责准备组件运行环境，
无需在主机安装 Go、Python、`uv` 或 Node.js。

```bash
docker compose up --build
```

Compose 启动 Scheduler、Runtime/Experiment、Job Controller、Operator、Gateway 和
Console。首次启动需要下载基础镜像和依赖。所有宿主机端口只绑定到 `127.0.0.1`，
四个有状态组件使用 named volumes。服务就绪后打开 <http://127.0.0.1:4173>。

停止并保留数据：

```bash
docker compose down
```

只有在确认不再需要 Job、Run、Replay、Decision 和恢复状态时，才删除 volumes：

```bash
docker compose down --volumes
```

### 从源码运行

从源码运行需要：

| 工具 | 版本或范围 | 用途 |
|---|---|---|
| Go | 1.26.4 | Scheduler、Job Controller、Operator |
| Python | 3.12.14；最低 3.12 | Runtime、Experiment、Gateway、SDK/CLI |
| `uv` | 0.12.7 | 安装锁定的 Python 环境 |
| Node.js | 24.20.0 LTS | Web Console |
| Buf | 1.72.0 | Protobuf lint 与代码生成 |
| Docker | Compose v2；已验证 Engine 29.6.1 / Compose 5.2.0 | 完整六服务本地栈和镜像构建 |
| Helm | 4.2.4 | Operator chart 校验与安装 |
| kubectl / minikube | kubectl 与集群相差不超过一个 minor；minikube 1.38.1 | 本地 Kubernetes 集成验证 |

从项目根目录准备依赖与私有状态目录：

```bash
make doctor
uv sync --frozen
npm --prefix console ci
umask 077
mkdir -p .cache/tgsrl
```

## 2. 手动启动组件

每段命令使用一个终端。推荐按下列顺序启动。

### Scheduler

```bash
go run ./scheduler-go/cmd/scheduler \
  -listen 127.0.0.1:50051 \
  -metrics-listen 127.0.0.1:9090 \
  -state-dir .cache/tgsrl/scheduler-state
```

未指定 `-manifest` 时，Scheduler 使用 `compatibility/manifests/cpu-mock.yaml`。

### Runtime 与 Experiment

```bash
uv run --frozen tgsrl-runtime \
  --bind 127.0.0.1:50071 \
  --scheduler-target 127.0.0.1:50051 \
  --operator-target 127.0.0.1:50081 \
  --job-control-target 127.0.0.1:50061 \
  --state-db .cache/tgsrl/runtime.db \
  --config-root .
```

一个 Python 进程在 `50071` 同时提供 `RuntimeControlService` 和
`ExperimentService`。显式绑定 `127.0.0.1` 可避免无意暴露未认证的 gRPC 服务。

### Job Controller

```bash
go run ./job-controller-go/cmd/job-controller \
  -listen 127.0.0.1:50061 \
  -runtime-target 127.0.0.1:50071 \
  -state-dir .cache/tgsrl/job-controller
```

### Operator

```bash
go run ./cmd/operator \
  -mode fake \
  -listen 127.0.0.1:50081 \
  -scheduler 127.0.0.1:50051 \
  -control 127.0.0.1:50061 \
  -runtime 127.0.0.1:50071 \
  -cursor-dir .cache/tgsrl/operator
```

`fake` 模式消费 Scheduler 决策、编译和调和 bundle，并向 Runtime 发布 Sandbox 事件，
但 backend 对象只保存在 Operator 进程内。它不会连接 Kubernetes API Server。

如需使用 `-mode kubernetes`，必须提供可访问的集群凭据、Kueue 与所选 GPU/DRA
依赖，并部署其余 TGS-RL 服务。随附 YAML/Helm 只部署 Operator。具体要求见
[Operator 指南](guides/operator.md)。

### Gateway

```bash
uv run --frozen tgsrl-gateway serve \
  --host 127.0.0.1 \
  --port 8080 \
  --backend-mode grpc \
  --job-control-target 127.0.0.1:50061 \
  --scheduler-target 127.0.0.1:50051 \
  --runtime-target 127.0.0.1:50071 \
  --experiment-target 127.0.0.1:50071
```

Runtime 与 Experiment 共用端口 `50071`；不要把 Experiment target 指向 Operator 的
`50081`。

### Console

```bash
make run-console
```

Vite 默认监听 <http://127.0.0.1:4173>，并将 `/health`、`/openapi.json` 和 `/v1`
代理到 `127.0.0.1:8080`。

也可以用以下等价目标启动各组件：

```bash
make run-scheduler
make run-runtime
make run-controller
make run-operator
make run-gateway
make run-console
```

## 3. 检查服务

```bash
uv run --frozen tgsrl health
uv run --frozen tgsrl capabilities
curl -fsS http://127.0.0.1:9090/metrics >/dev/null
```

`health` 报告 Job Control、Scheduler、Runtime 和 Experiment 四个 Gateway 依赖，
不包含 Operator。Runtime 的轻量探测在可达时也可能报告 `grpc_probe:unknown`，从而使
汇总状态为 `degraded`。因此：

- `healthy` 或 `degraded` 用于诊断服务连接，不表示 Job 已能完整执行；
- 还需确认 Operator `127.0.0.1:50081` 正在监听；
- 提交任务后，应通过 Run、Operation、Sandbox 和 Decision 查询确认控制链结果。

Console 提供 Overview、Jobs、Timeline、Topology、Sandboxes、Decisions 和
Experiment Compare 页面。Job Detail 可创建 Job/Run、执行 Admit、创建 Replay，
以及发送 Run/Replay 生命周期命令。

## 4. 提交并启动 Job

`job.json` 必须是 `tgsrl.v1.RLTrainingJob` 的 Proto JSON 表示，包含有效的协议版本、
算法、Rollout 模式、data kind、资源、`desiredUnits`、不可变镜像 digest、Runtime
组件和执行图。建议通过 Adapter 构造 `ExecutionContract`。

先校验并创建 Job：

```bash
uv run --frozen tgsrl validate-job \
  --job ./job.json \
  --idempotency-key validate-job-1 \
  --request-id request-1

uv run --frozen tgsrl create-job \
  --job ./job.json \
  --idempotency-key create-job-1
```

从创建响应的 `job.jobId` 获取 `JOB_ID`，然后准入：

```bash
uv run --frozen tgsrl admit-job JOB_ID \
  --idempotency-key admit-job-1
uv run --frozen tgsrl list-runs JOB_ID
```

`admit-job` 在没有 Run 时创建一个 Run，并完成 Runtime 校验、编译和准备。如果需要
先建立新的不可变 Run generation，可先调用 `create-run JOB_ID`；该 Run 仍需准入。

从准入响应的 `operation.runId` 或 `list-runs` 获取 `RUN_ID`，再启动：

```bash
uv run --frozen tgsrl job-command JOB_ID RUN_ID start \
  --idempotency-key start-run-1
```

`start` 会让 Runtime 发布各阶段 Intent。Scheduler 接收 Intent 并执行 Mock 资源动作，
Operator 将成功决策物化为 fake backend 对象。运行状态在 Operator 观察事件回传后收敛，
因此命令响应不是最终完成状态。查询结果：

```bash
uv run --frozen tgsrl list-runs JOB_ID
uv run --frozen tgsrl list-operations --job-id JOB_ID --run-id RUN_ID
uv run --frozen tgsrl timeline JOB_ID --run-id RUN_ID
uv run --frozen tgsrl topology JOB_ID --run-id RUN_ID
uv run --frozen tgsrl sandboxes JOB_ID --run-id RUN_ID
uv run --frozen tgsrl list-decisions JOB_ID --run-id RUN_ID
```

完整接口见 [API、CLI 与 Console](guides/api-and-console.md)。

## 5. 停止与恢复

手动运行时，用 `Ctrl-C` 停止各进程。为减少新的写入，推荐按以下顺序停止：
Console/Gateway → Operator → Job Controller → Runtime → Scheduler。

保留 `.cache/tgsrl/` 后按启动顺序重启：

1. Scheduler 从相同 `-state-dir` 恢复并调和 reservation；
2. Runtime/Experiment 从相同 `--state-db` 分页恢复数据，并仅补投未确认的 Start Intent；
3. Job Controller 从相同 `-state-dir` 恢复控制面记录；
4. Operator 从相同 `-cursor-dir` 恢复 cursor、delivery、观察注册和 lifecycle ledger；
5. Gateway 与 Console 从后端重新读取状态。

恢复不是跨服务事务。Job Controller 不自动重新执行中间态 Operation，fake Operator
backend 的内存对象在退出后丢失，Runtime Checkpoint 也不是训练进程镜像。恢复后应核对
Operation、Decision、reservation、Sandbox 和实际 backend 对象。详见
[配置、持久化与恢复](guides/configuration-and-recovery.md)。

要清空源码运行产生的状态，请先停止全部组件，再删除你在命令中显式指定的
`.cache/tgsrl/` 目录。该操作不可恢复。

## 常见问题

### Gateway 报告 `degraded`

确认 `50051`、`50061`、`50071` 和 `50081` 都在监听。Gateway 的 Runtime 与
Experiment target 都应为 `50071`；Runtime 的 Operator target 应为 `50081`。
如果只有 Runtime 探测返回 `grpc_probe:unknown`，继续用实际查询和 Job 操作判断可用性。

### Job 校验失败

运行 `validate-job` 并查看 `diagnostics`。常见原因包括缺少协议版本、资源、
`desiredUnits`、不可变镜像 digest、组件名称或完整执行图，以及 manifest 与配置图冲突。

### 没有调度决策

创建 Job 或 Run 不会启动执行。确认已对目标 Run 执行 `admit-job`，随后发送
`job-command ... start`，并确认 Runtime 的 `--scheduler-target` 可达。

### 端口被占用

修改监听端口时也要同步修改所有调用方 target。Console 开发代理默认指向 Gateway
`8080`；可通过 `VITE_TGSRL_GATEWAY_TARGET` 修改代理目标。
