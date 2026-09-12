# HTTP API、CLI 与 Console 指南

TGS-RL Northbound Gateway 将 Job、Run、Operation、Replay、Experiment 以及运行时观察
数据暴露为 JSON HTTP API。CLI 和 Python `GatewayClient` 都调用同一组 HTTP 路由；
Console 同时提供观察页面以及常用 Job/Run、Replay 写操作入口。

Gateway 默认连接 gRPC 服务，不会自行启动 Scheduler、Job Controller 或 Runtime，
也不会在 gRPC 模式下生成演示数据。调用前请先启动所需后端。

## 1. 安装并确认 CLI

先安装锁定的 Python 依赖：

```bash
uv sync --frozen
```

项目安装两个等价的命令入口。本文统一使用较短的 `tgsrl`：

```bash
uv run --frozen tgsrl --help
uv run --frozen tgsrl-gateway --help
```

两条命令都进入同一个 Gateway CLI。若已经激活项目虚拟环境，也可以直接运行
`tgsrl` 或 `tgsrl-gateway`。所有客户端子命令的默认地址都是
`http://127.0.0.1:8080`；连接其他地址时，在子命令后添加
`--base-url URL`。

## 2. 启动 gRPC 后端与 Gateway

Gateway 的默认 `grpc` 模式依赖四个服务角色：

| 角色 | 本地目标 | 用途 |
|---|---:|---|
| Job Control | `127.0.0.1:50061` | Job、Run、Operation 与 Timeline |
| Scheduler | `127.0.0.1:50051` | 调度决策 |
| Runtime | `127.0.0.1:50071` | Runtime、Topology 与 Sandbox |
| Experiment | `127.0.0.1:50071` | Replay 与 Experiment |

Runtime 进程同时注册 Runtime 和 Experiment gRPC 服务，所以两个目标默认都
使用 `127.0.0.1:50071`。Operator 的 lifecycle control gRPC 使用 `127.0.0.1:50081`，
它不是 Experiment target。

完整 backend 需要五个终端，按下列顺序运行：

```bash
make run-scheduler
```

```bash
make run-runtime
```

```bash
make run-controller
```

```bash
make run-operator
```

```bash
make run-gateway
```

`make run-gateway` 等价于使用以下关键参数启动已安装的 CLI：

```bash
uv run --frozen tgsrl serve \
  --host 127.0.0.1 \
  --port 8080 \
  --backend-mode grpc \
  --job-control-target 127.0.0.1:50061 \
  --scheduler-target 127.0.0.1:50051 \
  --runtime-target 127.0.0.1:50071 \
  --experiment-target 127.0.0.1:50071
```

也可以通过 `TGSRL_GATEWAY_BACKEND_MODE`、
`TGSRL_GATEWAY_JOB_CONTROL_TARGET`、`TGSRL_GATEWAY_SCHEDULER_TARGET`、
`TGSRL_GATEWAY_RUNTIME_TARGET` 和 `TGSRL_GATEWAY_EXPERIMENT_TARGET` 设置同样的
参数；显式 CLI 参数优先于环境变量。

检查 Gateway 及依赖：

```bash
uv run --frozen tgsrl health
uv run --frozen tgsrl capabilities
```

Gateway 优先调用标准 gRPC Health RPC；后端未注册该服务时，会调用一个轻量 unary RPC。
`dependencies` 列出 Job Control、Scheduler、Runtime 和 Experiment，但不包含 Operator。
Runtime 的空资源探测可能以 `grpc_probe:unknown` 返回，因此即使进程可达，汇总
`status` 也可能是 `degraded`。健康接口只用于依赖诊断，不包含 Operator，也不确认
业务写入、持久化或完整任务执行。需要用实际资源查询和 Job 操作确认所需链路。

### 无持久化演示模式

如果只想浏览内置演示数据，可以显式启动进程内后端：

```bash
uv run --frozen tgsrl serve --backend-mode memory
```

该模式不连接上述 gRPC 服务，数据随 Gateway 进程结束而消失。它只适合浏览接口与
界面，不能代表 gRPC 后端、资源 Provider 或外部运行环境的行为。

## 3. 使用 CLI

### 查询健康、能力和资源

```bash
uv run --frozen tgsrl health
uv run --frozen tgsrl capabilities
uv run --frozen tgsrl resources
uv run --frozen tgsrl list-jobs --limit 20
uv run --frozen tgsrl list-jobs --limit 20 --data-kind DATA_KIND_LIVE
uv run --frozen tgsrl list-operations --limit 20
uv run --frozen tgsrl list-operations --job-id JOB_ID --run-id RUN_ID \
  --type OPERATION_TYPE_START --state OPERATION_STATE_SUCCEEDED
uv run --frozen tgsrl list-replays --limit 20
uv run --frozen tgsrl list-experiments --limit 20
```

CLI 将响应格式化为 JSON。列表响应中的 `next_page_token` 非空时，可原样传回：

```bash
uv run --frozen tgsrl list-jobs \
  --limit 20 \
  --page-token '<上一页的 next_page_token>'
```

`resources` 调用 `GET /v1/resources`，返回 Scheduler 当前 `ClusterSnapshot`，包括设备、
allocation、pending unit、revision 和观察时间。它是调度资源账本的只读投影；DRA/HAMi
最终是否成功兑现仍由 Operator 的 allocation readback 与 managed-worker observation 证明。
内存后端返回的 A10/A100/MIG 数据只用于界面预览，不能作为硬件证据。

### 校验、创建并运行 Job

Job 请求体是 `tgsrl.v1.RLTrainingJob` 的 Protobuf JSON 表示。不同 Runtime、
执行契约和资源需求需要不同字段，本文不提供一个看似完整但无法在你的后端运行的
缩略载荷。请准备完整的 `job.json`，先校验，再创建：

```bash
uv run --frozen tgsrl validate-job \
  --job ./job.json \
  --idempotency-key validate-job-example-1 \
  --request-id request-validate-example-1

uv run --frozen tgsrl create-job \
  --job ./job.json \
  --idempotency-key create-job-example-1 \
  --request-id request-create-example-1
```

从创建响应的 `job.jobId` 取得 Job ID，随后执行生命周期操作：

```bash
JOB_ID='<job.jobId>'

uv run --frozen tgsrl get-job "$JOB_ID"
uv run --frozen tgsrl admit-job "$JOB_ID" \
  --reason 'approved for local run' \
  --idempotency-key admit-job-example-1
uv run --frozen tgsrl list-runs "$JOB_ID" --limit 20
```

`admit-job` 会为尚无 Run 的 Job 创建并准备一个 Run；如果已经存在最新的非终态 Run，
则准备该 Run。从准入响应的 `operation.runId` 或 `list-runs` 响应中取得 Run ID，再启动
和观察它：

```bash
RUN_ID='<run.runId>'

uv run --frozen tgsrl job-command "$JOB_ID" "$RUN_ID" start \
  --actor cli \
  --reason 'start local run' \
  --idempotency-key start-run-example-1
uv run --frozen tgsrl list-runs "$JOB_ID" --limit 20
uv run --frozen tgsrl timeline "$JOB_ID" --run-id "$RUN_ID" --limit 50
uv run --frozen tgsrl traces "$JOB_ID" --run-id "$RUN_ID" --limit 200
uv run --frozen tgsrl topology "$JOB_ID" --run-id "$RUN_ID"
uv run --frozen tgsrl sandboxes "$JOB_ID" --run-id "$RUN_ID"
uv run --frozen tgsrl list-decisions "$JOB_ID" --run-id "$RUN_ID" --limit 20
```

如需在准入前显式创建一个执行规格不可变、状态可演进的 Run generation，可以先执行
`tgsrl create-run "$JOB_ID" --idempotency-key KEY`，再调用 `admit-job`；后者会准备
这个最新的非终态 Run。

Job command 支持 `start`、`pause`、`resume`、`stop`、`retry` 和 `terminate`。
例如：

```bash
uv run --frozen tgsrl job-command "$JOB_ID" "$RUN_ID" pause \
  --idempotency-key pause-run-example-1
uv run --frozen tgsrl job-command "$JOB_ID" "$RUN_ID" resume \
  --idempotency-key resume-run-example-1
uv run --frozen tgsrl job-command "$JOB_ID" "$RUN_ID" stop \
  --idempotency-key stop-run-example-1
```

Job 状态转换和 Runtime 动作由 gRPC 后端决定。lifecycle command 成功返回时，Operation
通常仍处于 running，表示 desired-state dispatch 已被接受；Runtime 会在 Operator 观察到
所有目标收敛后回报 Job Controller，后者才完成 Operation 和 Run 状态。需要通过 Run、
Operation、Timeline、Sandbox 和 Decision 接口确认最终结果。
`create-run JOB_ID` 可显式创建新的 Run 记录，但新记录初始为 validating；需要再次
执行 `admit-job` 完成 Runtime 校验、编译和准备后才能 `start`。

### JSON 文件和行内 JSON

`--job`、`--replay` 和 `--experiment` 使用相同的参数规则：

1. 如果参数值是一个已存在的路径，CLI 以 UTF-8 读取该文件并解析 JSON；
2. 否则，CLI 将参数值本身解析为行内 JSON。

因此可以传 `--job ./job.json`，也可以将完整 JSON 对象作为一个正确引用的 shell
参数传给 `--job`。这里没有 `@file` 语法，也不支持用 `-` 从标准输入读取请求体。
若一个行内字符串恰好与现有路径同名，文件输入优先。

请求顶层必须是 JSON object。字段名遵循 Protobuf JSON 的 lowerCamelCase 形式，枚举
使用符号名称；未知字段会被拒绝。资源对象输出同样使用 Protobuf JSON 字段名，但
Gateway 自身的响应包裹字段（例如 `next_page_token` 和 `runtime_units`）保留
snake_case，调用方不应假定所有键采用同一种命名风格。

## 4. 使用 Python `GatewayClient`

SDK 随 Python 包一起安装，不需要额外 HTTP 客户端依赖：

```python
import json
from pathlib import Path
from urllib.error import HTTPError

from tgsrl_gateway import GatewayClient

client = GatewayClient("http://127.0.0.1:8080", timeout=5.0)
job = json.loads(Path("job.json").read_text(encoding="utf-8"))

try:
    print(client.health())
    print(client.capabilities())
    print(client.get_resources())
    print(client.list_jobs(limit=20))

    validation = client.validate_job(
        job,
        idempotency_key="sdk-validate-job-1",
        request_id="sdk-request-validate-1",
    )
    if not validation["valid"]:
        raise RuntimeError(validation["diagnostics"])

    created = client.create_job(
        job,
        idempotency_key="sdk-create-job-1",
        request_id="sdk-request-create-1",
    )
    job_id = str(created["job"]["jobId"])

    admitted = client.admit_job(job_id, idempotency_key="sdk-admit-job-1")
    run_id = str(admitted["operation"]["runId"])
    client.apply_job_command(
        job_id,
        run_id,
        "start",
        actor="sdk",
        idempotency_key="sdk-start-run-1",
    )
    print(client.get_topology(job_id, run_id=run_id))
    print(client.list_traces(job_id, run_id=run_id, limit=200))
    print(client.list_decisions(job_id, run_id=run_id, limit=20))
except HTTPError as error:
    print(error.code, error.read().decode("utf-8"))
    raise
```

`GatewayClient` 返回解码后的 JSON dictionary。HTTP 非成功响应不会转换成
`GatewayError`，而是由标准库 `urllib` 抛出 `HTTPError`。SDK 还提供
`openapi()`、Run、Timeline、Trace、DAG、Sandbox、Operation、Replay 和 Experiment 对应方法。

## 5. HTTP 路由

所有响应都是 JSON。可用路由按用途分组如下。

### 系统

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | Gateway 依赖的 gRPC 就绪状态 |
| `GET` | `/openapi.json` | 读取运行中 Gateway 的 OpenAPI 文档 |
| `GET` | `/v1/capabilities` | 协议版本、命令、数据类型和契约特性 |
| `GET` | `/v1/resources` | 读取 Scheduler 当前设备、allocation 与 pending unit 快照 |

### Job 与 Run 生命周期

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET`, `POST` | `/v1/jobs` | 列出或创建 Job |
| `POST` | `/v1/jobs/validate` | 校验并规范化 Job |
| `GET` | `/v1/jobs/{job_id}` | 读取 Job |
| `POST` | `/v1/jobs/{job_id}/admit` | 准入 Job |
| `GET`, `POST` | `/v1/jobs/{job_id}/runs` | 列出或创建 Run |
| `GET` | `/v1/jobs/{job_id}/runs/{run_id}` | 读取 Run |
| `POST` | `/v1/jobs/{job_id}/runs/{run_id}/commands/{command}` | 执行 Run 命令 |

### 运行时与决策观察

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/v1/jobs/{job_id}/timeline` | 读取 Job 事件 |
| `GET` | `/v1/jobs/{job_id}/traces` | 分页读取 Runtime 持久化的 TraceEvent，可按 Run、trace ID 和 data kind 过滤 |
| `GET` | `/v1/jobs/{job_id}/dag` | 读取阶段图和依赖 |
| `GET` | `/v1/jobs/{job_id}/topology` | 读取 Run、Manifest、Runtime 与最近决策 |
| `GET` | `/v1/jobs/{job_id}/sandboxes` | 列出 Sandbox |
| `GET` | `/v1/jobs/{job_id}/decisions` | 列出调度决策 |
| `GET` | `/v1/jobs/{job_id}/decisions/{decision_id}` | 读取单条决策 |

### Operation、Replay 与 Experiment

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/v1/operations` | 列出 Operation |
| `GET` | `/v1/operations/{operation_id}` | 读取 Operation |
| `GET`, `POST` | `/v1/replays` | 列出或创建 Replay |
| `GET` | `/v1/replays/{replay_id}` | 读取 Replay |
| `POST` | `/v1/replays/{replay_id}/commands/{command}` | 执行 Replay 命令 |
| `GET`, `POST` | `/v1/experiments` | 列出或创建 Experiment |
| `GET` | `/v1/experiments/{experiment_id}` | 读取 Experiment |

Replay command 支持 `start`、`pause`、`resume`、`stop` 和 `terminate`。
Experiment 提供创建、列表和详情接口。创建 Job、Run、Replay 和 Experiment
返回 HTTP `201`；其他成功请求返回 `200`。
`GET /v1/operations` 还可用 `job_id`、`run_id`、`type` 和 `state` 过滤；CLI 对应
`--job-id`、`--run-id`、`--type` 和 `--state`。

## 6. Header、错误与分页

### Header

发送 JSON body 时使用：

```http
Accept: application/json
Content-Type: application/json
Idempotency-Key: caller-stable-key
X-Request-Id: caller-request-id
```

`Idempotency-Key` 和 `X-Request-Id` 是可选的。对 Job/Run 创建、准入和 lifecycle command
等可能重试的写请求，调用方应生成作用域明确且稳定的幂等键；不要为不同意图复用
同一个键。
`X-Request-Id` 用于关联请求和错误。未提供时，错误响应中的值为
`request-anonymous`。重试同一写操作时，应保持请求内容和幂等键不变；后端可以拒绝
同一个幂等键对应的不同请求内容。Replay/Experiment 创建接收这些 Header，但其
服务端创建逻辑不提供同等级别的幂等保证，不应依赖 Header 对它们去重。
Gateway 的 JSON 请求体上限为 1 MiB；负数、非数字或超出上限的 `Content-Length` 会被拒绝。

### 错误

错误使用稳定包裹结构：

```json
{
  "error": {
    "code": "not_found",
    "message": "job not found",
    "details": {
      "resource": "job",
      "resource_id": "job-missing"
    },
    "request_id": "request-anonymous"
  }
}
```

常见映射：

| HTTP 状态 | `error.code` | 含义 |
|---:|---|---|
| `400` | `bad_request`、`backend_invalid_argument` | JSON、查询参数、命令或后端前置条件无效 |
| `401` | `backend_unauthenticated` | gRPC 后端要求身份认证 |
| `403` | `backend_forbidden` | gRPC 后端拒绝当前身份访问 |
| `404` | `not_found` | Job、Run、Decision 等资源不存在 |
| `409` | `conflict` | 资源已存在或写入冲突 |
| `429` | `backend_resource_exhausted` | 后端容量、配额或流控已耗尽 |
| `501` | `backend_feature_unavailable` | 连接的 gRPC 契约缺少所需 unary RPC |
| `503` | `backend_unavailable` | gRPC 目标不可用或调用失败 |
| `504` | `backend_timeout` | 后端 RPC 超时 |
| `500` | `internal_error` | Gateway 未预期错误 |

未知路由返回 `404 not_found`；已知路由上的不匹配方法返回
`405 method_not_allowed`，并通过 `Allow` Header 给出可用方法。

### 分页

Job、Run、Trace、Sandbox、Decision、Operation、Replay 和 Experiment 列表接受正整数 `limit` 和
不透明的 `page_token`。Gateway 未显式收到 `limit` 时通常向后端请求 50 条。响应包含
资源数组和 `next_page_token`；空字符串表示没有下一页。Token 的编码属于实现细节，
调用方应原样传回，不要解析或自行构造。

Job、Run、Replay 和 Experiment 列表还支持对应的 `after_*_id`；它与 `page_token` 互斥。
Timeline 支持 `run_id`、`limit`、`after_event_id` 和 `page_token`，其中后两者是两种互斥的
续读方式：可以原样传回 `next_page_token`，也可以把上一批最后一条事件的 `eventId`
作为 `after_event_id`。
Trace 支持 `run_id`、`trace_id`、`data_kind`、`limit` 与 `page_token`；page token 绑定完整
filter scope，改变筛选条件后必须从第一页重新查询。
CLI 参数分别是 `--run-id`、`--trace-id`、`--data-kind`、`--limit` 和 `--page-token`。
Job、Run、Replay、Experiment 的游标参数依次为 `--after-job-id`、`--after-run-id`、
`--after-replay-id`、`--after-experiment-id`；Timeline 使用 `--after-event-id`。

## 7. OpenAPI

运行中的 Gateway 在以下地址提供 OpenAPI 3.1 文档：

```text
http://127.0.0.1:8080/openapi.json
```

也可以在不连接 Gateway 的情况下生成文档：

```bash
uv run --frozen tgsrl openapi
uv run --frozen tgsrl openapi --output ./openapi.json
```

项目同时提供 [OpenAPI 工件](../../api/openapi.json)。生成命令读取已安装的路由定义，不会从
运行中的服务下载文档；Python SDK 的 `client.openapi()` 才会请求
`/openapi.json`。

OpenAPI 工件列出路由、公共 Header 和错误包裹，但请求与成功响应 schema 多为
通用 JSON object。它不是完整的 `RLTrainingJob` 字段校验器；Job 的实际 wire contract
以 `tgsrl.v1` Protobuf 和后端校验结果为准。

## 8. 启动 Console

Console 使用 Vite。安装前端锁定依赖后，从项目根目录启动：

```bash
cd console
npm ci
cd ..
make run-console
```

Vite 服务默认在 `http://localhost:4173` 打开。`make run-console` 使用 HTTP adapter；
Vite 将 `/v1`、`/health` 和 `/openapi.json` 代理到
`http://127.0.0.1:8080`，所以应先启动 Gateway。若 `4173` 已被占用，Vite 可以
选择其他端口，请以终端打印的 URL 为准。

Console 提供三种本地连接方式：

| 配置 | 行为 |
|---|---|
| `VITE_TGSRL_API_ADAPTER=http` | 使用 HTTP API；这是缺省值 |
| `VITE_TGSRL_API_ADAPTER=mock` | 使用浏览器内置静态数据，不需要 Gateway |
| `VITE_TGSRL_API_BASE=...` | 覆盖 HTTP adapter 的 API base URL；缺省为空并使用同源 Vite proxy |

直接运行前端静态 Mock：

```bash
cd console
VITE_TGSRL_API_ADAPTER=mock npm run dev
```

浏览器静态 Mock 与 Gateway 的 `--backend-mode memory` 是两套不同机制：前者完全不
发送 HTTP 请求，后者仍通过 HTTP 访问 Gateway 的进程内数据。要使用 HTTP 链路但不
启动 gRPC 服务，可以组合使用：

```bash
uv run --frozen tgsrl serve --backend-mode memory
```

```bash
make run-console
```

任务中心的**高级操作**区域可提交原始 Job JSON；**新建运行**调用
`POST /v1/jobs/{job_id}/runs`，只创建处于 validating 的 Run；其执行规格保持不变，
但 lifecycle state、component status 和 operations 会继续演进。首屏**运行控制**中的**准入**
调用 `POST /v1/jobs/{job_id}/admit` 完成准入与 Runtime 准备；独立的**启动**按钮向所选 Run
发送 `start` command。该区域也提供暂停、恢复、停止、重试、终止，以及 Replay 创建和控制。
新建 Job/Run 后应先准入，再选择该 Run 并启动。模拟状态筛选只在浏览器 Mock 模式出现；
HTTP 模式下数据筛选会传给 Gateway。写操作成功后，重新进入当前视图或刷新浏览器读取最新
观察态。

Vite proxy 只在 `npm run dev` 中生效。`npm run preview` 和部署后的静态文件不能假定仍有
这组代理；应由同源 Web 服务器转发 API。`VITE_*` 值会在 Vite 构建时写入前端包。
Gateway 不返回 CORS Header，因此从 `4173` 直接跨源访问 `8080` 可能被浏览器
阻止；单机运行时应保留空的 `VITE_TGSRL_API_BASE` 并使用 Vite proxy。

## 9. 部署要求与限制

- Gateway 默认仅绑定 `127.0.0.1`，并使用 Python 内置 WSGI server。该 server 不提供
  TLS、进程管理或面向公网的服务加固。
- Gateway 没有认证、授权、租户隔离、CORS 策略或限流；到后端的 gRPC channel 也是
  plaintext。不要将它直接绑定公网地址。
- 如需在受控网络中共享，应在外部提供身份认证、TLS、访问控制、限流、审计、CORS
  和适合部署环境的 HTTP server，并继续限制 gRPC 目标的网络可达范围。
- `memory` Gateway 和浏览器 Mock 只适合无持久化演示；它们的状态、数据和行为不能
  代表真实集群可用性、GPU 能力或性能。
- `/health` 是 gRPC 服务级就绪探测，不是业务写入、持久化或端到端运行成功的证明。
