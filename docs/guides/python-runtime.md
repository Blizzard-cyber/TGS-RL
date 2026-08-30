# Python Runtime、Trace 与实验指南

Python Runtime 同时提供可嵌入的 Python API 和 `RuntimeControlService`、
`ExperimentService` gRPC 服务。它负责把算法与 Rollout 语义编译为运行时单元，
维护 Runtime、Sandbox、Trace、Replay 和 Experiment 状态，并可向 Go Scheduler
发布设备无关的 `SchedulingIntent`。Runtime 的产品契约不限定硬件；默认单机方案使用
CPU Mock。连接真实训练框架或 GPU 时，用户需要提供对应依赖、执行后端、provider hook
和资源控制。

开始前安装锁定依赖：

```bash
uv sync --frozen
```

## 构建执行契约

内置语义适配器包括：

- 算法：`PPOAdapter`、`GRPOAdapter`；
- Rollout：`SyncRolloutAdapter`、`PartialAsyncRolloutAdapter`、
  `FullAsyncRolloutAdapter`。

```python
from adapters import GRPOAdapter, PartialAsyncRolloutAdapter, build_execution_contract

contract = build_execution_contract(
    GRPOAdapter(),
    PartialAsyncRolloutAdapter(),
)

print(contract.contract_id)
print([phase.phase_id for phase in contract.phase_graph.phases])
```

`contract_id` 是完整契约经确定性序列化后的 SHA-256 标识。修改 phase、规则、
版本约束或 policy 后，应重新生成该 ID。

## Runtime 服务

`tgsrl-runtime` 启动 Runtime 与 Experiment 两个 gRPC service：

```bash
uv run --frozen tgsrl-runtime \
  --bind 127.0.0.1:50071 \
  --scheduler-target 127.0.0.1:50051 \
  --operator-target 127.0.0.1:50081 \
  --job-control-target 127.0.0.1:50061 \
  --state-db .cache/tgsrl/runtime.db \
  --config-root . \
  --manifest compatibility/manifests/cpu-mock.yaml
```

也可以使用项目目标启动默认单机服务：

```bash
make run-runtime
```

主要参数：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--bind` | `[::]:50071` | Runtime/Experiment gRPC 监听地址 |
| `--scheduler-target` | `127.0.0.1:50051` | Scheduler gRPC 目标 |
| `--operator-target` | `127.0.0.1:50081` | Operator lifecycle control gRPC 目标 |
| `--job-control-target` | `127.0.0.1:50061` | Runtime 观察态回报目标 |
| `--state-db` | `.cache/tgsrl/runtime.db` | SQLite 文件；显式 `:memory:` 使用临时内存库 |
| `--config-root` | `.` | 配置图根目录 |
| `--manifest` | 空 | Manifest 覆盖；空值使用 CPU Mock manifest |

正常的 `tgsrl-runtime` 服务入口总会加载配置图。默认 manifest 是
`compatibility/manifests/cpu-mock.yaml`，并引用 BOM、profile、capabilities、policy、
representative scenario、patch ledger 与 compatibility matrix。路径也可通过
`TGSRL_CONFIG_MANIFEST_PATH`、
`TGSRL_CONFIG_BOM_PATH`、`TGSRL_CONFIG_PROFILE_PATH`、
`TGSRL_CONFIG_CAPABILITIES_PATH`、`TGSRL_CONFIG_POLICY_PATH` 和
`TGSRL_CONFIG_SCENARIO_PATH` 覆盖。

加载后，Runtime 会将配置投影应用到 manifest 校验、编译和准备阶段；省略的组件、
profile、data kind、rollout mode、policy、desired units 与 capability 会按配置补全，
显式值与配置冲突时则拒绝请求。

gRPC server 使用 insecure transport，未提供 TLS、认证或授权；应只监听可信
本地或隔离网络。

## Runtime Adapter 边界

Runtime 根据 `RuntimeManifest` 选择四类 adapter：framework、execution backend、
trainer 和 rollout engine。`fake`（以及其 `mock` 别名）用于本地可运行路径。

下列名称已注册为依赖门控的边界 adapter：

| 类型 | Adapter | Python 依赖 |
|---|---|---|
| Framework | veRL、OpenRLHF | veRL 使用仓库内 bridge；真实 worker 环境需 `verl`。OpenRLHF 需 `openrlhf` |
| Execution backend | Ray | `ray` |
| Trainer | PyTorch | `torch` |
| Rollout engine | vLLM、SGLang | `vllm`、`sglang` |

这些 adapter 会校验 manifest、报告依赖可用性，并把 typed lifecycle 转换为结构化
`LaunchSpec`。Adapter 层定义 direct command、Python module hook 和 API hook 等 bridge
契约；依赖或目标缺失时显式返回 unavailable，而不是假成功。产品服务中的 `start` 只把
unit 推进到 requested/starting、持久化 generation/幂等信息并发布调度 Intent；它不会在
资源分配前从 Runtime 进程直接启动 manifest command。pause/resume/stop/terminate 由 Runtime
转发到 Operator 的 generation-fenced backend control，最终 unit/sandbox 状态只由观察到的
`SandboxEvent` 推进。`checkpoint` 记录完成元数据和 digest，不是外部训练进程镜像。
`ValidateRuntime` 会把任一所选 adapter 的 `UNAVAILABLE` 或 `UNSUPPORTED` 结果判为无效，
且不会持久化该 manifest；`CompileRuntime` 会重复执行同一结构化能力检查，不能绕过准入。

仅安装对应 Python 模块不足以启动训练。使用这些 Adapter 时必须同时提供可用的执行
bridge、分布式环境、镜像/命令和 GPU 资源控制；veRL 使用仓库内 bridge，但 worker 必须提供
control socket 和实际训练 callback。缺少依赖或执行条件时请求明确失败。详细要求见
[支持范围与限制](../reference/current-capabilities.md)。

veRL 的第一方 bridge 位于 `adapters.frameworks.verl_bridge`。训练 worker 嵌入
`VerlWorkerBridge` 并提供 checkpoint/offload/reload/stop/resume callback 后，可通过 Unix
socket 接收 generation-fenced、幂等的生命周期请求；bridge 同时写出 policy version、buffer
level、policy lag、staleness、ESS、sample coverage、safe-point 和 action latency 的原始事件。
`TGSRL_VERL_CONTROL_SOCKET` 指定 worker socket；Runtime 会向 bridge 传递实际 generation 和
调用幂等键。worker state/receipt 默认持久化在 trace 文件旁，重启后不会重复执行已确认动作，
未确认动作会保持 fail-closed。可通过 `trace_sink` 将 typed `TraceEvent` 直接接入 Runtime
ingestion。
`scripts/verl-reference-workload.py` 使用同一 bridge 执行无 GPU 的进程级 conformance workload；
它只证明协议和执行链，不代表真实 veRL 训练或 GPU 性能。

## 生成与规范化 Trace

```python
from tgsrl_runtime import SyntheticScenario, SyntheticWorkload, TraceNormalizer

workload = SyntheticWorkload(seed=7, algorithm="grpo")
events = workload.generate(SyntheticScenario.TOOL_WAIT)
batch = workload.generate_batch(SyntheticScenario.TOOL_WAIT)
normalized = TraceNormalizer().normalize(events)
```

内置 Synthetic 场景包括：

| 场景 | 用途 |
|---|---|
| `TOOL_WAIT` | 模拟工具或环境等待 |
| `LONG_TAIL` | 模拟慢 rank 造成的长尾等待 |
| `TRAINER_STARVATION` | 模拟训练侧缺少 rollout 数据 |
| `BUFFER_BACKLOG` | 模拟 rollout buffer 积压 |
| `VERSION_DRIFT` | 模拟策略版本过旧的输出 |

相同 seed、算法、模式和基准时间会生成稳定事件。Synthetic Trace 只表达协议和
状态机语义，不代表真实训练采样或性能。

`TraceNormalizer` 会校验必要语义字段，对完全相同的 `event_id` 去重，拒绝同一 ID
的冲突内容，并按时间、source revision 和 event ID 稳定排序。输入消息不会被原地
修改。

## DAG 与等待分类

```python
from tgsrl_runtime import GapClassifier, IncrementalDAG

dag = IncrementalDAG(contract.phase_graph)
print(dag.ready_set)

dag.mark_completed("prefill")
print(dag.ready_set)

kind = GapClassifier().classify(normalized[0])
print(kind.value)
```

`IncrementalDAG` 支持追加 phase/edge、标记完成、估计剩余时间和导出
`PhaseGraph`。更新是事务性的：环、非法 entry 或冲突定义会使整次更新失败。

等待分类覆盖 long-tail、tool/environment wait、role/phase misalignment、
serial/weight-sync、invalid/stale output 和 unknown。

## 本地确定性 Replay

```python
import asyncio

from tgsrl_runtime import ReplayController

async def main() -> None:
    replay = ReplayController(normalized, seed=7, rate=2.0)
    async for event in replay.events():
        print(event.event_id)

asyncio.run(main())
```

控制接口：

```python
replay.pause()
event = replay.step()
checkpoint = replay.checkpoint()
replay.restore(checkpoint)
replay.resume()
```

Replay 使用虚拟时钟推进事件时间并返回事件副本。Checkpoint 只能恢复到同一事件流
和同一 seed。

## Replay 与 Experiment 服务

Runtime 进程同时注册 `ExperimentService`。Replay 保存带摘要校验的 typed step artifact。
新写入的 `tgsrl.replay-artifact.v2` 每一步包含事件、Intent、Snapshot、可选的 recorded
Decision，以及明确的 `EvaluationContext`（tick、evaluation time、decision sequence、cause、
observed revision 和 contract observation）。`start` 会按 ordinal 顺序把每一步发送给
Scheduler 的只读 `Schedule` RPC，并用 `decision-semantic-v2` 规范化易变 ID、cursor 和时间
字段后比较决策语义。实际 Decision artifact、语义摘要、比较状态、计数、
fallback/equivalence 指标和 Experiment result 会被保存。该路径不会调用 `PublishIntent`，
也不会执行 Provider mutation。

旧的 v1 artifact 仍可读取；缺少 evaluation context 时会合成 fast tick、
`replay-compat` cause 等兼容默认，并标记 `compatibilityDefaultsApplied=true`。这保证旧数据
可重放，但不表示原始 tick context 得到精确恢复。artifact schema、canonicalizer、digest 或
ordinal 不一致时 Replay 会失败，不会忽略损坏继续运行。

`pause`、`resume`、`stop`、`terminate` 更新 Replay 生命周期状态；Experiment 用于组织
replay run 与比较结果。`start` 要求 Replay 已带完整 typed scheduler input artifacts，且
Runtime 以 `--scheduler-target` 连接支持 `Schedule` 的 Scheduler；只有 Trace 引用而没有这些
artifact 的 Replay 不能执行 Scheduler-backed replay。

Replay runner 支持 recorded、override 和 derive 三种 seed mode，但当前服务端 `start` 使用
recorded mode，HTTP/CLI 没有 seed-mode 选择参数。每完成一个 Scheduler step，Runtime 都会按
Replay ID、START 幂等键摘要和 ordinal 持久化进度。同一 START 使用相同幂等键重试时，会先
校验已保存 step 的 digest，再从下一个未完成步骤继续；不会重新调度已经持久化的步骤。

面向终端用户时，通常通过 HTTP Gateway 或其 Python SDK 操作这些资源：

```python
from tgsrl_gateway import GatewayClient

client = GatewayClient("http://127.0.0.1:8080")
replays = client.list_replays()
experiments = client.list_experiments()
```

Gateway 的 gRPC backend 会把 Replay 和 Experiment 请求转发到 Runtime 暴露的
`ExperimentService`；`memory` backend 不持久化，也不连接 Runtime。Replay/Experiment
创建接口虽然接收 `Idempotency-Key`，但服务端不提供与 Job lifecycle 相同的请求去重保证。
Experiment 提供创建、查询、列表、Trace 汇总和 Scheduler 预览 Replay 结果，不应视为完整的远程
训练实验平台。

## 构建并发布 SchedulingIntent

```python
from datetime import timedelta

from tgsrl.v1 import execution_pb2, resource_pb2
from tgsrl_runtime import IntentBuilder

intent = IntentBuilder().build(
    execution_id="execution-1",
    stage_id="decode",
    job_id="job-1",
    contract=contract,
    rollout_mode=PartialAsyncRolloutAdapter.proto_value,
    phase_kind=execution_pb2.PHASE_KIND_DECODE,
    policy_version="policy-1",
    ttl=timedelta(seconds=60),
    resources_per_unit=resource_pb2.ResourceVector(
        cpu_millis=1000,
        memory_bytes=1_073_741_824,
    ),
    required_capabilities=resource_pb2.CapabilitySet(
        names=["logical-cpu"],
        algorithms=["grpo"],
        rollout_modes=["partially_async"],
        source="mock",
        revision=1,
        supported_actions=["bind"],
    ),
    labels={"data_kind": "synthetic"},
)
```

Builder 为每个 `(execution_id, stage_id)` 维护单调递增版本，生成规范幂等键，并校验
TTL、契约、stage、资源和能力。Runtime 服务会从 SQLite 中的 Intent 恢复这些版本水位；
单独创建的 Builder 对象仍只在自身生命周期内保存计数。

直接发布：

```python
import asyncio

from tgsrl_runtime import SchedulerClient

async def publish() -> None:
    async with SchedulerClient("127.0.0.1:50051") as client:
        response = await client.publish_intent(intent)
        print(response.status)

asyncio.run(publish())
```

Runtime 服务会从 manifest、runtime units 和 Trace 汇总构造每个 unit 的 Intent。发布成功
只表示 Scheduler 接受请求；最终资源动作应通过 Decision 结果判断。完整 RPC 语义见
[调度服务指南](scheduler-service.md)。

## SQLite 持久化与恢复边界

默认 `--state-db .cache/tgsrl/runtime.db` 会启用 SQLite；显式传 `:memory:` 可使用临时库。
数据库持久化 RuntimeManifest、runtime units、sandbox、sandbox event、Trace batch、Intent、
checkpoint、Replay、Experiment 和 component status，并启用 WAL、完整同步和 schema
migration。一次 Sandbox 观察会将 sandbox、unit、event 和 component status 作为同一事务
提交，再更新内存投影。

进程启动时会从 SQLite 重建：

- RuntimeManifest；
- runtime units；
- sandboxes；
- runtime event 与 Trace batch；
- 最新 Intent、checkpoint 元数据、Replay、Experiment 和 component status；
- 全局且可能稀疏的 runtime event sequence，以及 generation、Intent version、start 幂等和
  其他必要水位。

Runtime 会遍历持久化分页。SQLite 恢复服务查询和同 key Start 重试所需的本地状态，
并从持久 Start outbox 补投未确认的 Intent；确认过的 Intent 不会重复投递。该机制不提供
跨服务事务、HA 或灾备保证。
`WatchRuntimeEvents` 会按全局 sequence/cursor 返回保留的历史事件；尽管它是 streaming
RPC，一次调用不会等待未来事件。

## 使用限制

- CPU Mock 只提供逻辑资源与动作语义，不能代表真实 NVIDIA GPU、CUDA MPS/MIG、
  外部训练框架或多节点执行行为。
- Runtime gRPC 没有 TLS、认证或授权；不要直接暴露到公网或不可信共享网络。
- SQLite 适合单实例持久化；不要让多个 Runtime 进程写同一数据库文件。
- Runtime 不提供跨服务事务、HA、灾备或训练进程镜像恢复。
- Synthetic Trace、调度 Replay 和 Experiment 汇总不能用来推断真实训练性能、
  GPU 利用率、收敛质量或 wall-clock 收益。
- v1 Replay 的兼容 context 是合成值；需要精确比较 tick 和契约观测时应使用 v2 artifact。
