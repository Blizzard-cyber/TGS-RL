# Scheduler 指南

Scheduler 是 **Scheduling & Infrastructure Control** 的决策核心。它加载兼容性与
策略配置，维护版本化资源状态，接收设备无关的 `SchedulingIntent`，执行 Provider
动作并发布可审计的 `DecisionRecord`。默认使用 CPU Mock Provider。

## 启动

```bash
go run ./scheduler-go/cmd/scheduler \
  -listen 127.0.0.1:50051 \
  -config-root . \
  -manifest compatibility/manifests/cpu-mock.yaml \
  -state-dir .cache/tgsrl/scheduler-state \
  -metrics-listen 127.0.0.1:9090
```

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-listen` | `127.0.0.1:50051` | gRPC 地址 |
| `-config-root` | `.` | 配置引用解析根目录 |
| `-manifest` | CPU Mock manifest | 兼容性清单覆盖 |
| `-fallback` | 由 policy 推导 | 兼容参数：`noop` 或 `static` |
| `-state-dir` | `.tmp/scheduler-state` | checkpoint 与 journal 目录 |
| `-metrics-listen` | `127.0.0.1:9090` | Prometheus 地址；空字符串可禁用 |

`GET http://127.0.0.1:9090/metrics` 可读取指标。gRPC 与指标端点都没有 TLS 或
认证，只应绑定可信接口。

## 调度路径

```mermaid
flowchart LR
  RT[Runtime] -->|PublishIntent| SERVICE[Scheduler service]
  SERVICE --> STORE[Versioned state]
  STORE --> PLAN[Constraints + scoring + plan]
  PLAN --> PROVIDER[ResourceProvider]
  PROVIDER --> RESULT[ActionResult]
  RESULT --> DECISION[DecisionRecord]
  DECISION --> RT
  DECISION --> OP[Operator]
  DECISION --> GW[Gateway / Console]
```

一次已接受的 Intent 会先物化 pending units，并在不可变 Snapshot 上评估执行契约、
过滤硬约束、评分和计划。提交计划前会复核 Snapshot revision 与 Intent version。Provider
动作完成后，每个 binding 分别确认或释放资源；证据缺失时采用 fail-closed 处理。

Scheduler 使用 fast、medium、slow 三条进程内 keyed queue 处理同一个
`(execution_id, stage_id)`。同一 key 在队列中重复出现时会合并 cause、最新 revision 和
最新 contract observation，并保留原有 FIFO 位置。三个 loop 的默认周期分别为 `25ms`、
`100ms` 和 `250ms`；周期控制开始处理的节奏，不是决策完成时限或 SLA。同一档 tick 尚未
完成时，重叠 tick 会被跳过。

## gRPC 接口

| RPC | 用途 |
|---|---|
| `PublishIntent` | 接受版本化 Intent，并异步触发调度与动作 |
| `GetSnapshot` | 读取最新资源快照，或等待快照至少达到指定 revision |
| `Schedule` | 预览决策；不执行动作或提交 allocation |
| `ListDecisions` / `GetDecision` | 分页读取或按 ID 查询决策 |
| `WatchDecisions` | 使用 cursor 持续订阅决策 |

Python 异步客户端示例（放在调用方的 `async` 函数中运行）：

```python
from tgsrl_runtime import SchedulerClient

async with SchedulerClient(
    "127.0.0.1:50051",
    timeout=5.0,
    max_retries=3,
) as client:
    response = await client.publish_intent(intent)
    async for decision in client.watch_decisions(
        job_ids=(intent.job_id,),
        after_sequence=0,
        heartbeat_seconds=10.0,
    ):
        print(response.status, decision.decision_id, decision.fallback)
        break
```

通过 Gateway 查询决策通常更适合交互式客户端：

```bash
uv run --frozen tgsrl list-decisions JOB_ID --run-id RUN_ID
uv run --frozen tgsrl get-decision JOB_ID DECISION_ID
```

## 执行契约评估

候选放置前，Scheduler 会对 Intent 中的 `ExecutionContract` 执行确定性评估。当前评估：

- 带 typed `predicate` 的 `ValidityRule` 和 `Condition`；
- `component=protocol` 的 `VersionConstraint`；
- `BackpressurePolicy`；
- `CommitPolicy.require_safe_point` 与 `SafePointPolicy`。

每条 `ContractEvaluation` 记录 clause、状态、观测值、证据、缺失字段、failure mode 和建议
动作，并保存在 `DecisionRecord.contractEvaluations`。要求 reject、pause、abort 或等待 safe
point 的结果会阻止本次放置，形成 fallback；对应的 rejected candidate 也会携带该评估。

以下边界需要由调用方处理：

- 只有字符串 `expression`、没有 typed predicate 的旧式 validity rule 不会解释执行该
  字符串，而会记录为 `INDETERMINATE` 并按兼容规则放行；
- 非 `protocol` 的 version component 当前记录为 `NOT_APPLICABLE`；
- backpressure 的 block、shed 或 scale-out 是决策建议和审计证据；Scheduler 当前不会直接
  操作生产者、删除样本或扩容外部基础设施。

## 决策与 Fallback

Scheduler 会记录输入版本、tick 与 evaluation context、有界候选和拒绝证据、分项得分、
选中计划、Fallback、动作与补偿结果。`PublishIntent` 返回 accepted 只表示接收成功；应查看
`DecisionRecord` 的 `fallback` 和 `action_results` 判断资源动作结果。

`top_k` 是每个 pending unit 的策略选择面，不是全局候选上限，也不会跳过 unit/device pair
的硬约束评估。候选引擎先按统一的 score-first 顺序形成 TopK shortlist，再允许配置的
策略在该 shortlist 内二次选择。
`DecisionRecord.candidates` 与 `rejectedCandidates` 是有界审计投影，不保证包含完整候选
矩阵。candidate 与 rejection 共用默认 4096 条 evidence budget；已选候选始终保留，必要时
有效预算会增长。客户端应使用 `totalCandidateCount`、`totalRejectedCandidateCount` 和
`evidenceTruncated` 判断证据是否完整，不能用数组长度推断实际评估总数。

支持的 Fallback：

- `noop`：不授权新的资源 mutation；
- `static`：保留可用的已有 binding，不创建新的 mutation。

`Schedule` 是无资源副作用的预览：返回的 Decision 不进入 Scheduler 的 retained decision
history。Replay 会把这类预览结果作为自己的 typed artifact 保存。`WatchDecisions` 只订阅
权威 `PublishIntent` 路径发布的记录，支持 sequence 或 decision ID 恢复、job 过滤和
heartbeat；cursor 早于保留窗口时会返回 `OUT_OF_RANGE`。

## 配置和策略

Scheduler 启动时会加载 manifest 引用的 BOM、profile、capabilities、policy 和 scenario，
并进行 schema、引用、版本、能力和交叉字段校验。策略支持：

- `stable-first-fit` / `score-first`；
- `binpack`；
- `trace-aware`；
- 可配置 `top_k` 和 fast/medium/slow event-loop interval。

三档 tick 同时限定本轮可执行的最大动作级别：

| Tick | 默认周期 | 最大 ActionLevel |
|---|---:|---:|
| fast | `25ms` | L1 |
| medium | `100ms` | L3 |
| slow | `250ms` | L4 |

Action type 与 level 的映射由 Scheduler 统一校验：

| ActionLevel | Action type |
|---|---|
| L1 | `set_share`、`set_priority`、`resize`、`bind`、`release` |
| L2 | `pause`、`resume` |
| L3 | `sleep`、`offload` |
| L4 | `rebind`、`recreate` |

Action 声明的 level 必须与 type 匹配，`tick_kind` 必须与 Decision 的 tick 一致，且不得超过
该 tick 的 ceiling；不符合规则的 plan 会 fail closed，执行前还会再次校验。当前常规放置
planner 生成 L1 `bind`，L2–L4 表示协议和 Provider 可表达的动作范围，不表示 Scheduler 会
在默认路径主动生成所有这些动作。仅兼容输入允许缺失 tick 的旧式 L1 action；新调用方
应始终发送明确的 `tick_kind`。

默认 policy 还启用 mutation protection：按 execution/stage 应用 cooldown、hysteresis、
时间窗 action budget 和 circuit breaker。保护拒绝会形成带 `PROTECTION_*` 原因的 fallback。
可选择 `low_priority_first` preemption 候选策略，并应用 safe-point/capability 检查；
默认 CPU Mock policy 将 preemption 关闭为 `noop`。Store 不能原子表达“释放 victim
并预留 replacement”，所以即使显式启用也会 fail closed 为
`PREEMPTION_NOT_EXPRESSIBLE`，不会执行 release-only 计划。

路径和环境变量详见[配置、持久化与恢复](configuration-and-recovery.md)。

## Provider 边界

### Mock Provider

Mock Provider 支持能力检查、资源绑定、L1–L4 状态转换、故障注入、幂等 action 和
逐动作补偿。它只提供逻辑资源状态，不代表任何真实硬件行为或性能。

### NVIDIA Provider

选择 `provider=nvidia` 时，Provider 维护设备、Sandbox、action 幂等记录、plan 状态以及
resource/sandbox watch；执行 action 时先调用 Driver，再提交 Provider 自有状态，并在状态
提交失败时按声明尝试 rollback。默认 `LocalDriver` 只通过 `nvidia-smi` 探测本机设备，
不声明 supported actions，并拒绝 bind/release、MIG/MPS share/resize 和 Runtime 控制。

因此默认 NVIDIA 方案只支持设备发现，不能分配 GPU 或运行训练。要执行资源动作，
用户必须提供基础设施专用 Driver，并为其声明的 action 实现幂等、回滚、调和与隔离语义。

## 持久化与恢复

Scheduler 在 `-state-dir` 下维护 checkpoint 与 journal，并在监听请求前恢复 Snapshot、
Intent、Decision、action result、cursor 和 reservation。启动恢复会先向完整 Provider
查询或调和未完成 plan，把 reservation 收敛为成功或失败并保存 checkpoint；随后按稳定
顺序把恢复出的 Intent 重新送入同一权威调度路径。已由恢复 allocation 满足的 Intent
会被跳过，避免重复执行 bind。无法确认的 in-flight plan 会 fail closed 为失败，不会盲目
重放外部副作用。持久化失败会关闭后续 mutation 入口；损坏、I/O 或 Provider 调和错误
会使启动失败。完整恢复边界见
[配置、持久化与恢复](configuration-and-recovery.md)。
