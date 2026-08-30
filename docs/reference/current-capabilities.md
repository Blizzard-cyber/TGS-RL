# 支持范围与限制

本页用于选择适合的 TGS-RL 使用方案。状态含义如下：

- **支持（Supported）**：实现完成，并在目标环境获得了对应验证证据。
- **已实现，待硬件验证（Implemented, hardware verification pending）**：代码与模拟/CPU
  验证完成，但尚无目标硬件证据。
- **有条件（Conditional）**：实现依赖仓库外组件、特定版本或部署方集成。
- **不支持（Unsupported）**：尚未实现，或明确不在当前范围内。

## 功能支持矩阵

| 领域 | 状态 | 用户可用能力 | 要求与限制 |
|---|---|---|---|
| 单机控制链 | **支持** | Runtime → Scheduler → CPU Mock Provider → Decision → fake Operator → Sandbox observation | 不需要 GPU 或 Kubernetes；不会创建真实硬件或集群资源 |
| 执行语义 | **支持** | PPO、GRPO；sync、partially async、fully async；ExecutionContract、typed contract observation/evaluation、Trace、DAG、Gap、Intent 与调度预览 Replay | Synthetic Trace 和 Replay 只表达协议与决策语义，不代表训练工作负载；没有 typed predicate 的旧式字符串规则不会被解释执行 |
| Scheduler | **支持** | 硬约束、候选评分、per-unit TopK、周期 fast/medium/slow tick、Admission/Fast/Medium/Slow Planner、L1–L4 ActionLevel 授权、有界 Decision/Planner evidence、fallback、mutation protection、幂等 action、补偿、Decision cursor、provider projection 恢复和调和 | 默认 evidence budget 为 candidate/rejection 合计 4096 条且始终保留选中候选；用 totals 与 truncation 标志判断完整性。默认 preemption 为 `noop`；无法原子表达 replacement 时 fail closed |
| 配置图 | **支持** | Scheduler 与 Runtime 从 compatibility manifest 加载 BOM、profile、capabilities、policy 和 scenario | 配置在启动时读取，不支持热更新；未知能力和冲突引用会被拒绝 |
| 单机持久化 | **支持** | Scheduler checkpoint/journal、Job Controller 文件状态、Runtime/Experiment SQLite、Operator cursor 与 ledger | 不提供跨服务事务、HA 或灾备；fake backend 对象只存在于进程内 |
| Job、Runtime 与 Experiment 控制 | **支持** | JobControl、RuntimeControl、RuntimeBackendControl 和 Experiment gRPC 服务 | Runtime `start` 发布 Intent；workload 必须由 Operator/backend 启动并通过观察事件回报 |
| HTTP Gateway | **支持** | Job、Run、Timeline、DAG、Topology、Sandbox、Decision、Replay、Experiment、OpenAPI、CLI 与 Python SDK | gRPC 模式要求四个逻辑后端可达；内存模式不持久化 |
| Web Console | **支持** | 概览、任务详情、时间线、拓扑、Sandbox、Decision、实验比较，以及 Job/Run 准入和生命周期操作 | 静态 `mock` adapter 不访问 Gateway；静态部署需自行提供同源 API 代理 |
| CPU Mock Provider | **支持** | 能力匹配、逻辑资源绑定、L1–L4 逻辑模拟动作、故障注入、generation fence 和逐动作 rollback | Adaptive Planner 会在满足观测、能力与安全条件时生成 L1–L4 动作；这些结果只验证控制逻辑，不代表真实硬件行为或性能 |
| NVIDIA Provider（默认） | **有条件（Conditional）** | `LocalDriver` 可通过 `nvidia-smi` 形成设备快照 | 需要 NVIDIA 驱动和 `nvidia-smi`；默认不声明资源动作 |
| NVIDIA Driver v2 | **有条件（Conditional）** | 已实现 inventory、MPS `set_share` 写入与读回、MIG、binding、runtime command、事务、幂等、超时、回滚、重启发现、dry-run 与审计的 Go 编排及 fake conformance 测试 | 实际动作依赖仓库外 `tgsrl-nvidia-binding`、`tgsrl-nvidia-runtime`、`tgsrl-nvidia-mig` helper；MPS 不公开通用 `resize`，MIG L4 需 helper 声明完整 lifecycle transaction；仓库尚未提供这些 helper，也没有真实 NVIDIA/CUDA 证据，因此不能标记为“已实现，待硬件验证”或“支持” |
| 外部 Runtime Adapter | **有条件支持** | veRL、OpenRLHF、Ray、PyTorch、vLLM、SGLang 的依赖检查、manifest 校验和 typed lifecycle bridge | 必须安装对应 Python 包，并提供可用的 provider hook、执行后端、分布式环境和资源控制 |
| Kubernetes Operator | **有条件支持** | 编译和调和 `JobRunBundle`、Kueue `Workload`、Kubernetes `Job`、可选 `ResourceClaim`/`RuntimeClass`，并观察状态；Kubernetes 1.35.1 + Kueue 0.19.2 的本地 CPU API/RBAC/重启验证通过 | 用户必须提供其余 TGS-RL 服务和所选 GPU/DRA 组件；本地 CPU 证据不代表生产集群或 GPU 验证，随附工件只部署 Operator |

## 单机方案

推荐先使用 `docker compose up`。该方案提供：

- 通过 Gateway、CLI、SDK 和 Console 操作 Job/Run；
- 用 CPU Mock Provider 执行逻辑资源预留、动作、失败补偿和 Decision 订阅；
- 用 fake Operator backend 完成 bundle 编译、生命周期控制和 Sandbox 状态回传；
- 使用固定 seed 的 Synthetic Trace 和 Scheduler `Schedule` RPC 执行无资源副作用的 Replay；
- 保存并恢复各组件的单机状态。

Mock Provider 支持 `bind`、`release`、`set_share`、`set_priority`、`resize`、
`pause`、`resume`、`sleep`、`offload`、`rebind` 和 `recreate`。这些动作修改逻辑
资源状态，不操作物理 GPU、容器或 Kubernetes 对象。

## 外部训练框架要求

Runtime 可以选择以下 Adapter：

| 类型 | 名称 | 必要 Python 依赖 |
|---|---|---|
| Framework | veRL、OpenRLHF | `verl`、`openrlhf` |
| Execution backend | Ray | `ray` |
| Trainer | PyTorch | `torch` |
| Rollout engine | vLLM、SGLang | `vllm`、`sglang` |

Adapter 将 manifest 转换为结构化 `LaunchSpec`，并支持 direct command、Python module
hook 或 API hook。安装 Python 包只是必要条件；要运行训练，还必须提供与所选组合匹配的
镜像、命令、provider hook、资源后端、网络和分布式配置。缺少依赖或 hook 时请求会返回
unavailable，不会回退为成功。

## Kubernetes 方案要求

`kubernetes` Operator backend 需要：

- 可通过显式 kubeconfig、`KUBECONFIG`、用户默认 kubeconfig 或集群内 ServiceAccount
  访问的 Kubernetes API Server；
- 提供 `v1beta2` 或 `v1beta1` Workload API 的 Kueue；Operator 优先选择 discovery
  返回的受支持版本；
- 若使用 DRA，则优先使用稳定的 `resource.k8s.io/v1` API，也兼容
  `v1beta2`/`v1beta1`，并始终需要对应的 GPU DeviceClass 与驱动；
- 所选 GPU profile 所需的 Device Plugin、DRA 或 HAMi 组件；
- 独立部署且可从 Operator 访问的 Scheduler、Job Controller 和 Runtime；
- Operator cursor 目录的持久卷。

Helm 默认将业务对象写权限限制在 namespace，并为 Node、RuntimeClass、DeviceClass
discovery 提供只读 `list` 集群权限。只有在设置 `runtimeClassCreate=true` 时才授予
最小的 cluster-scoped RuntimeClass 写权限；否则应预先创建并引用 RuntimeClass。部署者
需要先在目标集群确认 API 版本、RBAC、StorageClass、准入策略和 GPU 控制器兼容性。

## NVIDIA Driver v2 启用条件

Scheduler 可通过 `-nvidia-driver-v2` 选择 v2 编排，默认分区模式是 MPS，也可选择 MIG。
启动后只有 helper 的 capability handshake、generation fencing、幂等与 durable receipt 条件
全部满足时，Provider 才会公开对应 action。helper 缺失或协议不匹配时返回 unavailable，
不会静默回退到模拟成功。`-nvidia-dry-run` 只验证命令计划，不能生成 GPU 通过证据。
MPS `set_share` 必须在写入后读回实际 active-thread percentage，事务提交后才发布该字段的
Sandbox observation；通用 `resize` 当前不由 MPS 暴露。MIG `rebind/recreate` 只有在 helper
同时声明 safe-point、checkpoint、stop、restore 和 readiness 时才可用。

## 明确不支持

- 直接把 Gateway、gRPC 或 Prometheus 端点暴露到公网或不可信共享网络；
- 内置 TLS、身份认证、授权、多租户隔离、CORS 策略、限流或密钥管理；
- 在未安装并验证外部 helper 时使用 NVIDIA Driver v2 执行 GPU 分配、MIG/MPS 管理或
  Runtime lifecycle；
- 把 `nvidia-smi` 设备发现、Mock 行为或单元测试解释为真实 GPU 调度与执行验证；
- 依靠 fake backend 在进程重启后恢复 workload 对象；
- 跨服务原子事务、自动故障转移、HA 或灾备；
- 把 Mock、Synthetic、Replay 或内存模式结果作为真实 GPU 吞吐、利用率、收敛质量、
  成本、多节点稳定性或 wall-clock 收益的依据。

如需共享或长期运行服务，应在外部补充 TLS、身份认证、授权、审计、网络策略、限流、
可靠的进程管理、备份与恢复演练，并使用适合目标环境的 HTTP server 和基础设施 Driver。
