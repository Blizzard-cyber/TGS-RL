# 支持范围与限制

本页用于选择适合的 TGS-RL 使用方案。状态含义如下：

- **支持**：项目提供可直接使用的实现和所需运行工件。
- **有条件支持**：接口或后端可用，但用户必须提供表中列出的外部依赖与集成。
- **不支持**：不要依赖该能力，也不要把本项目输出解释为对应保证。

## 功能支持矩阵

| 领域 | 状态 | 用户可用能力 | 要求与限制 |
|---|---|---|---|
| 单机控制链 | **支持** | Runtime → Scheduler → CPU Mock Provider → Decision → fake Operator → Sandbox observation | 不需要 GPU 或 Kubernetes；不会创建真实硬件或集群资源 |
| 执行语义 | **支持** | PPO、GRPO；sync、partially async、fully async；ExecutionContract、typed contract observation/evaluation、Trace、DAG、Gap、Intent 与调度预览 Replay | Synthetic Trace 和 Replay 只表达协议与决策语义，不代表训练工作负载；没有 typed predicate 的旧式字符串规则不会被解释执行 |
| Scheduler | **支持** | 硬约束、候选评分、per-unit TopK、fast/medium/slow tick、ActionLevel 授权、有界 Decision evidence、fallback、mutation protection、幂等 action、补偿、Decision cursor、恢复调和和 Prometheus metrics | 默认 evidence budget 为 candidate/rejection 合计 4096 条且始终保留选中候选；用 totals 与 truncation 标志判断完整性。默认 preemption 为 `noop`；无法原子表达 replacement 时 fail closed |
| 配置图 | **支持** | Scheduler 与 Runtime 从 compatibility manifest 加载 BOM、profile、capabilities、policy 和 scenario | 配置在启动时读取，不支持热更新；未知能力和冲突引用会被拒绝 |
| 单机持久化 | **支持** | Scheduler checkpoint/journal、Job Controller 文件状态、Runtime/Experiment SQLite、Operator cursor 与 ledger | 不提供跨服务事务、HA 或灾备；fake backend 对象只存在于进程内 |
| Job、Runtime 与 Experiment 控制 | **支持** | JobControl、RuntimeControl、RuntimeBackendControl 和 Experiment gRPC 服务 | Runtime `start` 发布 Intent；workload 必须由 Operator/backend 启动并通过观察事件回报 |
| HTTP Gateway | **支持** | Job、Run、Timeline、DAG、Topology、Sandbox、Decision、Replay、Experiment、OpenAPI、CLI 与 Python SDK | gRPC 模式要求四个逻辑后端可达；内存模式不持久化 |
| Web Console | **支持** | 概览、任务详情、时间线、拓扑、Sandbox、Decision、实验比较，以及 Job/Run 准入和生命周期操作 | 静态 `mock` adapter 不访问 Gateway；静态部署需自行提供同源 API 代理 |
| CPU Mock Provider | **支持** | 能力匹配、逻辑资源绑定、L1–L4 逻辑模拟动作、故障注入、generation fence 和逐动作 rollback | 默认放置 planner 当前生成 L1 `bind`；L2–L4 是 Provider 可表达的模拟动作，不代表默认 Scheduler 会主动规划，也不代表真实硬件行为或性能 |
| NVIDIA Provider | **有条件支持：设备发现** | 默认 LocalDriver 可通过 `nvidia-smi` 形成设备快照 | LocalDriver 不声明或执行 bind/release、MIG/MPS share、resize 或 Runtime 控制；仓库未提供真实 GPU 分配、训练执行、性能或恢复验证证据，资源操作需要自定义基础设施 Driver |
| 外部 Runtime Adapter | **有条件支持** | veRL、OpenRLHF、Ray、PyTorch、vLLM、SGLang 的依赖检查、manifest 校验和 typed lifecycle bridge | 必须安装对应 Python 包，并提供可用的 provider hook、执行后端、分布式环境和资源控制 |
| Kubernetes Operator | **有条件支持** | 编译和调和 `JobRunBundle`、Kueue `Workload`、Kubernetes `Job`、可选 `ResourceClaim`/`RuntimeClass`，并观察状态 | 用户必须提供其余 TGS-RL 服务、兼容的 Kubernetes API、Kueue 和所选 GPU/DRA 组件；随附工件只部署 Operator |

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
- 与生成对象兼容的 Kueue `v1beta1`；
- 若使用 DRA，则需要 Kubernetes 1.32 风格的 `resource.k8s.io/v1beta1` API 和对应驱动；
- 所选 GPU profile 所需的 Device Plugin、DRA 或 HAMi 组件；
- 独立部署且可从 Operator 访问的 Scheduler、Job Controller 和 Runtime；
- Operator cursor 目录的持久卷。

Helm 默认使用 namespace 范围的 RBAC。只有在设置 `runtimeClassCreate=true` 时才授予
最小的 cluster-scoped RuntimeClass 写权限；否则应预先创建并引用 RuntimeClass。部署者
需要先在目标集群确认 API 版本、RBAC、StorageClass、准入策略和 GPU 控制器兼容性。

## 明确不支持

- 直接把 Gateway、gRPC 或 Prometheus 端点暴露到公网或不可信共享网络；
- 内置 TLS、身份认证、授权、多租户隔离、CORS 策略、限流或密钥管理；
- 使用默认 NVIDIA LocalDriver 执行 GPU 分配、MIG/MPS 管理或 Runtime lifecycle；
- 把 `nvidia-smi` 设备发现、Mock 行为或单元测试解释为真实 GPU 调度与执行验证；
- 依靠 fake backend 在进程重启后恢复 workload 对象；
- 跨服务原子事务、自动故障转移、HA 或灾备；
- 把 Mock、Synthetic、Replay 或内存模式结果作为真实 GPU 吞吐、利用率、收敛质量、
  成本、多节点稳定性或 wall-clock 收益的依据。

如需共享或长期运行服务，应在外部补充 TLS、身份认证、授权、审计、网络策略、限流、
可靠的进程管理、备份与恢复演练，并使用适合目标环境的 HTTP server 和基础设施 Driver。
