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
| Scheduler | **支持** | 硬约束、候选评分、per-unit TopK、周期 fast/medium/slow tick、Admission/Fast/Medium/Slow Planner、L1–L4 ActionLevel 授权、有界 Decision/Planner evidence、fallback、mutation protection、幂等 action、补偿、Decision cursor、provider projection 恢复和调和 | 默认 evidence budget 为 candidate/rejection 合计 4096 条且始终保留选中候选；用 totals 与 truncation 标志判断完整性。阻断性 pause 必须覆盖全部活跃 allocation，任何目标缺失、观测过期或超预算都会整体 fail closed。默认 preemption 为 `noop`；无法原子表达 replacement 时 fail closed |
| 配置图 | **支持** | Scheduler 与 Runtime 从 compatibility manifest 加载 BOM、profile、capabilities、policy 和 scenario | 配置在启动时读取，不支持热更新；未知能力和冲突引用会被拒绝 |
| 单机持久化 | **支持** | Scheduler checkpoint/journal、Job Controller 文件状态、Runtime/Experiment SQLite、Operator cursor 与 ledger | 不提供跨服务事务、HA 或灾备；fake backend 对象只存在于进程内 |
| Job、Runtime 与 Experiment 控制 | **支持** | JobControl、RuntimeControl、RuntimeBackendControl 和 Experiment gRPC 服务 | Runtime `start` 发布 Intent；workload 必须由 Operator/backend 启动并通过观察事件回报 |
| HTTP Gateway | **支持** | Job、Run、Timeline、DAG、Topology、Sandbox、Decision、Replay、Experiment、OpenAPI、CLI 与 Python SDK | gRPC 模式要求四个逻辑后端可达；内存模式不持久化 |
| Web Console | **支持** | 概览、任务详情、时间线、拓扑、Sandbox、Decision、实验比较，以及 Job/Run 准入和生命周期操作 | 静态 `mock` adapter 不访问 Gateway；静态部署需自行提供同源 API 代理 |
| 性能回归门禁 | **支持（CI 回归）** | 独立非 race CI 检查 Scheduler 8 devices/100 units、1000 devices/1000 units 与 NVIDIA Provider observation apply 的 P95 预算 | 预算只约束固定 CPU fixture 的代码回退，不是生产 SLA、GPU 性能或训练收益证明 |
| CPU Mock Provider | **支持** | 能力匹配、逻辑资源绑定、L1–L4 逻辑模拟动作、故障注入、generation fence 和逐动作 rollback | Adaptive Planner 会在满足观测、能力与安全条件时生成 L1–L4 动作；这些结果只验证控制逻辑，不代表真实硬件行为或性能 |
| NVIDIA Provider（默认） | **有条件（Conditional）** | `LocalDriver` 可通过 `nvidia-smi` 形成设备快照 | 需要 NVIDIA 驱动和 `nvidia-smi`；默认不声明资源动作 |
| NVIDIA Driver v2 | **有条件（Conditional）** | 已实现 inventory、MPS `set_share` 写入与读回，以及 binding/runtime/MIG helper、Scheduler worker registry 和 workload bootstrap 的 generation fence、scoped registration、幂等 durable receipt、原子落盘、进程监管、PID 信号控制和 managed-worker lifecycle | DRA/CDI 负责设备注入；offload/reload 需要训练 worker 实现 Unix socket 协议；signal pause 不释放 GPU 显存；MPS PID 自动发布需要 host PID 可见性和共享目录；MIG 仅在已存在实例间切换；现有证据为真实本地子进程 + fake-command/CPU conformance，尚无真实 NVIDIA/CUDA 证据 |
| 外部 Runtime Adapter | **已实现，待硬件验证** | veRL 已有第一方 lifecycle/observation bridge，以及面向 veRL 0.9 trainer/worker-group/checkpoint-manager 公共接口的 callback adapter；支持显式 safe-point hook、checkpoint、rollout abort/sleep/wake、actor/critic offload/reload、policy update、durable receipt 与 typed TraceEvent | 当前验证使用 CPU 对象替身和 reference workload；真实 veRL/Ray/PyTorch/vLLM 依赖组合、分布式 collective 和 GPU 资源释放仍待目标环境验证；SGLang 与 OpenRLHF 仍只有通用 adapter 边界 |
| Kubernetes Operator | **有条件支持** | 编译和调和 `JobRunBundle`、Kueue `Workload`、Kubernetes `Job`、可选 `ResourceClaim`/`RuntimeClass`；typed NVIDIA DRA inventory 精确兑现 Full GPU/MIG UUID；可选 bootstrap 包装 RuntimeManifest command，自动注册真实 PID/control endpoint，并用 Pod readiness 阻止提前发布 RUNNING；全栈 Helm chart 部署六个控制面服务 | 精确 UUID 仅适用于 NVIDIA DRA 的整数个完整 GPU/MIG；镜像、Helm render、worker bootstrap 与 registry 已完成 CPU/HTTP/fake-process 契约验证，尚无真实 Kubernetes/DRA/Pod 证据；MPS 仍需节点侧 PID namespace/shared mount；Kueue 和 GPU 管理组件由平台侧提供 |

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
| Framework | veRL、OpenRLHF | veRL 使用仓库内 bridge，真实 worker 环境需 `verl`；OpenRLHF 需 `openrlhf` |
| Execution backend | Ray | `ray` |
| Trainer | PyTorch | `torch` |
| Rollout engine | vLLM、SGLang | `vllm`、`sglang` |

Adapter 将 manifest 转换为结构化 `LaunchSpec`，并支持 direct command、Python module
hook 或 API hook。veRL 可使用 `adapters.frameworks.verl_runtime.install_verl_control` 连接 0.9
trainer，并由训练循环显式调用 safe-point hook；其余 adapter 需要显式 bridge。要运行训练，还必须提供
与所选组合匹配的镜像、命令、资源后端、网络和分布式配置。缺少执行条件时请求会明确失败，
不会回退为成功。

## Kubernetes 方案要求

`kubernetes` Operator backend 需要：

- 可通过显式 kubeconfig、`KUBECONFIG`、用户默认 kubeconfig 或集群内 ServiceAccount
  访问的 Kubernetes API Server；
- 提供 `v1beta2` 或 `v1beta1` Workload API 的 Kueue；Operator 优先选择 discovery
  返回的受支持版本；
- 若使用 DRA，则优先使用稳定的 `resource.k8s.io/v1` API，也兼容
  `v1beta2`/`v1beta1`；当前只支持 NVIDIA `gpu.nvidia.com` driver，Full GPU 使用
  `gpu.nvidia.com` DeviceClass、MIG 使用 `mig.nvidia.com` DeviceClass，并要求 ResourceSlice
  提供 `type`、`uuid` 以及 MIG 的 `profile`、`parentUUID` typed metadata；
- 所选 GPU profile 所需的 Device Plugin、DRA 或 HAMi 组件；
- 可从 Operator 访问的 Scheduler、Job Controller 和 Runtime；可由 `deploy/helm/tgsrl`
  一并部署，也可使用 `deploy/helm/operator` 接入已有服务；
- Operator cursor 目录的持久卷。
- 使用 managed-worker bootstrap 时，已发布的不可变 bootstrap 镜像、workload 可访问的 registry
  URL，以及同时挂载给 Operator/Scheduler 的至少 32 bytes HMAC signing key；Pod 只获得 scoped token。

Helm 默认将业务对象写权限限制在 namespace，并为 Node、RuntimeClass、DeviceClass、ResourceSlice
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

仓库内 helper 可用 `make build-nvidia-binding` 构建到 `bin/tgsrl-nvidia-binding`。Scheduler
通过 `-nvidia-binding-helper` 指定二进制，通过 `-nvidia-binding-state` 指定持久化文件；
未指定时状态文件位于 `-state-dir` 下。bootstrap 可在显式配置 `TGSRL_NVIDIA_MPS_PID_DIR`、
且容器能看到唯一宿主 MPS server PID 时维护 generation-fenced `<sandbox>.pid`；普通 Pod
默认不具备该 PID 可见性。只有 PID identity 仍匹配时 helper 才声明 `mps_profile_pid`，从而允许 MPS
`set_share`；binding receipt 只表示该步骤已落盘，Provider commit 仍由事务执行器完成。
helper 的持久化 binding 需要由创建进程或容器的执行层消费，不能用它替代 CUDA/container
级设备隔离验证。

`make build-nvidia-runtime` 构建 `bin/tgsrl-nvidia-runtime`，`make build-worker-bootstrap` 构建
容器内 supervisor。Kubernetes 路径由 bootstrap 自动注册 PID、Pod UID、process token、binding、
generation、device IDs 和 control endpoint；本机兼容路径仍可手工 `register`。无 cooperative
control socket 时，进程存活即可注册，但 pause/sleep 仍要求 safe-point marker，且只使用
SIGSTOP/SIGCONT。offload 必须由 Unix
socket worker 显式确认 `prepare_pause → checkpoint → offload`，resume 会执行
`reload → resume` 并等待 readiness。状态文件由 `-nvidia-runtime-state` 指定，所有成功和
不确定结果都保留用于重启调和。

`make build-nvidia-mig` 构建 `bin/tgsrl-nvidia-mig`。该 helper 与 runtime helper 共享 worker
注册和 receipt 状态，并执行 `prepare_pause → checkpoint → stop → 目标 MIG UUID 回读 →
reload → readiness`。`rebind` 必须指定不同的 source/target MIG UUID，`recreate` 必须保持
同一 MIG UUID；当前不执行 `nvidia-smi mig -dci/-dgi/-cgi/-cci`，因为这种拓扑变更会生成
新设备身份，必须先由 Scheduler 的资源拓扑事务显式表达后才能安全开放。

Gate G/I runner 从同一个锁定 manifest 自动运行 baseline 和 variant，执行 warmup 与多次
measurement，并从原始 NDJSON 事件重新计算吞吐、P50/P95/P99 延迟、iteration time、GPU active
time、queue depth、policy lag、staleness、ESS、lifecycle latency、action/rollback rate 和 recovery
time。`make gate-cpu-integration` 会启动 Gateway、Job Controller、Runtime、Scheduler、Operator
process backend、worker bootstrap 与 veRL callback doubles；每个 trace 分组必须同时包含
service job/run identity、Scheduler decision/plan identity、worker/runtime-unit identity，variant 还必须
包含带 Runtime event 因果链的 Operator pause/resume。该路径只生成
`CPU_INTEGRATION/NOT_RUN`，不安装真实 veRL，也不声称 GPU/Kubernetes 通过。旧的
`verl-reference-workload.py` 仅保留为进程级 bridge/CUDA conformance；即使在 CUDA runner 上
成功也保持 `NOT_RUN`，不能单独形成 Gate PASS。`GPU_MULTI_NODE` 仍仅接受原始 trace 中至少有
两个真实节点身份且 baseline/variant 节点集合一致的外部运行证据。证据包包含原始
stdout/stderr、服务日志、worker 日志、trace、锁定配置、环境指纹、digest 和汇总报告，报告
指标必须与原始 trace 重算一致。

## 明确不支持

- 直接把 Gateway、gRPC 或 Prometheus 端点暴露到公网或不可信共享网络；
- 内置 TLS、身份认证、授权、多租户隔离、CORS 策略、限流或密钥管理；
- 在未验证仓库内 binding/runtime/MIG helper 与目标环境时执行 GPU 分配、
  MIG/MPS 管理或 Runtime lifecycle；
- 把 bootstrap/full-stack CPU 进程验证解释为真实 Kubernetes Pod、DRA/CDI、真实 veRL 包或 GPU 证据；
- 把 `nvidia-smi` 设备发现、Mock 行为或单元测试解释为真实 GPU 调度与执行验证；
- 依靠 fake backend 在进程重启后恢复 workload 对象；
- 跨服务原子事务、自动故障转移、HA 或灾备；
- 把 Mock、Synthetic、Replay 或内存模式结果作为真实 GPU 吞吐、利用率、收敛质量、
  成本、多节点稳定性或 wall-clock 收益的依据。

如需共享或长期运行服务，应在外部补充 TLS、身份认证、授权、审计、网络策略、限流、
可靠的进程管理、备份与恢复演练，并使用适合目标环境的 HTTP server 和基础设施 Driver。
