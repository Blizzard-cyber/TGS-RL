# 支持范围与限制

本页用于选择适合的 TGS-RL 使用方案。状态含义如下：

- **支持（Supported）**：实现完成，并在目标环境获得了对应验证证据。
- **单节点硬件已验证（Hardware verified, single node）**：声明的最小硬件场景已在一台
  真实 GPU 节点取得可复核证据；不外推到其他拓扑、完整训练或性能收益。
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
| 单机持久化 | **支持** | Scheduler checkpoint/journal、Job Controller 文件状态、Runtime/Experiment SQLite、Operator cursor 与 ledger；启动时调和遗留 delivery、终态 allocation 和已淘汰的重复 failure audit | 不提供跨服务事务、HA 或灾备；fake backend 对象只存在于进程内，Scheduler 通过 provider readback 保守修复其投影 |
| Job、Runtime 与 Experiment 控制 | **支持** | JobControl、RuntimeControl、RuntimeBackendControl 和 Experiment gRPC 服务 | Runtime `start` 发布 Intent；workload 必须由 Operator/backend 启动并通过观察事件回报 |
| HTTP Gateway | **支持** | Job、Run、Timeline、Trace、DAG、Topology、Resource Snapshot、Sandbox、Decision、Replay、Experiment、OpenAPI、CLI 与 Python SDK | `GET /v1/resources` 直接读取 Scheduler `ClusterSnapshot`；gRPC 模式要求四个逻辑后端可达；内存模式不持久化 |
| Web Console | **支持** | 九个中文工作区：运行总览、任务、Trace 多轨时间轴、时间线、算力资源、拓扑、Sandbox、Decision 和实验比较，以及 Job/Run 准入和生命周期操作 | 算力资源页显示 Scheduler 设备/分配账本，不替代 Operator allocation readback；Trace 只有事件携带真实 duration 属性时才显示耗时条。静态 `mock` adapter 不访问 Gateway；静态部署需自行提供同源 API 代理 |
| 性能回归门禁 | **支持（CI 回归）** | 独立非 race CI 检查 Scheduler 8 devices/100 units、1000 devices/1000 units 与 NVIDIA Provider observation apply 的 P95 预算 | 预算只约束固定 CPU fixture 的代码回退，不是生产 SLA、GPU 性能或训练收益证明 |
| CPU Mock Provider | **支持** | 能力匹配、逻辑资源绑定、L1–L4 逻辑模拟动作、故障注入、generation fence 和逐动作 rollback | Adaptive Planner 会在满足观测、能力与安全条件时生成 L1–L4 动作；这些结果只验证控制逻辑，不代表真实硬件行为或性能 |
| NVIDIA Provider（默认） | **有条件（Conditional）** | `LocalDriver` 可通过 `nvidia-smi` 形成设备快照 | 需要 NVIDIA 驱动和 `nvidia-smi`；默认不声明资源动作 |
| NVIDIA Driver v2 | **单节点硬件已验证** | `auto` 按设备发布 Full GPU 或已有 MIG 子设备；显式 MPS/MIG；binding/runtime/MIG helper、Scheduler worker registry 和 workload bootstrap 的 generation fence、scoped registration、幂等 durable receipt、原子落盘、进程监管、PID 信号控制和 managed-worker lifecycle | `68f5aea` 在单节点 NVIDIA A10 验证 Full GPU/DRA/CDI、真实 CUDA、注册、Trace、清理，以及 checkpoint、offload/reload/resume 和 allocator allocated/reserved bytes 的下降/恢复。MIG、MPS、完整训练与多节点仍待验证 |
| HAMi vGPU | **单节点硬件已验证** | 从 `hami.io/node-nvidia-register` 建立物理 UUID inventory；把单卡分数份额投影为 `nvidia.com/gpu`、`gpucores`、`gpumem-percentage`、显式 `hami-scheduler` 与 `use-gpuuuid`；从 Pod allocation annotation 回读 UUID、显存 MiB 和 core 百分比 | NVIDIA A10 上 H1 `0.4 → 40% core + 9211 MiB` 与 H2 两个 `0.4` worker 同卡并发均已通过，并在恢复 Device Plugin 后复跑 E1；单 Binding 仍只支持一张物理 GPU、`hami-core`、同一份额约束 core/memory。OOM 隔离、公平性、干扰上界和动态改份额仍待验证 |
| 外部 Runtime Adapter | **A10 最小 lifecycle 已验证；完整训练待验证** | veRL 已有第一方 lifecycle/observation bridge，以及面向 veRL 0.9 trainer/worker-group/checkpoint-manager 公共接口的 callback adapter；支持显式 safe-point hook、checkpoint、rollout abort/sleep/wake、actor/critic offload/reload、policy update、allocator bytes、durable receipt 与 typed TraceEvent | 锁定 workload 镜像导入真实 `verl/ray/torch/vllm` 包；最小 adapter trainer 已完成真实 CUDA/Trace，并在 A10 上通过 checkpoint/offload/reload/resume 与 allocator bytes 硬判定。完整 trainer、distributed collective 和真实模型 checkpoint 仍待验证 |
| Kubernetes Operator | **有条件支持；单节点 NVIDIA Helm 已验证** | 编译和调和 `JobRunBundle`、Kueue `Workload`、Kubernetes `Job`、可选 `ResourceClaimTemplate`/`RuntimeClass`；按每个 Binding 从有序 profile 中选择 DRA 或 HAMi；分别从 ResourceClaim/ResourceSlice 或 Pod HAMi annotation 回读实际 UUID；bootstrap 自动注册 PID/control endpoint，并用 Pod readiness 阻止提前发布 RUNNING；全栈 Helm chart 部署六个控制面服务 | `68f5aea` 在同一组 PVC 上完成 Helm install/upgrade 两轮、四个 Gateway gRPC 依赖就绪、A10 lifecycle 和 630/630 证据哈希校验。DRA 支持整数个 Full GPU/MIG；HAMi 当前支持单 Binding 单物理卡 `(0,1]`，多个 Binding 可共享同一物理 UUID。其他集群、MIG/MPS、HAMi 动态份额与故障隔离仍待验证 |
| 其他加速器厂商 | **不支持（接口已预留）** | 通用 Proto 已有 `NPU`、`TPU`、`CUSTOM`，Scheduler 使用厂商中立 `Device`、`CompleteResourceProvider`、`CapabilitySet` 与工厂注册表 | 当前没有昇腾或其他厂商的 Provider、Operator realization adapter、worker identity verifier、依赖锁、假实现或硬件证据 |

## 单机方案

推荐先使用 `docker compose up`。该方案提供：

- 通过 Gateway、CLI、SDK 和 Console 操作 Job/Run；
- 用 CPU Mock Provider 执行逻辑资源预留、动作、失败补偿和 Decision 订阅；
- 用 fake Operator backend 完成 bundle 编译、生命周期控制和 Sandbox 状态回传；
- 使用固定 seed 的 Synthetic Trace 和 Scheduler `Schedule` RPC 执行无资源副作用的 Replay；
- 保存并恢复各组件的单机状态。

服务健康后执行 `make compose-smoke` 可在 Docker 内从 Console 同源入口验证
`create → admit → start → pause → resume → stop`、Decision、Sandbox 终态、allocation 回收，
以及 Trainer/Request/Executor/Worker 四轨 synthetic Trace 的写入与查询。

Mock Provider 支持 `bind`、`release`、`set_share`、`set_priority`、`resize`、
`pause`、`checkpoint`、`resume`、`sleep`、`offload`、`reload`、`rebind` 和 `recreate`。这些动作修改逻辑
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
trainer，并由训练循环显式调用 safe-point hook。Runtime 只验证声明和控制桥，不在控制面容器
导入 Ray/PyTorch/vLLM；这些依赖由 workload 镜像提供。只有 manifest 同时包含 workload OCI
artifact 时，Operator 才把对应模块清单注入 bootstrap 并在启动前 fail closed。要运行训练，还必须提供
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
- 若使用 `hami-vgpu`，HAMi 必须发布健康、唯一且完整的
  `hami.io/node-nvidia-register`；admission/scheduler/device plugin 必须能处理
  `nvidia.com/gpu`、`nvidia.com/gpucores`、`nvidia.com/gpumem-percentage` 和
  `nvidia.com/use-gpuuuid`；
- 所选 GPU profile 所需的 Device Plugin、DRA 或 HAMi 组件；Operator 不会安装这些集群级
  依赖。仓库仅为专用单节点 Minikube H1/H2 验证提供显式、可逆的 HAMi 安装脚本；
- 可从 Operator 访问的 Scheduler、Job Controller 和 Runtime；可由 `deploy/helm/tgsrl`
  一并部署，也可使用 `deploy/helm/operator` 接入已有服务；
- Operator cursor 目录的持久卷。
- 使用 managed-worker bootstrap 时，已发布的不可变 bootstrap 镜像、workload 可访问的 registry
  URL，以及同时挂载给 Operator/Scheduler 的至少 32 bytes HMAC signing key；Pod 只获得 scoped token。

全栈 Helm 默认使用 CPU 配置；显式 `scheduler.nvidia.enabled=true` 时要求 NVIDIA
RuntimeClass、同节点 selector、worker registry/bootstrap、设备验证和所有控制面镜像 digest。
`make gpu-render-helm-values` 与 `make gpu-helm-smoke` 提供单节点实装和证据入口；最终
`68f5aea` 真实 A10 结果见 [A10 工程与 Helm 验收](../validation/a10-readiness-2026-09-14.md)。E1/H1/H2
历史 smoke 不能替代其他提交或拓扑的 Helm 实装证据，详见
[部署指南](../guides/deployment.md)和 [A10 Readiness](../guides/a10-readiness.md)。

Helm 默认将业务对象写权限限制在 namespace，并为 Node、RuntimeClass、DeviceClass、ResourceSlice
discovery 提供只读 `list` 集群权限。只有在设置 `runtimeClassCreate=true` 时才授予
最小的 cluster-scoped RuntimeClass 写权限；否则应预先创建并引用 RuntimeClass。部署者
需要先在目标集群确认 API 版本、RBAC、StorageClass、准入策略和 GPU 控制器兼容性。

## NVIDIA Driver v2 与异构设备

Scheduler 可通过 `-nvidia-driver-v2` 选择 v2 编排。Scheduler CLI 默认使用不会启动 MPS 的
`auto` 模式：未启用 MIG 的物理卡发布为 Full GPU；已启用 MIG 的卡只发布已有 MIG 子设备；
同一物理卡不会同时贡献整卡与 MIG 容量。不支持或未启用 MIG 的设备因此仍可正常整卡调度。
`full`、`mig` 可显式选择。生产 CLI 当前拒绝 `mps`：NVIDIA server-level active-thread
percentage 只影响之后创建的 MPS client，不能证明已经运行的训练进程份额发生变化。底层
backend 只保留 daemon/既有 binding 发现，不公开在线 `set_share`；需要共享时使用已验证的 HAMi。

启动后只有 helper 的 capability handshake、generation fencing、幂等与 durable receipt 条件
全部满足时，Provider 才会公开对应 action。helper 缺失或协议不匹配时返回 unavailable，
不会静默回退到模拟成功。`-nvidia-dry-run` 只验证命令计划，不能生成 GPU 通过证据。
MPS 在线 `set_share` 当前 fail closed；未来只有在新增 checkpoint/recreate 新 client、启动时
注入限额并对新 incarnation 回读验证后才可开放。MIG `rebind/recreate` 只有在 helper
同时声明 safe-point、checkpoint、stop、restore 和 readiness 时才可用。

仓库内 helper 可用 `make build-nvidia-binding` 构建到 `bin/tgsrl-nvidia-binding`。Scheduler
通过 `-nvidia-binding-helper` 指定二进制，通过 `-nvidia-binding-state` 指定持久化文件；
未指定时状态文件位于 `-state-dir` 下。bootstrap 可在显式配置 `TGSRL_NVIDIA_MPS_PID_DIR`、
且容器能看到唯一宿主 MPS server PID 时维护 generation-fenced `<sandbox>.pid`；普通 Pod
默认不具备该 PID 可见性。PID 记录仅保留为节点集成诊断信息，不授权在线 `set_share`；
binding receipt 只表示该步骤已落盘，Provider commit 仍由事务执行器完成。
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

## HAMi vGPU 启用条件

Operator 的 `-gpu-profile` 支持逗号分隔的有序候选，例如
`kubernetes-dra,hami-vgpu`。选择以每个 concrete Binding 为单位：

- DRA 只接受整数份额，并要求所有 UUID 出现在 typed ResourceSlice inventory；
- HAMi 只接受单物理 GPU、`accelerator_units ∈ (0,1]`，并要求 UUID 出现在 typed Node
  registration inventory；
- 候选不匹配时尝试下一个 profile；没有候选能兑现时直接拒绝；
- `nvidia-device-plugin` 与旧 `volcano-hami` 仍是数量型兼容 profile；当 Binding 已携带
  Scheduler 选择的 UUID 而 profile 无法证明 exact placement 时不会被选中。

HAMi 编译器将份额向上取整为 `1..100` 的 core/memory 百分比，请求一张物理 GPU，并写入
`nvidia.com/use-gpuuuid`、`nvidia.com/vgpu-mode=hami-core`、显式 `hami-scheduler` 和
编译期预期份额 metadata。Operator 观察 Pod 的 `hami.io/vgpu-devices-allocated`；只有实际
UUID、memory MiB、core 百分比与 Binding/请求完全一致才视为已分配。
bootstrap 还会从容器内再次核对可见设备。部署和故障定位见
[HAMi vGPU 接入指南](../guides/hami.md)。

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

`configs/gates/e1-e8.json` 将正式验证拆成 Full GPU identity、MIG identity、吞吐/VUG、
staleness/ESS、共置干扰、动作代价、故障恢复和跨节点收敛八个独立实验。
`python scripts/gate-tools.py campaign-evaluate` 会校验每个报告的 experiment identity、最低
GPU evidence、执行模式、设备 profile、节点数、实际动作、故障注入/恢复配对和精确设备
身份。缺少报告为 `NOT_RUN`，不合规证据为 `INVALID`；尚未从权威需求文档确认的数值阈值
标记为 `calibration_required`，在补齐前保持 `BLOCKED`，不能形成发布 PASS。
目标环境可用 `campaign-run --driver <path> --require-pass` 按顺序执行八项实验；仓库 executor
拥有 baseline/variant、迭代、动作、故障与失败 cleanup 顺序，runner 锁定每项
gate-tools/campaign/gate/scenario/executor/driver digest，拒绝旧 commit、目录逃逸和不完整 named evidence。
环境 driver 只负责目标 Kubernetes/GPU 原子操作并返回结构化观察。E1 的 `make gpu-smoke`
只执行单项并对 E1 自身失败/无效/规则失败返回非零；其成功不代表缺失的 E2–E8 已通过。也可用
`campaign-ingest E<n> --report <report.json>` 单独导入已有
证据，再以 `campaign-evaluate --require-pass` 作为发布门禁。Hardware Validation workflow
的 `e1-e8-run` 模式执行 campaign；`e1-e8-evaluate` 模式只消费名为
`gate-e1-e8-evidence` 的已采集 artifact，不在普通 GitHub runner 上伪造硬件执行。
仓库提供的 `scripts/tgsrl-hardware-environment-driver` 已用 fake Gateway/kubectl 验证 E1/E2
原子链、持久 receipt 和 identity fail-closed；E1 还已取得真实 `GPU_SINGLE_NODE` 证据，见
[E1 单节点 Full GPU 验证记录](../validation/e1-full-gpu-2026-09-12.md)。部署者需要从
`configs/hardware/environment.example.json` 创建本地配置，并提供真实 workload 模板、trace
导出命令与必要的 observation/fault hook；未配置的自适应动作会直接拒绝。

当前工作树另提供两个单卡实验入口：

- `make gpu-e6-action-cost` 使用 DRA Full GPU、代表性 CUDA tensor workload 和 scoped
  worker registry，独立测量 `pause/checkpoint/offload/reload/resume`，并要求真实 tensor
  checkpoint 与显存释放/恢复；
- `make gpu-e5-interference` 使用两个固定 `40%` HAMi worker，对比错峰 baseline 与并发
  variant，由 orchestrator 从逐 worker 吞吐推导 `interference_ratio`。该 `E5-STATIC`
  pilot 不声明在线动态份额或优先级能力，正式 E5 状态不因此改变。

2026-09-16 的单 A10 实机运行已为两项入口生成 `GPU_SINGLE_NODE`、`simulated=false`
报告：E6 与 E5-STATIC 的独立执行报告均为 `PASSED`。E6 测得 pause/checkpoint/reload
为 `45.042/704.530/260.322 ms`；E5-STATIC 测得 baseline 重叠 `0 ms`、variant 重叠
`8570.544 ms`、干扰率 `0.229306`。由于数值规则仍为 `calibration_required`，两个
campaign gate 都保持 `BLOCKED`；正式 E5 仍为 `NOT_RUN`。详见
[E6 与 E5-STATIC 单 A10 实验记录](../validation/e5-e6-single-a10-2026-09-16.md)。

独立 `A10-FULL` readiness 使用同一 executor，但 worker action 直接走 current
JobRunBundle 中的 scoped registry token，且只允许一个 exact sandbox/generation。它要求
offload/resume 动作前后存在权威 allocator bytes；`make gpu-a10-readiness` 再组合 H2 和
DRA/E1 恢复回归。`68f5aea` 的四项聚合结果均为 `PASSED`；后续提交没有自己的 GPU report 时
仍为 `NOT_RUN`，不能继承该结论。

HAMi 使用独立 H1/H2 campaign，不修改正式 E1–E8 顺序。`make gpu-prepare-hami` 在专用
Minikube 上安装锁定 chart；`make gpu-hami-smoke` 验证单 worker 身份/份额，
`make gpu-hami-concurrency-smoke` 验证两个 worker 共享同一物理 UUID、各自 `40%` 份额并
产生真实执行重叠。真实 A10 H1/H2 均已通过，见
[H1 单节点 HAMi 分数 GPU 验证记录](../validation/h1-hami-vgpu-2026-09-12.md)与
[H2 单节点 HAMi 双 worker 并发验证记录](../validation/h2-hami-concurrency-2026-09-13.md)。
H2 仍不等于 OOM 隔离、公平性、动态份额、干扰上界或训练收益成立。

Console 的链路追踪读取 Runtime 的真实 `TraceEvent`，支持 Trainer、Request、Executor、Worker
四类轨道、统一时间标尺、长耗时与空泡提示，以及基于 `request_id` 的跨轨关联。veRL adapter
会透传 duration、batch size、GPU active time、span 和执行身份；未上报持续时间的事件只显示
真实时间点。静态演示模式仅用于验证交互和布局，不作为真实性能证据。

## 明确不支持

- 直接把 Gateway、gRPC 或 Prometheus 端点暴露到公网或不可信共享网络；
- 内置 TLS、身份认证、授权、多租户隔离、CORS 策略、限流或密钥管理；
- 在未验证仓库内 binding/runtime/MIG helper 与目标环境时执行 GPU 分配、MIG/MPS 管理或
  Runtime lifecycle；HAMi H2 只覆盖双 worker 同卡并发，不能外推到 OOM 隔离、公平性或动态份额；
- 把 bootstrap/full-stack CPU 进程验证解释为真实 Kubernetes Pod、DRA/CDI、真实 veRL 包或 GPU 证据；
- 把 `nvidia-smi` 设备发现、Mock 行为或单元测试解释为真实 GPU 调度与执行验证；
- 依靠 fake backend 在进程重启后恢复 workload 对象；
- 跨服务原子事务、自动故障转移、HA 或灾备；
- 把 Mock、Synthetic、Replay 或内存模式结果作为真实 GPU 吞吐、利用率、收敛质量、
  成本、多节点稳定性或 wall-clock 收益的依据。

如需共享或长期运行服务，应在外部补充 TLS、身份认证、授权、审计、网络策略、限流、
可靠的进程管理、备份与恢复演练，并使用适合目标环境的 HTTP server 和基础设施 Driver。
