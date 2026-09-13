# 配置、持久化与恢复

本指南面向可信本机或隔离网络中的部署与故障恢复。示例命令均从项目根目录执行。
服务不提供内置 TLS、身份认证、跨服务事务、HA 或自动灾备；共享部署必须在外部补齐
这些能力。

## 配置入口

默认配置从 `compatibility/manifests/cpu-mock.yaml` 开始。Manifest 是配置图的根，
引用并统一校验以下内容：

```text
compatibility manifest
├── runtime BOM
├── compatibility profile
├── capability set
├── policy bundle
├── representative scenario
├── patch ledger
└── compatibility matrix

representative scenario
├── compatibility manifest
├── resource profile
├── capability set
└── policy bundle
```

加载器会检查 schema 版本、引用是否存在，以及 provider source、data kind、算法、
rollout mode、生命周期动作和策略是否相容。引用路径优先相对配置根解析；为避免歧义，
建议始终使用相对配置根的路径。配置在进程启动时读取，不支持热更新。

Scheduler 和 `tgsrl-runtime` 服务入口每次启动都会加载配置图；两者默认使用
项目根目录和 `compatibility/manifests/cpu-mock.yaml`。Runtime 会把配置投影应用到
manifest 校验、编译和准备阶段，填充或约束 framework、
execution backend、trainer、rollout engine、profile、data kind、rollout mode、policy、
desired units 和 capability；与显式 manifest 冲突的值会被拒绝。

### `TGSRL_CONFIG_*` 覆盖

Go 与 Python 加载器都支持以下路径覆盖：

| 环境变量 | 覆盖内容 |
|---|---|
| `TGSRL_CONFIG_MANIFEST_PATH` | 根 Manifest |
| `TGSRL_CONFIG_BOM_PATH` | Runtime BOM |
| `TGSRL_CONFIG_PROFILE_PATH` | 兼容性 Profile |
| `TGSRL_CONFIG_CAPABILITIES_PATH` | Capability set |
| `TGSRL_CONFIG_POLICY_PATH` | Policy bundle |
| `TGSRL_CONFIG_SCENARIO_PATH` | Representative scenario |

路径选择优先级是：环境变量覆盖 > `--manifest`/`-manifest` > Manifest 中的引用 >
内置默认 Manifest。所有被替换后的引用仍会接受完整配置图校验。

Scheduler 还支持以下启动覆盖：

| 环境变量 | 默认来源或值 | 约束 |
|---|---|---|
| `TGSRL_CONFIG_PROVIDER_KIND` | Profile 的 provider kind | 接受 Mock 与 NVIDIA provider 标识；默认 NVIDIA LocalDriver 只支持设备发现 |
| `TGSRL_CONFIG_STRATEGY` | Policy 的 selection strategy | `stable-first-fit`、`score-first`、`binpack` 或 `trace-aware` |
| `TGSRL_CONFIG_TOP_K` | Policy 的 `top_k` | 正整数 |
| `TGSRL_CONFIG_FAST_INTERVAL` | `25ms` | 正的 Go duration |
| `TGSRL_CONFIG_MEDIUM_INTERVAL` | `100ms` | 正的 Go duration |
| `TGSRL_CONFIG_SLOW_INTERVAL` | `250ms` | 正的 Go duration |

任意同前缀变量都可能出现在脱敏后的配置诊断中，但只有上表及路径表中的名称会影响
行为。不要依靠变量名脱敏来保护秘密，也不要把凭据放入这些变量。

## 服务参数

### Scheduler

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-listen` | `127.0.0.1:50051` | gRPC 监听地址 |
| `-config-root` | `.` | 配置图根目录 |
| `-manifest` | 空 | Manifest 覆盖；空值使用默认 Manifest |
| `-fallback` | 空 | 覆盖 Policy，只接受 `noop`/`no_op` 或 `static` |
| `-state-dir` | `.tmp/scheduler-state` | 持久化根目录 |
| `-metrics-listen` | `127.0.0.1:9090` | Prometheus 地址；空字符串关闭 |
| `-nvidia-driver-v2` | `false` | 启用可执行 NVIDIA Driver v2；未启用时 LocalDriver 只发现设备 |
| `-nvidia-partition-mode` | `auto` | 生产 CLI 支持 `auto`、`full`、`mig`；`mps` 因不能在线改变已有 client 而 fail closed |
| `-nvidia-dry-run` | `false` | 只生成/校验 NVIDIA 命令计划，不形成硬件通过证据 |
| `-nvidia-command-timeout` | `15s` | 单次 NVIDIA helper 命令超时 |
| `-nvidia-binding-helper` | `tgsrl-nvidia-binding` | binding helper 路径 |
| `-nvidia-binding-state` | `<state-dir>/nvidia-binding.json` | binding/receipt 状态 |
| `-nvidia-mps-pid-dir` | 空 | `<sandbox>.pid` MPS server 身份目录 |
| `-nvidia-runtime-helper` | `tgsrl-nvidia-runtime` | managed-worker runtime helper 路径 |
| `-nvidia-runtime-state` | `<state-dir>/nvidia-runtime.json` | worker/runtime receipt 状态 |
| `-nvidia-mig-helper` | `tgsrl-nvidia-mig` | MIG lifecycle helper 路径；`auto`/`mig` 模式必需 |
| `-worker-registry-listen` | 空 | managed-worker registry HTTP 监听地址；可供本地 process 或 NVIDIA/Kubernetes workload 使用 |
| `-worker-registry-runtime-target` | 空 | registry 发布 SandboxEvent 使用的 Runtime gRPC target |
| `-worker-registry-signing-key-file` | 空 | 至少 32 bytes 的 HMAC 主签名 key；也可用 `TGSRL_WORKER_REGISTRY_SIGNING_KEY` |
| `-worker-registry-state` | `<state-dir>/worker-registry.json`；NVIDIA v2 默认兼容旧 runtime state | managed-worker 注册与 lifecycle receipt 的绝对状态路径 |

`-fallback` 的优先级高于 Policy；provider、strategy、top-k 和三个调度周期则可由
对应的 `TGSRL_CONFIG_*` 变量覆盖。fast/medium/slow 队列本身是进程内状态；重启时
Scheduler 会从持久化的最新 Intent 重建待处理工作，并送回同一权威调度路径。三档
分别是 keyed queue，不是三个独立 Scheduler；同一 execution/stage 的重复触发会在各自
队列内合并，并在有效 Intent 生命周期内周期重排，从而让 idle 等时间阈值在没有新事件时
仍能生效。tick interval 只控制处理节奏，不是完成时限或 SLA；同档处理尚未结束时，
重叠 tick 会被跳过。

Scheduler 的 Prometheus 输出为指标名添加 `tgsrl_` 前缀。三档 loop 可通过
`tgsrl_ticks_started_fast|medium|slow`、`tgsrl_ticks_completed_fast|medium|slow`、
`tgsrl_ticks_skipped_fast|medium|slow` 和
`tgsrl_tick_latency_fast|medium|slow_{count,sum}` 观察启动、完成、重叠跳过和处理耗时。

### Job Controller

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-listen` | `127.0.0.1:50061` | JobControl gRPC 监听地址 |
| `-runtime-target` | `127.0.0.1:50071` | Runtime gRPC 目标 |
| `-state-dir` | 用户 cache 下的 `tgs-rl/job-controller` | snapshot 与 journal 目录 |
| `-reconcile-timeout` | `30s` | 启动时调和全部 RUNNING Operation 的总超时；必须为正值 |

### Runtime / Experiment

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--bind` | `[::]:50071` | 同时提供 Runtime 与 Experiment gRPC 服务 |
| `--state-db` | `.cache/tgsrl/runtime.db` | SQLite 文件；显式 `:memory:` 才使用临时内存库 |
| `--scheduler-target` | `127.0.0.1:50051` | Scheduler gRPC 目标 |
| `--operator-target` | `127.0.0.1:50081` | Operator lifecycle control gRPC 目标 |
| `--job-control-target` | `127.0.0.1:50061` | Runtime 观察态回报目标 |
| `--config-root` | `.` | 配置图根目录 |
| `--manifest` | 空 | Manifest 覆盖；空值使用默认 CPU Mock Manifest |
| `--worker-registry-signing-key-file` | 空 | 验证 Scheduler 转发 worker Trace 的 HMAC key 文件；也可用同名环境变量 |

本地运行时应显式使用 `--bind 127.0.0.1:50071`，避免模块默认的 `[::]` 暴露到
所有网络接口。

### Gateway

| 环境变量 | 默认值 |
|---|---|
| `TGSRL_GATEWAY_BACKEND_MODE` | `grpc` |
| `TGSRL_GATEWAY_JOB_CONTROL_TARGET` | `127.0.0.1:50061` |
| `TGSRL_GATEWAY_SCHEDULER_TARGET` | `127.0.0.1:50051` |
| `TGSRL_GATEWAY_RUNTIME_TARGET` | `127.0.0.1:50071` |
| `TGSRL_GATEWAY_EXPERIMENT_TARGET` | `127.0.0.1:50071` |
| `TGSRL_GATEWAY_GRPC_TIMEOUT_SECONDS` | `2.0` |
| `TGSRL_GATEWAY_COMMAND_TIMEOUT_SECONDS` | `30.0` |
| `TGSRL_GATEWAY_HEALTH_TIMEOUT_SECONDS` | `0.5` |
| `TGSRL_GATEWAY_DECISIONS_TIMEOUT_SECONDS` | `0.25` |
| `TGSRL_RUNTIME_OPERATOR_TIMEOUT_SECONDS` | `30.0` |

`serve` 子命令的 `--backend-mode`、`--job-control-target`、
`--scheduler-target`、`--runtime-target` 和 `--experiment-target` 会覆盖相应环境变量。
HTTP 的 `--host`、`--port` 默认是 `127.0.0.1`、`8080`。

**组合式本地服务把 Runtime 与 Experiment 都指向 `127.0.0.1:50071`。** Runtime
进程在该端口同时注册两个服务；`50081` 属于 Operator 的 lifecycle control 服务。例如：

```bash
export TGSRL_GATEWAY_RUNTIME_TARGET=127.0.0.1:50071
export TGSRL_GATEWAY_EXPERIMENT_TARGET=127.0.0.1:50071
```

### Operator

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-mode` | `kubernetes` | `fake` 使用进程内 backend；`process` 启动本机真实子进程；`kubernetes` 连接 API Server |
| `-controller` | `true` | 是否消费 Scheduler Decision；关闭时仍提供 backend lifecycle control RPC |
| `-scheduler` | `127.0.0.1:50051` | Scheduler gRPC target |
| `-control` | `127.0.0.1:50061` | Job Controller gRPC target |
| `-runtime` | `127.0.0.1:50071` | Runtime gRPC target |
| `-listen` | `127.0.0.1:50081` | Runtime backend lifecycle control gRPC 地址 |
| `-namespace` | `default` | `JobRunBundle` namespace |
| `-cursor-dir` | 系统临时目录 | Decision cursor 目录 |
| `-kubeconfig` | 空 | 显式 kubeconfig；仅 `kubernetes` 模式使用 |
| `-gpu-profile` | `none` | 逗号分隔的有序兑现候选；支持 `none`、`kubernetes-dra`、`hami-vgpu`、`nvidia-device-plugin` 与旧 `volcano-hami` |
| `-runtime-class-name` | 空 | 引用已有 RuntimeClass |
| `-runtime-class-handler` | 空 | 创建 RuntimeClass 时使用的 handler |
| `-runtime-class-create` | `false` | 是否创建 RuntimeClass；启用时需要额外 cluster-scoped 权限 |
| `-node-selector` | 空 | 可重复的 `key=value` Pod node selector |
| `-worker-bootstrap` | `false` | 包装 workload 并自动注册真实子进程；process 模式自动启用，`kubernetes-dra`/`hami-vgpu` 要求显式启用 |
| `-worker-bootstrap-image` | 空 | bootstrap installer 的不可变 digest 镜像 |
| `-worker-registry-url` | 空 | workload 可访问的 Scheduler registry URL |
| `-worker-registry-signing-key-file` | 空 | 与 Scheduler 相同的主签名 key，仅供 Operator 派生 scoped token |
| `-worker-verify-device-identities` | `false` | 注册前核对容器可见设备 UUID；DRA claim 物化时强制执行 |
| `-worker-host-network` | `false` | 专用单节点 GPU smoke 的 Pod host network；必须启用 bootstrap，control listener 使用动态端口以支持多 worker |
| `-worker-bootstrap-binary` | `tgsrl-worker-bootstrap` | process backend 使用的本机 bootstrap 可执行文件 |
| `-process-state-dir` | `<cursor-dir>/processes` | process backend 的状态与 worker log 目录 |

Kubernetes 凭据解析顺序为：显式 `-kubeconfig`、`KUBECONFIG`、用户默认
`.kube/config`；没有可用文件路径时使用集群内 ServiceAccount 配置。kubeconfig
读取器只解析 API server、静态 bearer token、内嵌 CA data 和 namespace，不执行
exec/auth-provider 插件，也不合并多文件 `KUBECONFIG`。

`deploy/helm/tgsrl` 为 Scheduler、Runtime/Experiment、Job Controller、Operator、Gateway
和 Console 建立完整控制面 Deployment、Service、健康探针与持久卷，并默认用 NetworkPolicy
限制控制面入口。`deploy/helm/operator` 与原生 YAML 仍支持只部署 Operator。Operator chart
传入 Scheduler、Job Controller 和 Runtime 的 service address，并默认把
`/var/lib/tgsrl-operator` 挂载到 `ReadWriteOnce` PVC。默认 RBAC 将 `JobRunBundle`、`Workload`、`Job` 和
`ResourceClaimTemplate` 权限限制在目标 namespace 的 `Role` / `RoleBinding`，Pod 生成的
`ResourceClaim` 只授予 read 权限，CRD 的 `spec`
使用单一 `bundle` envelope；Node、RuntimeClass、DeviceClass 和 ResourceSlice discovery 使用只读
`list` ClusterRole。Helm 可覆盖依赖地址、现有 PVC、storage class、容量和
保留策略；仅当显式启用 `runtimeClassCreate=true` 时，才追加最小 cluster-scoped
RuntimeClass 写权限。全栈 chart 包含 `JobRunBundle` CRD，但不部署 Kueue 或 GPU/DRA
控制器。使用 Kubernetes backend 前，部署者必须提供这些外部依赖，并确认目标集群
支持通过 API discovery 选择 Kueue `v1beta2`/`v1beta1` 与 DRA
`resource.k8s.io/v1`/`v1beta2`/`v1beta1`。`kubernetes-dra` 不是通用 driver 模式：它固定
使用 NVIDIA `gpu.nvidia.com` driver，根据 ResourceSlice 的 typed metadata 为 Full GPU 选择
`gpu.nvidia.com` DeviceClass、为 MIG 选择 `mig.nvidia.com` DeviceClass，并要求 `type`、`uuid`
以及 MIG 的 `profile`、`parentUUID`。Operator 将 Scheduler `device_ids` 编译为 CEL selector，
并在 allocation 后通过最新 ResourceSlice 回读 UUID 与 DeviceClass 一致性。

`hami-vgpu` 使用 Node `hami.io/node-nvidia-register` 建立 typed physical-GPU inventory，
将单卡分数份额编译为 HAMi 官方 NVIDIA 资源/注解，并从 Pod
`hami.io/vgpu-devices-allocated` 回读实际 UUID。配置多个候选时，Compiler 按每个 Binding
依次尝试；例如 `kubernetes-dra,hami-vgpu` 可让整数 DRA 设备和单卡 HAMi 分数任务共存。
传统 Device Plugin 与旧 `volcano-hami` 仍只有数量语义。详见 [HAMi 接入指南](hami.md)。

启用 bootstrap 时，Helm 的 `controller.workerBootstrap.registrySigningKeySecret` 必须指向已有
Secret，默认 key 为 `signing-key`。同一主 key 还必须以文件或 Secret 挂载给 Scheduler。Pod 不会
引用这个 Secret；Operator 只把按 binding 派生的 scoped token 写入 Pod 环境。Operator 和
Scheduler 的状态目录均包含敏感 worker control material。helper 会把目录和文件限制为
`0700`/`0600`；部署层仍须保证私有挂载和受限备份。
bootstrap installer 固定安装到 `/var/run/tgsrl-bootstrap`，不会覆盖 workload 镜像常用的
`/opt/tgsrl`。只有存在 workload OCI artifact 时才会注入 Python 模块依赖检查；依赖由
workload 镜像提供，不由 Runtime 控制面提供。

## 本地数据目录

建议为完整本地栈显式使用同一个私有父目录：

```bash
umask 077
mkdir -p .cache/tgsrl/scheduler-state .cache/tgsrl/job-controller .cache/tgsrl/operator
```

| 组件 | 建议参数 | 实际数据 |
|---|---|---|
| Scheduler | `-state-dir .cache/tgsrl/scheduler-state` | `scheduler/state.checkpoint` 与 `scheduler/state.journal` |
| Runtime / Experiment | `--state-db .cache/tgsrl/runtime.db` | SQLite 主文件，以及运行期间可能存在的 `-wal`、`-shm` 文件 |
| Job Controller | `-state-dir .cache/tgsrl/job-controller` | `snapshot.gob` 与 `journal.gob` |
| Operator | `-cursor-dir .cache/tgsrl/operator` | `decision-cursor.json`、`delivery.json`、`backend-controls.json` |
| Gateway / Console | 无 | 不保存本地业务状态 |

未显式指定时，Scheduler 使用 `.tmp/scheduler-state`；Job Controller 使用操作系统
用户缓存目录；Operator 使用系统临时目录；Runtime 使用 `.cache/tgsrl/runtime.db`。
要运行临时 Runtime，可显式传 `--state-db :memory:`。不要让多个进程实例同时写同一
状态目录或 SQLite 文件。备份前先停止写入进程；备份 SQLite 时使用 SQLite
在线备份机制，或在进程停止后连同 `-wal`、`-shm` 一起处理。

## 各组件的恢复边界

| 组件 | 重启时会恢复 | 不会自动完成的事项 |
|---|---|---|
| Scheduler | Cluster snapshot、最新 Intent、Decision 与 action result、Decision cursor、provider sandbox projection/event cursor、reservation；启动时调和未完成 reservation 并重新排队恢复出的 Intent | Provider 进程内执行对象本身；无法由 Provider 确认完成的 plan 会失败收敛，不会盲目重放外部副作用 |
| Runtime / Experiment | 分页回填最新 Intent、Trace batch、manifest、runtime unit、sandbox、全局且可稀疏的 runtime event sequence、generation、intent-version、Start 发布进度、checkpoint、Replay、Experiment、Replay START 已完成的 Scheduler step、component status 及必要计数 | 仅补投未确认的 Start Intent；checkpoint 不是外部进程镜像；不是跨服务 HA/灾备 |
| Job Controller | Job、Run、Operation、幂等记录、事件历史与事件序号；启动时扫描 RUNNING Operation，优先按 Runtime 收敛状态完成，未派发且有幂等键时安全重放 | Runtime 不可达时保留启动失败以便重试；已派发但结果不确定或缺少幂等键时标记 `RECONCILIATION_REQUIRED`，不猜测成功 |
| Operator | Decision cursor、未完成 delivery、持久化 observation registration、已发布 transition，以及 lifecycle control 幂等记录；Kubernetes 对象由 API Server 保存 | fake backend 对象不持久化；跨服务没有分布式事务 |
| Gateway / Console | 无本地状态 | 重启后从后端重新读取 |

需要特别注意以下边界：

1. **Scheduler 恢复会重建工作，但不会盲目重放副作用。** 启动先恢复 Decision/cursor，
   再由统一 Plan Executor 对新式 transaction 调用 `ResumeAll`，通过 Provider 的事务 receipt
   与 `ReconcilePlanTransaction` 收敛 prepare/apply/commit/abort；没有 transaction record 的旧式
   reservation 才走 `RecoverInFlightPlans` / `ReconcilePlan` 兼容路径。保存 checkpoint 后，
   Scheduler 按稳定顺序重新排队 Intent。已有足够 active allocation 的 Intent 会直接跳过；
   无法确认完成的 plan 会按失败或 degraded 收敛。该机制没有跨进程事务保证，运维时仍应
   核对 Decision、transaction、reservation 和 Provider 实际状态。
2. **Runtime 的恢复会遍历持久化分页。** Runtime 会保留全局 event sequence（包括空洞）、
   generation、Intent version 与 durable Start 发布水位，并仅补投持久 outbox 中未确认的
   Intent。Runtime checkpoint 记录完成元数据，不是可直接恢复外部训练
   进程的完整镜像。Replay START 还会逐步持久化已完成的 Scheduler preview；使用相同
   幂等键重试时可从下一步继续。输入 digest 或 ordinal 不一致时会 fail closed，不会复用
   不匹配的进度。
3. **Operator 先持久化观察交接，再推进 Decision cursor。** `delivery.json` 保存进行中的
   delivery 与可独立恢复的 observation registrations；`backend-controls.json` 保存 lifecycle
   幂等与 revision。进程重启后会恢复注册并继续观察，Kubernetes 模式还会读取现有
   `JobRunBundle` 补建缺失注册。更新时新 generation 对象全部写入后才清理旧对象并提交
   marker；terminal observation 持久化后才按 generation 清理对象。fake backend 对象随进程
   退出而消失，所以保留这些文件仍不能恢复其内存对象；跨服务也没有原子事务。容器部署
   必须为整个 cursor 目录配置持久卷。

状态损坏或读写失败时，不要删除文件后直接继续。先停止相关服务并保留副本；Scheduler
会在恢复失败时拒绝启动，持久化写失败后也会阻止后续资源变更。

## 重启与恢复顺序

推荐按依赖顺序启动：

1. **Scheduler**：使用原 `-state-dir`，确认进程成功完成恢复并开始监听。
2. **Runtime / Experiment**：使用原 `--state-db`，确认 manifest、unit、sandbox、event、
   Replay 和 Experiment 均可查询。
3. **Job Controller**：使用原 `-state-dir`；启动前会查询 Runtime 并调和 RUNNING Operation。
   已收敛状态直接补写终态，尚未派发且有稳定幂等键的请求按原身份重放；已派发但结果未知
   或缺少幂等键的记录会进入 `RECONCILIATION_REQUIRED`，需要人工核对后再发新命令。
4. **Operator**：使用原 `-cursor-dir` 并监听原 lifecycle control 地址；确认三个上游都
   可用后再恢复消费和 observation registration。fake backend 需要单独处理已丢失对象；
   Kubernetes backend 应核对 API Server 中对象、registration 与 cursor 是否一致。
5. **Gateway**：确认 Runtime 与 Experiment target 在组合式本地部署中均为 `50071`。
6. **Console**：最后启动并通过 Gateway 重新读取状态。

停机时采用逆序：先停止 Console/Gateway 和 Operator，再停止 Job Controller、Runtime、
Scheduler。该顺序是运维建议，不是系统提供的事务性编排保证。

## 安全要求

gRPC、HTTP 和 Prometheus 端点没有 TLS、认证、授权、租户隔离或限流，只适合
可信本地环境。请遵守以下原则：

- 显式绑定 `127.0.0.1`，不要直接暴露到公网或共享网络；跨主机使用时应放在受控代理
  和网络策略之后。
- 状态文件没有加密，默认文件权限也不应视为秘密存储。使用私有目录和限制性
  `umask`，不要写入令牌、密码或其他凭据。
- 新创建的核心状态文件使用 `0600`，状态目录使用 `0700`；已有目录权限、SQLite 旁路文件、
  挂载卷 ACL 和备份介质仍需部署者核查，文件权限不能替代加密或密钥管理。
- `/metrics` 没有访问控制，指标也可能泄露运行信息。
- worker registry、bootstrap control endpoint 与 gRPC 一样没有内建 TLS；只绑定受控网络，
  并使用 NetworkPolicy 或外部 mTLS/TLS 代理隔离。
- Manifest 及其引用文件属于可信输入；只加载经过审查、权限受控的配置图。
- Gateway 的 `memory` backend 只提供无持久化演示数据；需要连接完整调用链时使用 `grpc`。
