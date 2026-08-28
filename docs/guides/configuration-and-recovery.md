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

`-fallback` 的优先级高于 Policy；provider、strategy、top-k 和三个调度周期则可由
对应的 `TGSRL_CONFIG_*` 变量覆盖。fast/medium/slow 队列本身是进程内状态；重启时
Scheduler 会从持久化的最新 Intent 重建待处理工作，并送回同一权威调度路径。

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
| `TGSRL_GATEWAY_HEALTH_TIMEOUT_SECONDS` | `0.5` |
| `TGSRL_GATEWAY_DECISIONS_TIMEOUT_SECONDS` | `0.25` |

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
| `-mode` | `kubernetes` | `fake` 使用进程内 backend；`kubernetes` 连接 API Server |
| `-scheduler` | `127.0.0.1:50051` | Scheduler gRPC target |
| `-control` | `127.0.0.1:50061` | Job Controller gRPC target |
| `-runtime` | `127.0.0.1:50071` | Runtime gRPC target |
| `-listen` | `127.0.0.1:50081` | Runtime backend lifecycle control gRPC 地址 |
| `-namespace` | `default` | `JobRunBundle` namespace |
| `-cursor-dir` | 系统临时目录 | Decision cursor 目录 |
| `-kubeconfig` | 空 | 显式 kubeconfig；仅 `kubernetes` 模式使用 |

Kubernetes 凭据解析顺序为：显式 `-kubeconfig`、`KUBECONFIG`、用户默认
`.kube/config`；没有可用文件路径时使用集群内 ServiceAccount 配置。kubeconfig
读取器只解析 API server、静态 bearer token、内嵌 CA data 和 namespace，不执行
exec/auth-provider 插件，也不合并多文件 `KUBECONFIG`。

随附 Kubernetes YAML 与 Helm chart 为 Operator 建立 `50081` Service，传入 Scheduler、
Job Controller 和 Runtime 的 service address，并默认把 `/var/lib/tgsrl-operator` 挂载到
`ReadWriteOnce` PVC。默认 RBAC 将 `JobRunBundle`、`Workload`、`Job` 和
`ResourceClaim` 权限限制在目标 namespace 的 `Role` / `RoleBinding`，CRD 的 `spec`
使用单一 `bundle` envelope。Helm 可覆盖依赖地址、现有 PVC、storage class、容量和
保留策略；仅当显式启用 `runtimeClassCreate=true` 时，才追加最小 cluster-scoped
RuntimeClass 写权限。这些工件不部署 Scheduler、Job Controller、Runtime、Kueue 或
GPU/DRA 控制器。使用 Kubernetes backend 前，部署者必须提供这些依赖，并确认目标集群
支持生成对象使用的 Kueue `v1beta1` 与 Kubernetes 1.32 风格 DRA
`resource.k8s.io/v1beta1` API。

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
| Scheduler | Cluster snapshot、最新 Intent、Decision 与 action result、Decision cursor、reservation；启动时调和未完成 reservation 并重新排队恢复出的 Intent | Provider 进程内状态本身；无法由 Provider 确认完成的 plan 会失败收敛，不会盲目重放外部副作用 |
| Runtime / Experiment | 分页回填最新 Intent、Trace batch、manifest、runtime unit、sandbox、全局且可稀疏的 runtime event sequence、generation、intent-version、Start 发布进度、checkpoint、Replay、Experiment、component status 及必要计数 | 仅补投未确认的 Start Intent；checkpoint 不是外部进程镜像；不是跨服务 HA/灾备 |
| Job Controller | Job、Run、Operation、幂等记录、事件历史与事件序号 | 不会扫描并重新执行重启前未完成的 Operation |
| Operator | Decision cursor、未完成 delivery、持久化 observation registration、已发布 transition，以及 lifecycle control 幂等记录；Kubernetes 对象由 API Server 保存 | fake backend 对象不持久化；跨服务没有分布式事务 |
| Gateway / Console | 无本地状态 | 重启后从后端重新读取 |

需要特别注意以下边界：

1. **Scheduler 恢复会重建工作，但不会盲目重放副作用。** 启动先恢复 Decision/cursor，
   再通过完整 Provider 的 `RecoverInFlightPlans` / `ReconcilePlan` 调和未完成 reservation，
   保存收敛后的 checkpoint，最后按稳定顺序重新排队恢复出的 Intent。已有足够 active
   allocation 的 Intent 会直接跳过；无法确认完成的 plan 会按失败收敛。该机制没有
   跨进程事务保证，运维时仍应核对 Decision、reservation 和 Provider 实际状态。
2. **Runtime 的恢复会遍历持久化分页。** Runtime 会保留全局 event sequence（包括空洞）、
   generation、Intent version 与 durable Start 发布水位，并仅补投持久 outbox 中未确认的
   Intent。Runtime checkpoint 记录完成元数据，不是可直接恢复外部训练
   进程的完整镜像。
3. **Operator 先持久化观察交接，再推进 Decision cursor。** `delivery.json` 保存进行中的
   delivery 与可独立恢复的 observation registrations；`backend-controls.json` 保存 lifecycle
   幂等与 revision。进程重启后会恢复注册并继续观察，Kubernetes 模式还会读取现有
   `JobRunBundle` 补建缺失注册。fake backend 对象随进程退出而消失，所以保留这些文件
   仍不能恢复其内存对象；跨服务也没有原子事务。容器部署必须为整个 cursor 目录配置
   持久卷。

状态损坏或读写失败时，不要删除文件后直接继续。先停止相关服务并保留副本；Scheduler
会在恢复失败时拒绝启动，持久化写失败后也会阻止后续资源变更。

## 重启与恢复顺序

推荐按依赖顺序启动：

1. **Scheduler**：使用原 `-state-dir`，确认进程成功完成恢复并开始监听。
2. **Runtime / Experiment**：使用原 `--state-db`，确认 manifest、unit、sandbox、event、
   Replay 和 Experiment 均可查询。
3. **Job Controller**：使用原 `-state-dir`，检查 Job、Run、Operation 与事件；人工处理
   重启前处于中间态的 Operation。
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
- `/metrics` 没有访问控制，指标也可能泄露运行信息。
- Manifest 及其引用文件属于可信输入；只加载经过审查、权限受控的配置图。
- Gateway 的 `memory` backend 只提供无持久化演示数据；需要连接完整调用链时使用 `grpc`。
