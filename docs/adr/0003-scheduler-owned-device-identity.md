# ADR-0003：Scheduler 持有设备身份，NVIDIA DRA 强制兑现

- 状态：Accepted
- 日期：2026-08-31

## 背景

Scheduler 的 reservation、rebind、MIG 和 NVIDIA receipt 均以 `Binding.device_ids` 中的
GPU/MIG UUID 为事务身份。如果 Operator 只向 Kubernetes 请求设备数量，Pod 可能获得另一块
设备，调度账本、Provider 状态和训练进程就会分叉。传统 Device Plugin 和 HAMi 的扩展资源
请求只能表达数量，不能兑现 Scheduler 已选择的 UUID。

Kubernetes DRA 可以用 driver-specific CEL selector 约束设备，并在 ResourceClaim status 中
返回 `driver/pool/device`。NVIDIA DRA ResourceSlice 同时发布 `gpu.nvidia.com` driver 和
`uuid` 属性，因此能够建立可验证的身份映射。

## 决策

- Scheduler 是设备选择与 reservation 的唯一权威，`Binding.device_ids` 使用 NVIDIA
  GPU/MIG UUID。
- Operator 的 `kubernetes-dra` profile 固定要求 NVIDIA DRA driver `gpu.nvidia.com`，并根据
  typed inventory 为 Full GPU 选择 `gpu.nvidia.com` DeviceClass、为 MIG 选择
  `mig.nvidia.com` DeviceClass。
- 每个 binding 的 ResourceClaim 使用 `uuid` CEL selector，只允许绑定中列出的 UUID。
- Operator capability discovery 必须同时发现对应 DeviceClass、最新 ResourceSlice generation
  以及包含 type、driver、pool、device、profile、parent UUID 的唯一设备记录；仅发现 DRA API
  或 DeviceClass 不足以开放该 profile。
- 同一个 binding 不能混用 Full GPU 与 MIG DeviceClass；未知类型和不完整 MIG 元数据直接拒绝。
- ResourceClaim allocation 后，Operator 用 `driver/pool/device` 在最新 ResourceSlice 中反查
  UUID。实际集合与 Binding 不完全一致时 fail closed，不发布 `BOUND` 或 `RUNNING`。
- 经过验证的 allocation UUID 写回 SandboxEvent，保持 Runtime 与 Scheduler 的观测一致。
- Device Plugin 与 HAMi 仍可被探测，但当前不能兑现 UUID，因此 Operator 不将它们作为可执行
  profile。
- 在 NVIDIA DRA sharing configuration 接线前，只接受整数个完整 GPU/MIG 设备；分数 share
  必须拒绝。

## 结果

这项决定避免了两个资源权威，也保留现有 Scheduler 事务和 MIG 重配置语义。代价是 Kubernetes
精确 GPU 路径明确依赖 NVIDIA DRA schema，不能再表述为通用 DRA 支持；ResourceSlice 的
top-level 和 v1beta1 `basic` attributes 会合并，相同字段冲突时拒绝；ResourceSlice 读取需要
最小集群级 `list` 权限。未来若支持其他 DRA driver，必须新增明确的 identity adapter 和
readback 规则，不能复用 NVIDIA 属性名称。

本决策只关闭资源身份从 Scheduler 到 Pod 的一致性。后续 workload bootstrap 已实现
PID/control endpoint 注册和 generation-fenced MPS PID 发布；
veRL 0.9 callback adapter 也已接到 trainer/worker-group/checkpoint-manager 公共接口。真实
Kubernetes PID namespace、NVIDIA MPS 和分布式 veRL/GPU 行为仍属于后续环境验证。
