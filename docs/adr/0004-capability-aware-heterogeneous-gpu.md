# ADR-0004：按设备能力发布资源，按 Binding 选择兑现方式

- 状态：Accepted
- 日期：2026-09-12

## 背景

集群可能同时包含多个厂商、型号和硬件代次。MIG 只由部分设备能力集支持，而且设备即使具备
该能力，也只有在 MIG mode 已启用并存在实例时才应按切片调度。若 Scheduler 使用一个全局
`full|mps|mig` 开关描述整个集群，会出现两类错误：

- 因某张卡不支持 MIG 而把它排除，浪费可用整卡或共享容量；
- 同时发布同一物理卡的整卡和 MIG 子设备，造成容量重复计算。

另一方面，Scheduler 选择的是资源身份与份额，而 Kubernetes 集群可能用 NVIDIA DRA 或
HAMi 兑现该选择。兑现后端不应反过来成为第二个设备选择权威。

本 ADR 的“异构”指当前 NVIDIA 设备在型号、代次和分区能力上的差异。当前仓库没有实现其他
加速器厂商；跨厂商扩展由通用 `Device`、`CompleteResourceProvider` 和 composition-root
注册表承载，真正接入时再增加独立实现，不在本 ADR 中放置占位 Provider。

## 决策

### Scheduler 按设备发布

NVIDIA Driver v2 增加 `auto` partition mode，并作为 Scheduler CLI 默认值：

- `MIGEnabled=false`：发布物理 GPU UUID，`partition_mode=full`；
- `MIGEnabled=true`：只发布已发现的 MIG UUID，不发布父卡整卡容量；
- MIG helper 或 MIG inventory 不可用时，只影响对应 MIG 设备；其他非 MIG GPU 继续可用；
- Full GPU 设备不声明 MIG 专属 capability 或 `rebind/recreate`；
- `auto` 不启动 MPS；动态 MPS 仍是显式运维选择；
- 显式 `full` 也排除已启用 MIG mode 的物理卡。

### Operator 按 Binding 选择

`-gpu-profile` 从单值改为逗号分隔的有序候选。对每个 concrete Binding，Operator 结合
`device_ids`、`accelerator_units` 与当前集群 inventory 选择首个可精确兑现的 profile：

```text
kubernetes-dra,hami-vgpu
        │              │
        │              └─ 单物理 GPU，份额 (0,1]，HAMi UUID inventory 命中
        └─ 整数 GPU/MIG 数量，DRA typed inventory 全部命中
```

一个 PlacementPlan 中的不同 Binding 可以选择不同 profile。没有候选能兑现时拒绝编译，
不丢弃 UUID，也不回退为仅按数量分配。

### HAMi 是执行适配器，不是第二个控制面

TGS-RL 只复用 HAMi 的公开资源和注解协议：

- Node inventory：`hami.io/node-nvidia-register`；
- 目标 UUID：`nvidia.com/use-gpuuuid`；
- vGPU 模式：`nvidia.com/vgpu-mode=hami-core`；
- 资源：`nvidia.com/gpu`、`nvidia.com/gpucores`、
  `nvidia.com/gpumem-percentage`；
- allocation readback：`hami.io/vgpu-devices-allocated`。

Scheduler 仍持有 Binding、reservation、generation 和 Decision。HAMi 负责 Kubernetes 内部
调度与设备注入，Operator 回读结果并与 Binding 对账。

## 结果

- 不支持或未启用 MIG 的 GPU 不再被错误排除，可继续整卡调度，并可在安装 HAMi/MPS 后共享；
- 支持并启用 MIG 的 GPU 不会与父卡整卡重复计量；
- 混合集群不需要选择一个全局最小能力；
- DRA 与 HAMi 都遵循 exact identity 和 fail-closed 原则；
- Console 可从 Scheduler snapshot 展示设备级能力，但最终基础设施兑现仍以 Operator
  readback 与 worker observation 为准。

代价是 Operator 需要维护 DRA ResourceSlice 和 HAMi Node/Pod 两套 identity adapter，
部署方也必须自行安装并验证相应集群组件。当前 HAMi 路径已在 NVIDIA A10 上通过 H1
单 worker 份额兑现与 H2 双 worker 同卡并发验证；OOM 隔离、公平性、动态份额和性能收益
仍需独立实验。这项 ADR 不改变 E1 Full GPU DRA 与正式 E1–E8 的证据边界。
