# ADR-0002：Mock-first ResourceProvider

- 状态：Accepted
- 日期：2026-08-27

## 背景

不同硬件和资源管理系统提供的动作、粒度、延迟、状态保留与故障模式并不等价。
在没有真实环境证据时，把模拟行为当成硬件能力会污染调度逻辑和性能结论。
另一方面，通用协议、状态机、幂等和补偿应当能够在普通 CPU 环境中持续验证。

## 决策

采用 Mock-first、capability-gated 的 Provider 边界：

- `MockResourceProvider` 只声明 `source=mock`；
- Scheduler 只能选择 Provider 明确声明的 capability 和 action；
- 未声明或未测量的能力默认不可用；
- Plan 使用厂商中立 action，Provider 负责前置条件、执行、结果归一和补偿；
- Mock 可注入延迟、action failure、partial failure、rollback failure 和迟到事件；
- 新硬件 Provider 不得要求 Scheduler 添加厂商名称或设备型号分支。

## 动作契约

每个 action 携带 plan/action ID、目标、generation、Snapshot revision、capability、
safe-point 要求、deadline、idempotency key 和 rollback。

- 相同幂等键与内容返回已有结果，不重复产生副作用；
- 不满足 capability 或前置条件时显式失败；
- 部分失败时按逆序补偿已成功步骤；
- rollback 声明的 action type、target 和 restore binding 必须匹配 forward action；
- 补偿使用 target/field scoped before-image，不覆盖其他资源状态；
- rollback 失败时结果和未恢复状态必须一致；
- L4 replacement 创建新 generation，旧 generation 事件不能覆盖新状态。

## 结果

CPU 环境可以验证调度与资源动作的通用正确性，但 Mock 行为不能证明任何实际
硬件支持、动作成本、隔离强度或性能收益。接入真实 Provider 时，只有经过实际
探测和回归的能力才能进入 live `CapabilitySet`。

代码中已有 NVIDIA Provider 自有状态与 Driver 执行、回滚、调和边界；默认 LocalDriver
通过 `nvidia-smi` 做本机发现，但不声明或执行基础设施动作。可注入 Driver 的 action 流程
测试不改变 Mock-first 的验证原则，也不能作为真实 GPU 集成证据。当前支持边界见
[当前能力与限制](../reference/current-capabilities.md)。
