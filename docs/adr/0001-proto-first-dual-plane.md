# ADR-0001：Proto-first 跨系统契约

- 状态：Accepted
- 日期：2026-08-27

## 背景

Job、Runtime、Experiment、Scheduler 和 Operator 跨 Go 与 Python 协作。如果每个
服务维护独立的数据对象，字段、枚举和默认值容易漂移；如果资源决策同步依赖算法
实现，Runtime 故障也会阻塞调度路径。

## 决策

三个产品系统共享 `tgsrl.v1` Protobuf 作为 wire contract 的唯一权威：

- **Job & Product Control** 使用 Job、Run、Operation 和 JobEvent 契约；
- **Runtime, Trace & Experiments** 使用 ExecutionContract、RuntimeManifest、Trace、
  Replay、Experiment 和 SchedulingIntent 契约；
- **Scheduling & Infrastructure Control** 使用 ClusterSnapshot、PlacementPlan、
  ActionResult、DecisionRecord 和 SandboxEvent 契约；
- gRPC 承担服务间调用，HTTP Gateway 负责 Proto JSON 转换；
- Scheduler 只读取版本化 Snapshot，提交 Plan 时复核 Snapshot revision、Intent version
  与 generation；
- Scheduler 不需要理解 PPO、GRPO 或具体框架实现，厂商能力由 Adapter 与 Provider
  边界表达。

## 契约规则

- Go/Python 类型及 gRPC stub 由同一组 Proto 生成；
- 枚举保留 `UNKNOWN = 0`；
- 删除字段时 reserve number 和 name；
- 时间使用 Protobuf Timestamp/Duration，跨进程时间使用 UTC；
- ID、排序、幂等键和序列化必须确定；
- 不允许手写同名 DTO 绕开协议校验。

## 结果

任务、运行时和基础设施可以独立演进，并通过显式版本、TTL、cursor、generation 和
幂等约束协作。代价是每次 schema 变更都必须同步生成客户端并验证跨语言兼容性。

完整关系见[三系统架构](../design/system-design.md)。
