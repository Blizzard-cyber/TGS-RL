# TGS-RL 文档

TGS-RL 由 **Job & Product Control**、**Runtime, Trace & Experiments** 和
**Scheduling & Infrastructure Control** 三个系统组成。第一次使用时，建议先通过
Docker Compose 启动单机栈，再根据需要选择 Console、CLI、Python SDK 或 HTTP API。

## 开始使用

- [快速上手](getting-started.md)：安装、启动、健康检查、提交首个 Job，以及停止和恢复。
- [API、CLI 与 Console](guides/api-and-console.md)：调用 HTTP API，使用 CLI/SDK，配置 Console。
- [支持范围与限制](reference/current-capabilities.md)：选择本地、外部框架、NVIDIA 或 Kubernetes
  方案前需要满足的条件。

## 配置各组件

- [Python Runtime](guides/python-runtime.md)：执行契约、Adapter、Trace、DAG、Replay、
  Experiment 与 Intent。
- [Scheduler](guides/scheduler-service.md)：调度 RPC、策略、Provider、指标、决策与恢复。
- [Operator](guides/operator.md)：决策消费、对象编译、调和、Kubernetes 要求与恢复。
- [配置、持久化与恢复](guides/configuration-and-recovery.md)：配置图、启动参数、状态目录、
  备份和重启顺序。

## 理解系统

- [系统架构](design/system-design.md)：组件职责、端到端控制流、状态权威和部署边界。
- [源码导读与维护边界](maintainers/code-walkthrough.md)：按真实调用链理解状态、事务、
  恢复、NVIDIA/veRL 执行和 Gate 证据。
- [架构决策记录](adr/README.md)：协议与资源抽象背后的设计约束。

Gateway 运行后可通过 `GET /openapi.json` 获取 OpenAPI 3.1 文档。也可以直接查看
[`api/openapi.json`](../api/openapi.json)。
