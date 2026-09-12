# TGS-RL 文档

TGS-RL 由 **Job & Product Control**、**Runtime, Trace & Experiments** 和
**Scheduling & Infrastructure Control** 三个系统组成。第一次使用时，建议先通过
Docker Compose 启动单机栈，再根据需要选择 Console、CLI、Python SDK 或 HTTP API。

## 开始使用

- [快速上手](getting-started.md)：安装、启动、健康检查、提交首个 Job，以及停止和恢复。
- [单机 GPU 全链路 Smoke](guides/gpu-smoke.md)：空白 Ubuntu GPU 主机的依赖安装、
  Minikube/Kueue/DRA、镜像、前后端启动、E1 证据与故障定位。
- [HAMi vGPU 接入](guides/hami.md)：让未启用 MIG 的设备按算力/显存份额参与调度，
  并保持 Scheduler UUID、Pod allocation 与 worker 可见设备一致。
- [API、CLI 与 Console](guides/api-and-console.md)：调用 HTTP API，使用 CLI/SDK，配置 Console。
- [支持范围与限制](reference/current-capabilities.md)：选择本地、外部框架、NVIDIA 或 Kubernetes
  方案前需要满足的条件。
- [贡献指南](../CONTRIBUTING.md)：开发环境、变更边界与提交前检查。
- [安全策略](../SECURITY.md)：漏洞报告入口与部署安全边界。
- [Apache License 2.0](../LICENSE)：使用、修改与分发本项目的许可证条款。

## 配置各组件

- [Python Runtime](guides/python-runtime.md)：执行契约、Adapter、Trace、DAG、Replay、
  Experiment 与 Intent。
- [Scheduler](guides/scheduler-service.md)：调度 RPC、策略、Provider、指标、决策与恢复。
- [Operator](guides/operator.md)：决策消费、对象编译、调和、Kubernetes 要求与恢复。
- [配置、持久化与恢复](guides/configuration-and-recovery.md)：配置图、启动参数、状态目录、
  备份和重启顺序。

## 理解系统

- [项目设计、代码导读与工程 Review](project-design-and-code-review.md)：单文档理解项目目标、
  架构、端到端控制链、状态权威、源码入口、恢复、安全、部署、测试和真实环境准入。
- [系统架构](design/system-design.md)：组件职责、端到端控制流、状态权威和部署边界。
- [加速器 Provider 扩展设计](design/accelerator-extension.md)：当前 NVIDIA 支持边界，以及未来
  新增 NPU/TPU/其他厂商 Provider 与 Operator adapter 的稳定接口。
- [E1–E8 硬件验证 Campaign](design/gate-e1-e8.md)：实验矩阵、证据合同与 fail-closed 发布准入。
- [E1 单节点 Full GPU 验证记录](validation/e1-full-gpu-2026-09-12.md)：真实 A10、DRA、
  managed worker、CUDA、Trace 与 cleanup 的脱敏验收摘要。
- [Managed-worker bootstrap](design/managed-worker-bootstrap.md)：Pod 内进程监管、身份、注册、
  生命周期控制、安全边界与真实环境验证要求。
- [源码导读与维护边界](maintainers/code-walkthrough.md)：按真实调用链理解状态、事务、
  恢复、NVIDIA/veRL 执行和 Gate 证据。
- [维护者开发指南](maintainers/development.md)：测试分层、性能 benchmark、完整门禁和提交边界。
- [仓库卫生与发布内容](maintainers/repository-hygiene.md)：哪些文件必须提交、哪些必须保持本地。
- [架构决策记录](adr/README.md)：协议与资源抽象背后的设计约束。

Gateway 运行后可通过 `GET /openapi.json` 获取 OpenAPI 3.1 文档。也可以直接查看
[`api/openapi.json`](../api/openapi.json)。

文档以当前代码和机器可读配置为准，不把历史 commit、某次 CI run 或本机实验结果当作永久
能力声明。提交前运行 `make check-docs`，验证本地链接、Make 目标和 `tgsrl` CLI 命令引用。
