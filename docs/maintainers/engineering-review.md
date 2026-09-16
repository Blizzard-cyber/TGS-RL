# 工程审查指南

本页定义维护者在评审 TGS-RL 变更时需要检查的工程边界。它不是某次发布记录，也不用于
替代安全审计、生产可用性认证或硬件验证报告。

## 审查范围

审查应覆盖第一方代码和入口调用链：

- Job Controller、Runtime、Scheduler、Provider、Operator、Gateway 和 Console；
- bootstrap、worker registry、hardware driver、Gate tools 和 evidence ingest；
- 打包、部署、配置图、生成协议、数据库迁移和公开文档。

第三方系统通过锁文件、配置合同和验证入口约束；不在常规工程审查中逐行审计外部源码。
硬件结论必须引用 `docs/validation/` 中的记录或新的目标环境报告。

## 严重级别

| 级别 | 定义 | 处理要求 |
|---|---|---|
| P0 | 数据损坏、错误提交成功、证据伪造、凭据泄漏、无法恢复的状态破坏 | 立即修复并补回归，修复前不发布 |
| P1 | 关键路径失败、身份/代际/回执不一致、部署不可用、测试实际未覆盖源码 | 发布前修复，必须说明回归入口 |
| P2 | 可维护性、错误信息、文档不一致、边界说明不清 | 同批修复或登记明确后续项 |
| P3 | 局部清理、命名、重复说明和非关键可读性问题 | 不阻塞发布，避免扩大修改面 |

审查结论要区分“代码缺陷”“部署环境问题”“缺少目标硬件证据”。缺少证据不能写成已经失败；
同样，CPU/Mock/CI 通过也不能写成 GPU、MIG、多节点或训练收益已经通过。

## 必查项

| 主题 | 检查点 |
|---|---|
| 状态权威 | desired state、observed state、Decision、receipt 和 replay cursor 不互相代替 |
| 身份与代际 | Job/Run/Sandbox/Binding/worker generation 必须在跨服务调用中可核对 |
| 幂等与恢复 | 请求 ID、operation key、journal/checkpoint、SQLite 迁移和 unknown result 处理可重放 |
| GPU 证据 | Scheduler UUID、allocation UUID、worker 可见 UUID、DeviceClass 和 trace identity 一致 |
| 自适应动作 | `set_share`、`set_priority`、`rebind`、fault hook 没有权威 readback 时 fail closed |
| 打包边界 | wheel 包含迁移和许可证；Docker context 不带本地硬件配置、凭据、数据库或缓存 |
| 前端行为 | Console 不吞后端错误；Trace duration 只来自事件字段；资源页不伪装成 allocation proof |
| 文档 | 公开文档描述稳定合同；进度、排期和本地交接材料保存在 ignored 本地文件 |

## 验证分层

| 层 | 入口 | 能证明 | 不能替代 |
|---|---|---|---|
| 单元与组件 | `make test`、语言子目录测试 | 状态机、协议、序列化和局部逻辑 | 集成链路 |
| 静态与治理 | `make lint`、`make check-governance`、`make check-repository` | 格式、生成物、仓库内容和合同一致性 | 运行时行为 |
| 产品进程 | `make product-e2e`、`make gate-cpu-integration` | 多服务、真实子进程、Trace、pause/resume 控制链 | Kubernetes、GPU、训练收益 |
| 前端 | `make test-console`、`make test-console-browser` | Console 数据层、路由、布局和错误状态 | 后端正确性 |
| 部署渲染 | `make check-deploy`、`make gpu-render-helm-values` | Helm/manifest 结构、值约束和镜像引用 | 目标集群可用性 |
| 硬件验证 | `make gpu-smoke`、`make gpu-a10-readiness`、E1-E8 campaign | 指定 commit、硬件、拓扑和 workload 下的真实 evidence | 其他硬件或后续代码 |

硬件 evidence 的公开摘要放在 [验证记录](../validation/README.md)。原始 Trace、日志、集群快照、
临时报告和审计 JSON 保存在 ignored 的 `.cache/` 或本地证据目录中。

## 审查输出

评审结论应包含：

1. 发现的问题，按 P0-P3 排序，附文件和可复现路径。
2. 修复或接受风险的理由。
3. 执行过的验证命令，以及没有执行的原因。
4. 对公开文档、配置合同、验证记录和本地进度材料的影响。

不要把一次本地路径、短期排期、个人机器状态或论文进度写进公开维护文档。需要保留项目推进信息时，
写入 ignored 的本地进度文件。
