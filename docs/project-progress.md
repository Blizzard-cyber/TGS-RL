# 毕业设计推进状态

更新基线：`68f5aeaf4e601af01c8fa8d7e33cd3f16f94f898`（2026-09-15）。

## 当前结论

TGS-RL 已完成**系统设计、核心实现、工程回归、单节点 GPU 接线和实验前工程验收**。
项目已经从“实现系统”阶段进入“执行正式毕业实验并形成结论”阶段。

当前不能写成“毕设已完成”，因为正式 E1–E8 实验矩阵只完成了 E1；E2 缺少可用 MIG
硬件，E3–E7 尚未使用正式 workload 和环境 hook 运行，E8 还需要至少两个 GPU 节点。
完整 veRL trainer、实验数据分析、论文结果章节和答辩材料也没有仓库内完成证据。

2026-09-15 的当前工作树已补齐 E6 五步 lifecycle 实验入口，以及单卡可执行的
`E5-STATIC` 静态份额共置干扰 pilot。E6 使用代表性 CUDA tensor workload；E5-STATIC
在同样两个 `40%` worker 上比较错峰执行与并发执行。二者尚未在 A10 上产生新报告，
因此仍不能标为通过，也不能替代完整模型训练结果。

## 里程碑

| 里程碑 | 状态 | 已完成证据 | 仍需工作 |
|---|---|---|---|
| 问题定义与系统架构 | **完成** | Proto-first、Dual-plane、多权威状态、VUG 目标、ADR 和系统设计已落库 | 论文中压缩为问题、假设和设计选择 |
| 核心控制面 | **完成** | Job Controller、Runtime、Scheduler、Provider、Operator、Gateway、Console 均可运行 | 不再扩张功能面，实验发现真实阻断时再修 |
| 可靠性与恢复 | **完成于当前工程范围** | 幂等、generation fence、receipt、补偿、SQLite/文件恢复、真实进程故障 readiness 已验证 | HA、灾备和跨服务事务不属于当前完成范围 |
| GPU 兑现与进程接线 | **完成于单节点 NVIDIA** | DRA Full GPU E1、HAMi H1/H2、真实 CUDA、Trace、checkpoint/offload/reload/resume 已验证 | MIG、MPS、新厂商和多节点仍未验证 |
| Kubernetes/Helm 交付 | **完成于专用单节点 A10** | 六服务不可变镜像、install + upgrade 两轮 smoke、同 PVC 重跑、严格依赖健康门禁已通过 | 其他 CNI、StorageClass、GPU 型号和生产安全需独立验证 |
| 实验平台与证据合同 | **完成** | E1–E8 campaign、scenario、runner、driver、证据哈希、校准入口和 fail-closed 规则已实现；E6 与 E5-STATIC 已有专用入口 | 在 A10 上执行 E6/E5-STATIC；继续为 E3/E4/E5/E7 接入正式 workload 或 hook |
| 正式毕业实验 | **进行中** | E1 已取得真实 `GPU_SINGLE_NODE` 证据；H1/H2 和 A10 readiness 是工程辅助证据 | E2–E8 尚未形成正式通过结论 |
| 结果分析与论文 | **尚无完成证据** | 架构、工程验收和限制可直接作为系统章节素材 | 完成实验数据、统计分析、图表、结果讨论、论文与答辩材料 |

## 正式实验矩阵

| 实验 | 当前状态 | 已具备 | 下一步 |
|---|---|---|---|
| E1 Full GPU 精确设备执行 | **PASSED** | 单节点 A10、DRA exact UUID、managed worker、真实 CUDA/Trace、清理 | 作为正式基线保留，不用新 smoke 覆盖旧原始证据 |
| E2 MIG 精确设备与 rebind | **BLOCKED / NOT_RUN** | typed MIG inventory、DeviceClass、identity/rebind 合同和 fake 回归 | 获取支持 MIG 的目标 GPU；不在 A10 上强改拓扑 |
| E3 吞吐与 VUG | **NOT_RUN** | 指标、baseline/variant runner、throughput 最低规则 | 接入正式训练 workload，重复 baseline/variant，标定 VUG 阈值 |
| E4 policy lag / staleness / ESS | **NOT_RUN** | 指标与故障/动作协议 | 实现并审查 staleness-pressure 与 `set_share` 环境 hook，标定两条阈值 |
| E5 共置干扰隔离 | **NOT_RUN** | `E5-STATIC` 已实现同一双 worker、40% HAMi 配额下的串行基线与并发变量；尚无新 A10 报告 | 先运行静态共置 pilot；正式 E5 仍需动态 share/priority 权威动作与回读 |
| E6 lifecycle 动作代价 | **NOT_RUN** | 已实现独立 `pause/checkpoint/offload/reload/resume` scoped action、真实 tensor checkpoint 和显存变化证据；尚无新 A10 报告 | 在 A10 上运行并评审三项延迟阈值 |
| E7 故障与事务恢复 | **NOT_RUN** | CPU/真实进程已验证 crash、response loss、partial failure；GPU fault schema 已有 | 在专用 GPU 资源实现 worker-exit/control-response-loss hook，测恢复时间 |
| E8 多节点稳定性与收敛 | **BLOCKED / NOT_RUN** | 多节点证据合同和收敛指标已定义 | 准备至少两台 GPU 节点、node-loss hook 和完整训练 workload |

H1/H2、A10-FULL、`E5-STATIC` 和两轮 Helm smoke 不计作正式 E2–E8 的替代结果。
其中 `E5-STATIC` 是当前单卡可执行的静态份额 pilot；正式 E5 仍保留动态 share/priority
控制要求。它们用于证明设备兑现、同卡共享、生命周期控制、恢复和部署链足够稳定。

## 进度解释

若把毕设拆为“系统工程”和“实验研究”两条线：

- **系统工程线：已完成当前设计范围。**
- **实验基础设施线：已完成。**
- **正式实验线：8 项中 1 项通过，2 项受硬件/拓扑阻塞，其余 5 项待运行。**
- **论文结果线：尚未形成可提交的完整结果。**

因此当前阶段不是继续堆控制面功能，而是冻结工程基线、准备正式 workload 与实验资源。
当前先执行已经准备好的 E6 与 E5-STATIC，再按 E3、E4、正式 E5、E7、E8 推进；
E2 在获得 MIG 硬件后插入。

## 下一阶段

1. 冻结 `68f5aea` 为实验前工程基线；非实验阻断问题不再扩张系统范围。
2. 提交当前实验实现并构建新的不可变 workload/控制面镜像。
3. 在单张 A10 上先运行 E6，采集五步 lifecycle 延迟和显存释放/恢复证据。
4. 切换 HAMi 后运行 E5-STATIC，比较同样两个 40% worker 的串行基线与并发变量。
5. 接入完整 veRL workload，明确模型、数据、seed、batch、节点和镜像 digest，再推进 E3。
6. 为 E4、正式 E5 和 E7 实现并审查环境 hook；正式 E5 不由 E5-STATIC 自动替代。
7. 单独准备 MIG 和多节点环境执行 E2、E8；资源未具备时保持 `BLOCKED/NOT_RUN`。
8. 固化原始证据、统计方法和图表，完成论文实验设计、结果分析和局限性章节。
