# 开发与 GPU 测试交接

更新日期：2026-09-16。此文件按约定提交，用于两台机器通过 GitHub 协作；不是产品设计文档。
正式发布前移除本文件与 `handoff/`，长期内容保留在 `docs/`。

## 当前周期：工程基线已完成，进入毕业实验

代码、模块、执行接线、失败/取消/重启/清理和单节点 GPU 部署验收已经完成。
后续主线是正式 workload、E2–E8 实验、阈值标定和论文结果，不再无边界扩张控制面功能。
统一进度口径见 `docs/project-progress.md`。

- **开发机**：macOS，无 NVIDIA GPU。负责实现、review、CPU/Mock、真实子进程、文档与治理。
- **测试机**：Linux + NVIDIA。拉取干净 commit，验证 CUDA/设备兑现和真实故障，提交脱敏结果。
- 开发机不拉取/构建 Docker 镜像；A10 测试机只操作专用验证资源，并保留旧数据库和原始硬件证据。

## 当前工作树与 CI

- 最新硬件实验实现基线：`3c9147c83065434eef1f28a0d4731306fa3f90ba`，分支 `main`；
  实验前工程验收基线仍为 `68f5aeaf4e601af01c8fa8d7e33cd3f16f94f898`。
- E6 报告绑定 `701e11b13ed9a14359593e7bd73c80608017de95`，E5-STATIC 报告绑定
  `3c9147c83065434eef1f28a0d4731306fa3f90ba`；两次运行时工作树均干净。
- `68f5aea` 的 GitHub CI 10/10 全部 `success`，包括 Product E2E、Full-stack CPU Gate、
  Process E2E、race/staticcheck、部署与兼容性治理。
- 提交无自动 `Co-authored-by` trailer。新实验仍必须记录实际 `git rev-parse HEAD`，
  不能把本基线的证据外推到后续代码。

## 本批改动

| 内容 | 结果 |
|---|---|
| Runtime 发布包 | 补 SQL migrations package-data，缺失即失败，初始化异常关闭 SQLite 连接 |
| 安装回归 | 干净 sdist → wheel → 隔离安装 → 数据库写入/重开；不依赖 editable 源码 |
| Docker context | 最后排除本机硬件配置、嵌套 credentials/私钥、PID/socket；保留 example |
| hardware driver | 等 bind 时检查 Start Operation 终态，避免已失败仍等完整 timeout |
| MPS | share 四舍五入为 0 时执行前拒绝，不发送无效控制命令 |
| Console | typecheck 检查 app/node；总览/任务列表不再吞掉 Run 查询失败 |
| Helm | 使用 Helm 4 的 rollback-on-failure，检查 install/upgrade 参数可用性 |
| 文档检查 | 未暂存删除文档不再导致扫描崩溃，失效链接仍报错 |
| 可读性 | 失败上下文重命名、错误 registry 注释清理 |
| 开源文档 | 首页精简，设计与源码导读分层，独立部署/审查记录，CPU 完整任务示例与 smoke 共用 |
| scoped lifecycle | registry/driver 直接执行 exact worker offload/resume，保留 Scheduler bind 权威 |
| A10 lifecycle | resident CUDA tensor、allocator bytes、checkpoint/offload/reload/resume 硬判定 |
| A10 聚合 | 故障 readiness → Full lifecycle → H2 HAMi → DRA 恢复 → E1，生成总汇摘要 |
| 故障 readiness | 真实进程/响应丢失/重启/partial failure 可重复报告；GPU fault 保持 NOT_RUN |
| Helm 实装 | 全部不可变控制面镜像、values、install/upgrade、健康检查和证据索引 |
| cleanup 竞态 | preconditioned delete 仅把精确 NotFound 视为幂等成功，Conflict 继续 fail closed |
| observation 回收 | 启动时原子剪枝、运行时回收已删除 Bundle 的 registration，关闭 watcher/ledger 泄漏 |
| Helm 依赖就绪 | Gateway 必须确认四个 gRPC 依赖全部 serving，HTTP 200 + degraded 不再通过 |

详细发现与本批最终验证见 [工程审查](docs/maintainers/engineering-review.md)。
稳定架构见 [系统设计](docs/design/system-design.md)，使用入口见 [快速上手](docs/getting-started.md)。

## 仍需推进的项目项

1. 工程基线与单节点 A10 验收已完成；详见
   `docs/validation/a10-readiness-2026-09-14.md` 和 `docs/project-progress.md`。
2. 接入完整或代表性的 veRL workload，冻结模型、数据、seed、batch、镜像和节点条件。
3. `make engineering-fault-readiness` 已覆盖 worker crash、响应丢失、组件重启和 partial failure；
   GPU Pod/容量/节点网络故障仍为 NOT_RUN。
4. 正式 E1、E6 已通过；E6 在 `13d0f34` 独立确认中以
   `54.479/695.910/272.408 ms` 通过 `60/900/400 ms` 门槛。E5-STATIC 已冻结
   `32%` 干扰率门槛并等待独立确认，正式 E5
   仍需动态 share/priority 权威动作。
5. E2 等待可用 MIG GPU，E8 等待至少两个 GPU 节点；不为过进度强改 A10 拓扑。
6. MPS 在线 `set_share` 已因 NVIDIA 语义不成立而撤下；未来需 checkpoint/recreate 新 client 的
   节点级 adapter。A10 无 MIG，不为 E2 改拓扑。
7. 新硬件证据必须包含 cluster/namespace UID、环境/模板/hook 摘要、rendered Job 和聚合摘要。

当前实验入口：

- `make gpu-e6-action-cost`：DRA Full GPU，使用代表性 CUDA tensor workload 独立执行并计时
  `pause/checkpoint/offload/reload/resume`，验证真实 tensor checkpoint 与 allocator bytes。
- `make gpu-e5-interference`：HAMi 双 worker 各 `40%`，baseline 错峰、variant 共置，
  从逐 worker 吞吐推导静态份额干扰率；它不替代正式 E5 动态控制。

吞吐/VUG、staleness/ESS、干扰、公平性、动作代价、恢复与收敛属于正式实验；长期存储、HA
与入口安全属于超出当前毕设工程基线的生产化工作，不混作已完成能力。

## 本批本机验证

- Python Runtime/Storage/Governance 520 passed，Gateway API 39 passed；Go 全模块/race、lint/staticcheck 通过。
- Console 73 unit tests + build/typecheck/lint；17 项 mock 浏览器路由/响应式检查通过。
- 产品 E2E 重启/恢复通过；生成协议、docs、部署、Compose config、SBOM/公开内容/仓库检查通过。
- 干净源码副本新建独立 venv、重新构建 Go，六服务 smoke 四次操作成功、13 条 Trace、无 allocation 残留；
  六个真实 HTTP 页面、Trace 四轨、390px、Run 查询故障/恢复已在浏览器验证，测试服务已停止。
- CPU process Gate 8/8 执行；四源 Trace 和 worker pause/resume 确认；variant scheduling P95 60.611 ms。
  吞吐比 0.7034，包含控制暂停开销；只证明正吞吐和控制链，不证明性能收益，GPU 状态仍 NOT_RUN。
- 原始本机证据：`.cache/tgsrl/review-clean-7s7wkc0m/`、`.cache/tgsrl/review-20260913-process/`。
- 本轮 CPU process Gate 证据：`.cache/tgsrl/final-engineering-process/`，三条工程规则通过，GPU 状态保持 `NOT_RUN`。
- 本轮故障 readiness：`.cache/tgsrl/engineering-fault-readiness/report.json`，七类真实进程/组件检查通过；三类 GPU fault 为 `NOT_RUN`。
- 本机 Python 为 3.12.13，BOM 为 3.12.14；Docker/GPU 实装在测试机执行，不把开发机结果
  冒充硬件证据。

## 已有硬件证据（不可改写来源）

| 日期 | 源代码 | 场景 | 结果与记录 |
|---|---|---|---|
| 2026-09-12 | `d033566` | E1 Full GPU | PASSED，8/8 执行、Scheduler/DRA/worker UUID 一致、CUDA/Trace/清理；`docs/validation/e1-full-gpu-2026-09-12.md` |
| 2026-09-12 | `9c65d46` | H1 HAMi | PASSED，单 worker 40% core、9211 MiB、UUID 一致；`docs/validation/h1-hami-vgpu-2026-09-12.md` |
| 2026-09-12 | `9c65d46` | E1 恢复回归 | HAMi 卸载并恢复 Device Plugin 后通过 |
| 2026-09-13 | `a45a432` | H2 HAMi | PASSED，双 worker 同卡各 40%/9211 MiB，测量期正重叠；`docs/validation/h2-hami-concurrency-2026-09-13.md` |
| 2026-09-13 | `a45a432` | E1 恢复回归 | Device Plugin 1/1、单 GPU 容量恢复、无 HAMi 注解或 workload 残留 |
| 2026-09-14 | `0295132` | 中间 A10 验收批次 | PASSED，但后续最终 SHA 又关闭 cleanup/registration/依赖就绪问题；仅保留为历史证据 |
| 2026-09-14 | `0295132` 镜像 + 工作树修复 | 中间 Helm 批次 | PASSED，315 项证据；已由 `68f5aea` 干净基线两轮结果取代 |
| 2026-09-14 | `68f5aea` | 最终 A10 总体验收 | PASSED：四组件同一干净 SHA 全部通过；MIG、多节点和 GPU 破坏性故障保持 NOT_RUN |
| 2026-09-14 | `68f5aea` | 六服务 Helm install + upgrade | PASSED：同 8 个 PVC、revision 2、两轮各 315 项、630/630 哈希匹配、6/6 Ready |
| 2026-09-16 | `701e11b` | E6 lifecycle 动作代价 pilot | 执行报告 PASSED；三轮最大值 46.907/711.072/301.490 ms，冻结阈值 60/900/400 ms，随后由 `13d0f34` 独立确认 |
| 2026-09-16 | `3c9147c` | E5-STATIC HAMi 干扰 pilot | 执行报告 PASSED：三轮干扰率 0.222769/0.218472/0.246678，冻结门槛 0.32；正式 E5 仍 NOT_RUN |
| 2026-09-16 | `13d0f34` | E6 独立确认 | PASSED：54.479/695.910/272.408 ms 均低于 60/900/400 ms；三轮五类 receipt、显存释放/恢复与资源清理通过 |

这些是单节点限定证据，不证明 OOM 隔离、公平性、动态份额、完整训练、MIG/MPS、多节点或性能收益。
E6 已正式通过；E5-STATIC 已冻结 32% 门槛、待独立确认，正式
E2–E5/E7–E8 仍未完成。

测试机既有环境快照：Ubuntu 22.04 x86_64、NVIDIA A10、580.178.04 driver、CUDA driver API 13.0、
Docker 29.8.0、Compose 5.5.1、Toolkit 1.20.0、Kubernetes 1.35.1、Kueue 0.19.2、NFD 0.18.3、
DRA 0.5.0、Minikube 1.38.1。**这是历史快照，复测先检查当前状态。** root 专用 smoke 环境需要
显式 `TGSRL_MINIKUBE_ALLOW_ROOT=1`；可复用主机优先非 root。此卡无 MIG，不要为了 E2 改造拓扑。

## 测试机如何执行

1. clone/pull 指定 clean commit，记录 `git rev-parse HEAD` 与 `git status --short`。
2. 按 [GPU Smoke](docs/guides/gpu-smoke.md) 安装依赖/准备集群/镜像/凭据；已有资源先检查再复用。
   网络受限可 `export TGSRL_NETWORK_PROFILE=cn`。仅在明确需要时安装或构建，不重建已健康集群。
3. 新批次使用 `make gpu-a10-readiness`；脚本会归档旧 A10/H2/E1 输出并执行完整顺序。
4. 检查 `readiness-summary.json`、子 report、显存 before/after、真实 CUDA Trace 与资源回收。
5. Helm 使用 `make gpu-render-helm-values`、`make gpu-helm-smoke`；旧 Helm 证据目录需先归档。
6. 故障测试仅针对专用测试 Job/进程，记录故障注入、受影响对象、恢复过程和残留资源。
7. 尚无硬件/接线的场景标 NOT_RUN/BLOCKED，不改 DataKind、阈值或校验逻辑强行通过。

## 结果放哪里、如何回传

原始数据仍保留在测试机：

- E1：`.cache/tgsrl/gpu-smoke/`
- H1：`.cache/tgsrl/hami-smoke/`
- H2：`.cache/tgsrl/hami-concurrency-smoke/`
- A10 汇总：`.cache/tgsrl/a10-readiness/readiness-summary.json`
- Helm：`.cache/tgsrl/helm-smoke/evidence-index.json`
- Helm 第二轮：`.cache/tgsrl/helm-smoke-round2/evidence-index.json`
- 最终审计：`.cache/tgsrl/final-a10-audit-68f5aea.json`
- E6：`.cache/tgsrl/e1-e8/e6-action-cost/`
- E6 确认审计：`.cache/tgsrl/e6-confirm-13d0f34/e6-confirmation-audit.json`
- E5-STATIC：`.cache/tgsrl/e5-static-interference/e5-static-interference/`
- E5/E6 审计：`.cache/tgsrl/e5-e6-audit-2026-09-16.json`
- 故障 readiness：`.cache/tgsrl/engineering-fault-readiness/report.json`

每批复测前归档旧目录，不能覆盖原证据。提交给开发机的是**脱敏副本**：

```text
handoff/<场景>-<日期>-<短SHA>/
  NOTES.md             命令、源码 SHA、环境版本、预期/实际、精确错误、清理结果
  report.json          脱敏报告副本
  campaign-report.json 脱敏汇总
  logs/                必要服务/worker 日志与相关 Trace
```

保留原始文件 hash 与采集位置记录；脱敏副本不是重新生成的原始签名证据，不修改原 report 的 SHA
来使其看似属于新版本。日志中的 token、私网地址、个人路径等需脱敏；不提交 kubeconfig、registry
登录文件、signing key、`.env` 或 `configs/hardware/environment.json`。

在 `test/*` 分支逐文件暂存，运行 `make check-public-content`、`make check-repository`、
`git diff --check` 后提交并推送；将 commit/分支和本节结果一起交给开发机。每次推送检查该 SHA
的 CI。开发机拉取后复现/修复，再交测试机复测；不为过门禁削弱身份、safe-point、receipt 或回读规则。

## 发布前清理

确认所有需要的交接结论已迁入 `docs/validation/`/维护文档后，再删除本文件和 `handoff/`，
恢复相应 ignore 规则。保留 LICENSE、配置示例、SQL 迁移、锁文件、生成协议和必要回归用例。
