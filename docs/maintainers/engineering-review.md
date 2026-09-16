# 工程完整性审查（2026-09-13—2026-09-15）

## 结论与范围

审查起点为 `4831ac041e71b93d3bb53a4765971445efc5e7be`，本记录同时覆盖本批修复与文档整理。
本周期目标是**验证模块、执行链和工程交付是否完整**，不是毕业实验、性能收益或生产发布验收。

已具备 Job → Runtime → Scheduler → Provider → Operator → bootstrap → worker → observation/Trace
的实现路径。本批进一步补齐 scoped lifecycle、真实 allocator 观测、A10 readiness 聚合和
六服务 NVIDIA Helm 实装入口；A10 lifecycle、H2、DRA 恢复 E1 与 Helm 已在单张 A10 上验证，
最终 `68f5aea` 还完成同一 PVC 上的 install/upgrade 两轮 smoke。GPU 集群故障证据与完整
训练 callbacks 仍需后续测试。

审查以第一方代码和入口调用链为范围：Job Controller、Runtime/SQLite、Scheduler/事务/Provider、
Operator/backend/registry、Gateway、Console 数据层、打包/部署与 Gate。生成协议和第三方依赖
通过契约/锁文件检查，不逐行审计；这不是全量安全认证。

## 已确认并修复

| 优先级 | 问题与影响 | 修复 | 回归 |
|---|---|---|---|
| P0 | Python wheel 不含 SQL migrations；源码 editable 测试通过，但独立 Runtime 无法建业务表 | 显式 package-data；迁移目录缺失/为空立即失败；初始化失败关闭连接 | 干净源目录构建 sdist → wheel → 隔离安装 → SQLite 写入/重开 |
| P1 | `.dockerignore` 后部重新包含 `configs/**`，可能把本机 hardware 配置打入镜像 | 最后再次排除真实配置、嵌套凭据、私钥、PID/socket，保留 examples | 排除规则回归；`68f5aea` 在 A10 构建并推送全部不可变镜像 |
| P1 | driver 等待 bind Decision 时不看 Start Operation，已明确失败仍可能等完整 timeout | 同一轮检查 Operation，终态失败立即报错；进行中的 Start 不阻止 Decision | 立即失败、延迟失败、Start 未结束但 bind 成功 |
| P1 | Console `tsc --noEmit` 只检查空 root project，实际未检查源码 | 明确检查 app/node 两个 tsconfig | 修复前 listFiles 为空；修复后两个项目 typecheck |
| P1 | 总览/任务列表吞掉 Run 查询失败，页面可能显示旧状态而不报错 | 保留查询错误，不把失败映射成“没有 Run” | 两条 API contract 回归与实际 HTTP 页面故障注入 |
| P1 | 部署脚本使用锁定 Helm 4 已移除的 `--atomic`，render 通过而 install/upgrade 失败 | 使用 `--rollback-on-failure`；从本机 Helm help 校验脚本参数 | 两类 chart 校验与 install/upgrade 参数检查 |
| P2 | 文档校验从 Git index 读取已删除、未暂存的文件而崩溃 | 跳过不存在的文档，引用到它的链接仍报错 | 临时 Git 仓库的未暂存删除回归 |
| P1 | MPS 合法小份额可四舍五入为 0，先发无效 mutation 再因 readback 拒绝 | 转换后为 0 时在执行前拒绝，不偷偷提高请求份额 | dry-run/真实执行均无命令发出 |
| P2 | `_placeholder_launch_spec` 和 registry 注释误导读者认为执行层缺失 | 改名为失败上下文，删除错误的“无 lifecycle 接口”注释 | 既有 Runtime/Go 测试 |
| P2 | README 无可运行任务示例、设计与滚动审查混杂、bootstrap 端口/探针描述过时 | 文档分层；示例与 smoke 共用；更新动态端口与 marker probe | 文档链接/命令检查、实际服务 smoke |
| P1 CI | 已推送基线 Python 格式检查失败，下游进程检查 skipped | 按已有 Ruff 规范整理，不削弱 CI | 最终 `68f5aea` 精确 SHA 的 10/10 GitHub checks 全部 success |
| P1 | hardware campaign 跨批次复用确定性 create-job 幂等键，持久化 Gateway 会对新镜像请求返回 409 | 每个持久化 attempt 生成随机身份；Job ID 与操作键绑定该身份；同 attempt 的崩溃重试仍稳定 | driver 37 项；同一 Helm PVC 连续实机复跑通过 |
| P1 | `kubectl rollout status deployment --all` 在 Kubernetes 1.35 已移除 | 显式等待六个预期 Deployment | Helm contract 与 A10 实装 |
| P1 | 非 root Scheduler helper 尝试 `chmod` root-owned PVC 挂载点，导致 `bind/release` capability 不发布 | binding/runtime/registry state 使用 PVC 下由进程创建的私有子目录 | Helm contract、Pod 内 helper discovery 与 A10 lifecycle |

## 本轮进一步关闭的工程事项

| 项 | 处理结果 | 验证 |
|---|---|---|
| hardware cleanup GET→DELETE 竞争 | 子资源和 Bundle 使用 UID/resourceVersion 条件删除；finalizer patch 使用 JSON Patch test | driver/Gate 专项测试 |
| 硬件 provenance 不完整 | 锁定 cluster/namespace UID、Kubernetes 版本、environment、Job template、Trace command、hook executable 和 rendered Job | 73 项 Gate/driver 测试 |
| worker 故障回写 | crash、注册响应丢失、marker 写失败、终态上报失败和 callback timeout 均有真实进程/通信回归 | bootstrap Go 测试 |
| 单节点 NVIDIA Helm | NVIDIA Driver v2、helpers、共享 worker state、RuntimeClass、节点绑定和专用镜像 target 已接入并 fail closed 校验 | Helm render contract 与单张 A10 六服务实装 |
| MPS 在线份额错误声明 | 生产 CLI 和 Helm 拒绝 MPS，backend 不再广告/执行在线 `set_share` | NVIDIA Provider/CLI 测试 |
| scoped worker lifecycle | registry 允许 exact token/sandbox/generation 的 pause/checkpoint/sleep/offload/reload/resume/stop；`bind` 仍由 Scheduler 权威执行 | Go registry/controller 与 driver contract |
| A10 显存证据 | 最小 CUDA workload 保留 resident tensor；allocator bytes 经 worker/bootstrap/registry/driver 返回，Gate 要求 offload 下降、resume 回升 | Python/Go/Gate 专项；单张 A10 实机通过 |
| A10 总体验收 | 卡型硬校验、Full lifecycle、H2、恢复 DRA 后 E1、聚合摘要与旧证据归档 | `68f5aea` 单张 A10 四组件聚合结果通过 |
| 故障 readiness | crash、response loss、receipt 重启、Operator partial failure、CPU/内存/GPU capacity 拒绝、provider unavailable、Runtime SQLite 恢复形成可重复报告 | `.cache/tgsrl/engineering-fault-readiness/report.json` 本机 PASSED |
| cleanup 删除竞争 | 条件删除只将精确 `NotFound` 视为幂等完成；UID/resourceVersion Conflict 保持 fail closed | driver 专项、全量回归与 A10 cleanup |
| observation registration 泄漏 | 启动时按现存 Bundle 原子剪枝，运行中在 Bundle absence 后删除 registration | 97 个孤儿记录降为 0；Operator CPU 恢复，A10/H2/E1 重跑通过 |
| Helm 依赖就绪 | 不再把 HTTP 200 + degraded 当就绪；要求 Gateway 四个 gRPC 依赖全部 serving | 首次失败证据保留；`68f5aea` install/upgrade 两轮均通过 |
| Helm 实装入口 | 构建全部不可变镜像，生成 values，install/upgrade、健康检查、A10 Full、成功/失败证据索引 | 单张 A10 六服务两轮实装，各 315 项，共 630/630 哈希匹配 |

## 尚未关闭的工程事项

| 项 | 源码/证据入口 | 影响与下一步 |
|---|---|---|
| MPS 节点级 realization 缺失 | `mps_backend.go` | 在线 `set_share` 已撤下；未来需要 checkpoint/recreate 新 client、启动限额注入与 incarnation readback |
| 完整 veRL 未验收 | `make gpu-a10-full-readiness`、GPU workload | 最小 adapter 已在 A10 完成真实 checkpoint/offload/reload 和 allocator 观测；完整 trainer/collective 另行验证 |
| 真实 GPU 故障证据不足 | `make engineering-fault-readiness`、environment hooks | CPU/真实进程报告已覆盖 crash/timeout/响应丢失/组件重启/partial failure；Pod/容量/节点网络故障明确 `NOT_RUN` |
| 大文件维护成本 | hardware driver、Gate tools、planner 等 | 后续新增功能前按职责拆分；本轮不为“缩行数”重写稳定状态机 |

以上区分“已确认代码/部署缺口”和“缺少目标环境证据”。未在本轮复现的风险不写成确定故障，
也不因历史 E1/H1/H2 通过而直接关闭。

## 文档与仓库整理

```text
README                      项目定位、快速运行、能力边界、导航
  docs/getting-started       第一个任务、观察结果与停止
  docs/design               问题、架构、身份、进程监管、扩展边界
  docs/guides               使用、配置、部署与排障
  docs/reference            当前支持矩阵
  docs/maintainers           源码地图、开发规则、本轮审查
  docs/validation            绑定具体版本的历史实机结论
```

旧的混合设计/审查长文由上述入口承接，不另留一份影子设计。Mock、Synthetic fixture、生成协议、
SQL、锁文件和原始本机证据保留；`.cache`、依赖、数据库、私有配置不进入发布内容。
`WORKLOG.local.md`/`handoff/` 继续承担临时跨机器交接，正式发布前移除。

## 验证记录

本批在 macOS arm64、Python 3.12.13 环境执行，Python patch 低于 BOM 的 3.12.14；本机结果
不用于替代 Linux/GPU 证据。最终 `68f5aea` 的本地门禁、GitHub CI 10/10 和 A10 实机验收
分别核对，三类证据不互相替代。

| 检查 | 结果 |
|---|---|
| Python Runtime/Storage/Governance | **520 passed**；Gateway API **39 passed** |
| Go 模块与 race | 全部通过；MPS 零百分比 dry-run/执行前拒绝回归通过 |
| 静态检查 | `make lint`、`make staticcheck` 通过；Ruff、mypy、Go vet/Buf lint |
| 前端 | typecheck、ESLint、**73 unit tests**、production build 通过 |
| 浏览器 fixture | **17 passed**，九个路由与多宽度布局 |
| 产品进程 E2E | 创建/准入/控制/停止/重试与持久服务重启、幂等恢复通过 |
| CPU process Gate | 8/8 workload 执行，三条工程规则通过；GPU 发布状态仍是 NOT_RUN |
| 配置与发布边界 | docs、repository、public-content、SBOM、compatibility、OpenAPI、两层 Helm、Compose config 通过 |
| 生成协议 | `make check-generated` 通过 |
| 性能回归 | 本轮 Scheduler 小/大 fixture CPU P95 中位数约 10.79/20.10 ms；Provider observation 6 µs，均在预算内；非 wall-clock SLA |

### 干净副本与真实 HTTP 前端

从 Git 跟踪文件及本批新源码复制干净目录，不带本机缓存、egg-info、数据库或虚拟环境；
使用已缓存依赖离线创建独立 venv，在副本重新构建 Go 服务。Console 使用本批已构建的 HTTP
adapter 产物，由真实静态/同源代理服务提供；Playwright 使用本机已有浏览器，不下载镜像。

结果：六服务启动成功，公共 CPU 示例经过 start/pause/resume/stop，四次 Operation 成功；
两个终态 Sandbox、13 条 Trace（12 条明确 Synthetic），**allocation 残留为 0**。
浏览器验证总览、任务、Trace、Decision、Sandbox、资源六个实际 HTTP 路由，Trace 四轨可见；
390px 无页面级横向溢出；注入 Run 查询 503 后总览/任务页明确报错，恢复后正常加载，无页面异常。
测试创建的服务均已关闭。

本机产物位于 `.cache/tgsrl/review-clean-7s7wkc0m/`：`smoke.json`、`browser.json`、服务日志和
桌面/移动截图。此为本地验证路径，不要求新 clone 携带，也不提交原始数据。

### CPU Gate 证据核对

`.cache/tgsrl/review-20260913-process/` 的报告与 Trace 已核对：

- baseline/variant `scheduling_latency_ms_p95` 分别为 **67.787 / 60.611 ms**，非零事件测量。
- variant 同时有 scheduler、runtime、operator、worker 四类来源。
- 每轮 variant 均有 worker 的 prepare_pause/pause/resume，Operator 动作有对应成功记录。
- 吞吐约 44.37 / 31.21 items/s，比例 **0.7034**；variant 含暂停/恢复，当前规则只要求观测到
  正吞吐，不要求 0.95，也不能声称性能不退化或有收益。
- evidence 为 CPU_INTEGRATION；即使工程规则通过，report 的 GPU 准入状态仍为 NOT_RUN。

本批新增故障 readiness 报告在本机为 `PASSED`，证据级别是
`CPU_REAL_PROCESS_AND_COMPONENT_INTEGRATION`；GPU fault 三项保持 `NOT_RUN`。
开发机未执行 Docker build/pull 或 GPU 测试；测试机已在最终 `68f5aea` 干净基线上完成
A10 lifecycle/H2/DRA 恢复 E1，并完成六服务 Helm install/upgrade 两轮 smoke。详见
[A10 工程与 Helm 验收](../validation/a10-readiness-2026-09-14.md)。
后续 `701e11b`/`3c9147c` 又完成 E6 与 E5-STATIC 的单 A10 数据采集；两项独立 report
均为 `PASSED`。E6 已从 pilot 冻结 `60/900/400 ms` 门槛，并在 `13d0f34`
独立运行中全部通过；E5-STATIC 仍待阈值标定。详见
[E6 与 E5-STATIC 单 A10 实验记录](../validation/e5-e6-single-a10-2026-09-16.md)。

## 后续按工程依赖推进

1. 保留 `68f5aea` 为实验前工程基线，并以各实验报告中的 source commit 绑定后续结果。
2. 评审 E5-STATIC 干扰率阈值。
3. 接入完整或代表性的 veRL workload，冻结模型、数据、seed、batch、镜像和节点条件，推进 E3。
4. 接入 E4/正式 E5/E7 所需环境 hook；正式 E5 不继承 E5-STATIC 的结果。
5. 获得 MIG 硬件后执行 E2；准备至少两个 GPU 节点后执行 E8。
6. 每轮核对正常/异常退出、控制超时、重启恢复、UUID 与资源清理，保留脱敏证据。
7. 完成统计分析、图表、论文结果章节和答辩材料。统一进度见
   [毕设推进状态](../project-progress.md)。

历史 E1/H1/H2 报告继续引用原 commit，不重写 source hash，不把本地 CPU PASS 当作新的 GPU PASS。
