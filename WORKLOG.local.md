# 开发与 GPU 测试交接

更新日期：2026-09-13。此文件按约定提交，用于两台机器通过 GitHub 协作；不是产品设计文档。
正式发布前移除本文件与 `handoff/`，长期内容保留在 `docs/`。

## 当前周期：工程完整性，不是毕业实验

目标是确认代码、模块和执行接线能正常运行，并处理失败、取消、重启和资源清理。
暂不推进 VUG/算法优化或 E3–E8 性能标定，不用一个进度百分比代替验收证据。

- **开发机**：macOS，无 NVIDIA GPU。负责实现、review、CPU/Mock、真实子进程、文档与治理。
- **测试机**：Linux + NVIDIA。拉取干净 commit，验证 CUDA/设备兑现和真实故障，提交脱敏结果。
- 本批不访问 ECS，不在开发机拉取/构建 Docker 镜像，不删除旧数据库或原始硬件证据。

## 当前工作树与 CI

- 本批起点：`4831ac041e71b93d3bb53a4765971445efc5e7be`，分支 `main`；本文件随本批实现一起提交。
- 测试机复测前以实际拉取的 `git rev-parse HEAD` 为准，不要把起点 SHA 当作本批结果 SHA。
- 基线 [CI run 34710517617](https://github.com/Blizzard-cyber/TGS-RL/actions/runs/34710517617)
  为 **failure**：6 success、1 failure、3 skipped。Python 格式检查失败导致三个进程类 job 跳过。
- 格式已在本地修正，但本地通过不等于远端绿色。获授权推送后逐项检查新 SHA 的所有适用 workflow/job
  到终态；skipped/cancelled/running 都不是通过。不绕过 hooks，不加自动 `Co-authored-by` trailer。

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

详细发现与本批最终验证见 [工程审查](docs/maintainers/engineering-review.md)。
稳定架构见 [系统设计](docs/design/system-design.md)，使用入口见 [快速上手](docs/getting-started.md)。

## 仍需推进的工程项

1. 本批回归/干净副本六服务/真实 HTTP 前端已通过，下一步审查并按授权提交；细节见下节。
2. 经授权推送并核对 CI，测试机拉取该精确 SHA；不要拿旧硬件结果冒充新版本验证。
3. 新包/driver 先复跑 E1；共享路径相关改动再复跑 H1/H2，并检查失败与正常清理。
4. 补 worker 异常退出、callback timeout/响应丢失、Runtime/Operator 重启、partial failure 的真实证据。
5. 完整 veRL trainer/collective/checkpoint/offload/reload 尚待验证；最小 adapter 不是完整训练。
6. 通用 GPU Helm 未完整接 NVIDIA v2/helper/host-device/shared mount；默认 chart 是 CPU 配置。
7. MPS 需验证 server/client 归属和运行中份额是否生效；MIG 仅在具备实例的硬件上测，不阻塞其他卡。
8. driver cleanup 尚有 GET→DELETE 竞争窗口，需 UID/resourceVersion precondition；完整 provenance
   还需固定环境配置、Job template、hook、rendered Job 和 cluster UID。

后续实验标定、吞吐/干扰公平性、长期存储、HA 与入口安全另行排期，不把它们混作已完成能力。

## 本批本机验证

- Python 529 passed；Go 全模块/race、lint/staticcheck、性能门禁通过。
- Console 73 unit tests + build/typecheck/lint；17 项 mock 浏览器路由/响应式检查通过。
- 产品 E2E 重启/恢复通过；生成协议、docs、部署、Compose config、SBOM/公开内容/仓库检查通过。
- 干净源码副本新建独立 venv、重新构建 Go，六服务 smoke 四次操作成功、13 条 Trace、无 allocation 残留；
  六个真实 HTTP 页面、Trace 四轨、390px、Run 查询故障/恢复已在浏览器验证，测试服务已停止。
- CPU process Gate 8/8 执行；四源 Trace 和 worker pause/resume 确认；variant scheduling P95 60.611 ms。
  吞吐比 0.7034，包含控制暂停开销；只证明正吞吐和控制链，不证明性能收益，GPU 状态仍 NOT_RUN。
- 原始本机证据：`.cache/tgsrl/review-clean-7s7wkc0m/`、`.cache/tgsrl/review-20260913-process/`。
- 本机 Python 为 3.12.13，BOM 为 3.12.14；没有 Docker 实装、Helm 实装或 GPU 复测，不夸大验证范围。

## 已有硬件证据（不可改写来源）

| 日期 | 源代码 | 场景 | 结果与记录 |
|---|---|---|---|
| 2026-09-12 | `d033566` | E1 Full GPU | PASSED，8/8 执行、Scheduler/DRA/worker UUID 一致、CUDA/Trace/清理；`docs/validation/e1-full-gpu-2026-09-12.md` |
| 2026-09-12 | `9c65d46` | H1 HAMi | PASSED，单 worker 40% core、9211 MiB、UUID 一致；`docs/validation/h1-hami-vgpu-2026-09-12.md` |
| 2026-09-12 | `9c65d46` | E1 恢复回归 | HAMi 卸载并恢复 Device Plugin 后通过 |
| 2026-09-13 | `a45a432` | H2 HAMi | PASSED，双 worker 同卡各 40%/9211 MiB，测量期正重叠；`docs/validation/h2-hami-concurrency-2026-09-13.md` |
| 2026-09-13 | `a45a432` | E1 恢复回归 | Device Plugin 1/1、单 GPU 容量恢复、无 HAMi 注解或 workload 残留 |

这些是单节点限定证据，不证明 OOM 隔离、公平性、动态份额、完整训练、MIG/MPS、多节点或性能收益。
E2–E8 未执行，九条实验阈值仍待后续真实 baseline 标定。

测试机既有环境快照：Ubuntu 22.04 x86_64、NVIDIA A10、580.178.04 driver、CUDA driver API 13.0、
Docker 29.8.0、Compose 5.5.1、Toolkit 1.20.0、Kubernetes 1.35.1、Kueue 0.19.2、NFD 0.18.3、
DRA 0.5.0、Minikube 1.38.1。**这是历史快照，复测先检查当前状态。** root 专用 smoke 环境需要
显式 `TGSRL_MINIKUBE_ALLOW_ROOT=1`；可复用主机优先非 root。此卡无 MIG，不要为了 E2 改造拓扑。

## 测试机如何执行

1. clone/pull 指定 clean commit，记录 `git rev-parse HEAD` 与 `git status --short`。
2. 按 [GPU Smoke](docs/guides/gpu-smoke.md) 安装依赖/准备集群/镜像/凭据；已有资源先检查再复用。
   网络受限可 `export TGSRL_NETWORK_PROFILE=cn`。仅在明确需要时安装或构建，不重建已健康集群。
3. 新批次先保留旧输出，再运行 `make gpu-preflight`、`make gpu-up`、`make gpu-smoke`。
4. 检查 report 为 PASSED、身份一致、真实 CUDA Trace、worker 操作确认与资源回收，不只看退出码。
5. HAMi 按 [HAMi 指南](docs/guides/hami.md) 执行 H1/H2，结束恢复 DRA，并再次核对 E1。
6. 故障测试仅针对专用测试 Job/进程，记录故障注入、受影响对象、恢复过程和残留资源。
7. 尚无硬件/接线的场景标 NOT_RUN/BLOCKED，不改 DataKind、阈值或校验逻辑强行通过。

## 结果放哪里、如何回传

原始数据仍保留在测试机：

- E1：`.cache/tgsrl/gpu-smoke/`
- H1：`.cache/tgsrl/hami-smoke/`
- H2：`.cache/tgsrl/hami-concurrency-smoke/`

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
