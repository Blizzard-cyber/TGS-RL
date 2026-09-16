# NVIDIA A10 工程验收

本指南用于毕业设计实验前的工程验收。它验证代码、控制链、真实 CUDA、同卡共享、恢复和
证据归档是否可工作；它**不**执行 E3–E8 性能标定，也不把一次 smoke 解释为调度收益、
完整训练或生产可靠性。

## 验收边界

完整入口要求一台 Linux 单节点环境中恰好有一张 `NVIDIA A10`，并依次执行：

1. Full GPU / DRA exact UUID；
2. cooperative `checkpoint → offload → reload → resume`；
3. PyTorch allocator 的 allocated/reserved bytes 在 offload 后真实下降、resume 后回升；
4. HAMi H2 两个 `0.4` worker 在同一物理 UUID 上产生正执行重叠；
5. 卸载 HAMi、恢复原 NVIDIA Device Plugin，并再次执行 E1；
6. 汇总三个子报告和 SHA-256。

由于 A10 不提供本项目可用的 MIG 拓扑，MIG 明确为 `NOT_RUN`。单节点环境也不覆盖
GPU 多节点、节点丢失和跨节点网络恢复。GPU Pod 强制退出、容量耗尽和网络中断只有在提供
专用、幂等、可恢复的 target-environment hook 后才能执行；仓库不会内置破坏任意集群的命令。

## 本机先做什么

开发机不需要 GPU。先运行真实子进程和组件级失败路径：

```bash
make engineering-fault-readiness
```

报告写入 `.cache/tgsrl/engineering-fault-readiness/report.json`，覆盖：

- workload 非零退出和终态回写；
- registration/control response loss 重试；
- pending worker receipt 的重启调和；
- Operator partial failure 的持久化与恢复；
- CPU、内存和 GPU capacity 不足的显式拒绝；
- Provider unavailable 的 no-op fallback；
- Runtime trace/checkpoint receipt 的 SQLite 重启恢复。

这类结果标记为 `CPU_REAL_PROCESS_AND_COMPONENT_INTEGRATION`，不能升级为 GPU 证据。报告中的
GPU Pod、容量、节点/网络故障保持 `NOT_RUN`。

## A10 测试机准备

从干净提交开始，并保留旧证据：

```bash
git status --short
git rev-parse HEAD
export TGSRL_IMAGE_REGISTRY=registry.example.com/your-user/tgsrl
export DOCKER_CONFIG=$PWD/.cache/tgsrl/docker

make gpu-install-host
make gpu-create-cluster
make gpu-prepare-cluster
make gpu-configure-access
make gpu-build-images
make gpu-configure-registry
make gpu-render-config
```

网络和 root-only 测试机开关见 [GPU Smoke](gpu-smoke.md)。`gpu-build-images` 要求干净工作树，
并为 bootstrap、GPU workload、NVIDIA Scheduler、Runtime、Job Controller、Operator、
Gateway 和 Console 生成不可变引用。

## Full GPU lifecycle

只验证 Full GPU lifecycle 时：

```bash
set -a
source .cache/tgsrl/gpu-runtime.env
set +a
make gpu-preflight
make gpu-up
make gpu-a10-full-readiness
make gpu-down
```

`A10-FULL` 使用 `configs/hardware/verl-lifecycle-job.example.json`。workload 保留一个
512 MiB CUDA tensor，driver 通过 scoped worker registry 执行动作：

```text
status
→ prepare_pause
→ checkpoint
→ offload
→ status
→ reload
→ resume
```

Gate 不只检查 `offloaded=true`。每个 measurement iteration 都必须满足：

```text
allocated_after(offload) < allocated_before(offload)
allocated_after(resume)  > allocated_before(resume)
checkpoint_present == true
resume.ready == true
```

结果写入 `.cache/tgsrl/a10-readiness/`。没有真实 report 时状态仍是 `NOT_RUN`。

## HAMi 双 worker 与恢复

只执行 A10 H2 和恢复动作：

```bash
make gpu-a10-hami-readiness
```

该入口先验证主机恰好是一张 `NVIDIA A10`，然后复用 [HAMi 指南](hami.md)中的 H2 严格合同：

- 两个独立 sandbox/binding/worker；
- 同一物理 UUID；
- 每个 worker `40%` core 和对应显存份额；
- aggregate core 为 `80%`；
- measurement 执行区间 overlap 大于 0。

脚本在退出路径尝试停止控制面并恢复 DRA，避免把 HAMi 状态遗留给下一轮。但失败时仍需人工
检查 `make gpu-hami-status`、Node 注解、Device Plugin 和 namespace 中的对象。

## 一键总体验收

完整顺序使用：

```bash
make gpu-a10-readiness
```

它会：

1. 拒绝脏工作树；
2. 把已有 A10/H2/E1 输出移动到 `.cache/tgsrl/archive/`；
3. 运行并归档真实进程/组件故障 readiness；
4. 运行 A10 Full lifecycle；
5. 切换到 HAMi 并运行 H2；
6. 恢复 DRA 并复跑 E1；
7. 写入 `.cache/tgsrl/a10-readiness/readiness-summary.json`。

四个组件必须都为 `PASSED`，总状态才是 `PASSED`。摘要始终列出未执行的 MIG、多节点和 GPU
集群故障，不会用已有 H2 历史报告或本机 CPU 结果代替本次提交的实机结果。

readiness 通过后，当前单卡可继续执行：

```bash
make gpu-e6-action-cost
make gpu-down
make gpu-prepare-hami
make gpu-hami-up
make gpu-e5-interference
```

E6 使用代表性 CUDA tensor workload 测五步 lifecycle；`gpu-e5-interference` 执行
`E5-STATIC` 静态份额 pilot。2026-09-16 已完成一轮真实 A10 采集，两项独立执行报告均为
`PASSED`。E6 随后在 `13d0f34` 独立运行中以 `54.479/695.910/272.408 ms`
通过 `60/900/400 ms` 门槛；E5-STATIC 已冻结 `32%` 门槛并等待后续干净提交
独立确认。`E5-STATIC` 不替代正式 E5 动态 share/priority 门禁。详见
[E6 与 E5-STATIC 单 A10 实验记录](../validation/e5-e6-single-a10-2026-09-16.md)。

## Helm 六服务验收

完整控制面镜像已经纳入 `gpu-build-images`。渲染并安装不可变 Helm values：

```bash
make gpu-render-helm-values
make gpu-helm-smoke
```

`gpu-render-helm-values`：

- 检查 Kubernetes context 和单节点名称；
- 创建/更新 worker registry signing Secret；
- 要求所有控制面、bootstrap 镜像为 `repository@sha256:...`；
- 配置 pull secret、NVIDIA RuntimeClass、同节点 selector、DRA profile。

`gpu-helm-smoke`：

- 拒绝覆盖已有证据目录；
- install 或 upgrade 六服务；
- 等待全部 Deployment rollout；
- 校验 NVIDIA Scheduler、NetworkPolicy、Console，以及 Gateway 到 Job Controller、
  Scheduler、Runtime、Experiment 四个 gRPC 依赖全部 serving；
- 为宿主机 hardware driver 建立独立 Gateway 与 worker-registry port-forward；Pod 内仍使用
  `http://tgsrl-scheduler:50091` 和原 scoped token；
- 通过 port-forward 复用 A10 Full lifecycle；
- 无论成功失败都保存 Helm status、values、资源快照、控制面日志和 SHA-256 索引。

默认证据目录是 `.cache/tgsrl/helm-smoke/`。最终基线 `68f5aea` 在同一组 PVC 上连续完成
install 与 upgrade 两轮 smoke，每轮 315 个索引文件均通过逐项 SHA-256 校验；单张 A10 的
六服务实装与 lifecycle 已通过，
见 [A10 工程与 Helm 验收记录](../validation/a10-readiness-2026-09-14.md)。该记录不能替代
其他 Kubernetes 版本、StorageClass、CNI、GPU 型号或多节点拓扑的独立验证。

## 结果判定

| 项目 | 可接受状态 |
|---|---|
| A10 Full lifecycle | `PASSED`，真实 `GPU_SINGLE_NODE` |
| H2 HAMi concurrency | `PASSED`，真实 `GPU_SINGLE_NODE` |
| DRA restore E1 | `PASSED`，真实 `GPU_SINGLE_NODE` |
| Helm smoke | `PASSED`，证据索引完整 |
| MIG | `NOT_RUN`，A10 无可用拓扑 |
| GPU multi-node | `NOT_RUN`，环境单节点 |
| GPU Pod/node/network faults | `NOT_RUN`，直到提供安全 hook |

失败时不要删除原始 `.cache/tgsrl/` 目录来重试。先保存 `readiness-summary.json`、
`campaign-execution.json`、driver request/response、Pod 事件和控制面日志，再修复根因。
