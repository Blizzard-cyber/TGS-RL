# E1–E8 硬件验证 Campaign

本设计把发布前的硬件验证拆成八个相互独立、可追溯的实验。它建立的是**证据合同和
验收入口**，不是硬件通过声明。没有目标 GPU/Kubernetes 环境报告时，实验状态必须保持
`NOT_RUN`；证据结构错误、设备身份不一致或故障事件不完整时为 `INVALID`。

## 实验矩阵

| 实验 | 目标 | 当前状态 | 关键要求 | 当前阈值状态 |
|---|---|---|---|---|
| E1 | Full GPU 精确设备执行 | **PASSED**，`GPU_SINGLE_NODE` | Scheduler、DRA allocation 与 worker UUID 一致；bind 成功 | action success 已锁定 |
| E2 | MIG 精确设备执行 | **BLOCKED / NOT_RUN**，当前 A10 无可用 MIG 拓扑 | MIG profile、parent UUID、设备身份一致；bind/rebind 成功 | action success 已锁定 |
| E3 | 吞吐与 Valuable Useful GPU | **NOT_RUN**，待正式 workload | 同模型、数据、seed、节点；采集 GPU active/useful time | throughput 下限已锁定；VUG 待校准 |
| E4 | policy lag、staleness 与 ESS | **NOT_RUN**，待环境 hook | 注入 policy update delay；采集真实 batch quality | 待校准 |
| E5 | 共置干扰隔离 | **NOT_RUN**；E5-STATIC 已完成 A10 采集，但不替代正式 E5 | 正式 E5 仍需 competing workload 与动态 share/priority 控制 | 待校准 |
| E6 | lifecycle 动作代价 | **PASSED**；`13d0f34` A10 独立确认、`GPU_SINGLE_NODE` | pause/checkpoint/offload/reload/resume 全部有独立 receipt | 54.479/695.910/272.408 ms 均通过 60/900/400 ms |
| E7 | 故障与事务恢复 | **NOT_RUN**，CPU/进程故障不替代 GPU fault | worker exit、response loss 均有 injected/recovered 事件 | action success 已锁定；恢复时间待校准 |
| E8 | 多节点稳定性与收敛 | **BLOCKED / NOT_RUN**，缺少两个 GPU 节点 | 至少两节点；node loss 恢复；两侧节点集合一致 | throughput 下限已锁定；收敛质量待校准 |

机器可读入口为：

- `configs/gates/e1-e8.json`：实验、最低 evidence、动作、故障和规则；
- `configs/scenarios/e1-*.yaml` 到 `e8-*.yaml`：每项独立拓扑与 workload/fault 合同；
- `configs/gates/gate-e1-e8-hardware.json`：所有实验复用的硬件 trace/report schema；
- `scripts/hardware-campaign-executor.py`：仓库控制的 baseline/variant、迭代、动作、故障和 cleanup 编排；
- `scripts/gate-tools.py`：单项 evidence ingest、验证、归档与 campaign 汇总。

目标环境执行入口为 `campaign-run`。它按 E1 到 E8 的固定顺序运行仓库 executor，
逐项校验并归档证据，最后执行 release evaluation：

```bash
make gate-campaign-run \
  TGSRL_HARDWARE_DRIVER_CONFIG=/etc/tgsrl/hardware/environment.json
```

首次单机 Full GPU 联调使用 `make gpu-smoke`，它等价于只选择 E1 的 campaign run。E1
执行失败、证据无效或规则失败时命令返回非零；E1 通过时命令成功，但 campaign 总状态仍会
因为 E2–E8 缺失而是 `NOT_RUN`。只有不带 `--experiment` 且带 `--require-pass` 的完整
`make gate-campaign-run` 才是 E1–E8 发布准入。单项运行结束后应直接读取
`.cache/tgsrl/gpu-smoke/e1-full-gpu/report.json` 查看 E1 证据；根目录的
`campaign-report.json` 同时列出未运行实验，因此总体状态不会是 `PASSED`。

HAMi 分数 GPU 使用独立的 H1/H2 campaign，不插入或重排正式 E1–E8。H1 验证单 worker
份额兑现；H2 验证两个 worker 共享同一物理 UUID、总 core 份额为 `80%` 且执行区间真实重叠。
两者复用同一 executor/evidence schema，execution mode 固定为 `hami-vgpu`。执行入口为：

```bash
make gpu-prepare-hami
TGSRL_OPERATOR_GPU_PROFILES=hami-vgpu \
TGSRL_GPU_MANIFEST=compatibility/manifests/hami-smoke-verl.yaml \
  make gpu-up
make gpu-hami-smoke
make gpu-down
make gpu-hami-up
make gpu-hami-concurrency-smoke
```

H1 结果写入 `.cache/tgsrl/hami-smoke/h1-hami-vgpu/`，H2 结果写入
`.cache/tgsrl/hami-concurrency-smoke/h2-hami-concurrency/`。两者都是 realization smoke，
不进入 E1–E8 release evaluation；H2 也不替代 E5 共置干扰、OOM/公平性或 E6 生命周期实验。

当前单张 A10 可先执行独立 `E5-STATIC` pilot：

```bash
make gpu-down
make gpu-prepare-hami
make gpu-hami-up
make gpu-e5-interference
```

它保持两个 worker、每个 `40%` core/显存份额、workload 参数、seed、镜像和节点一致，仅让
baseline 的两个 worker 错峰执行、variant 的两个 worker 并发执行。orchestrator 从每个 worker
的 `item_count / elapsed_ms` 推导共置干扰率，并硬校验 baseline 无重叠、variant 有正重叠。
该 pilot 用于得到当前硬件上的静态份额干扰边界，不满足正式 E5 的在线 `set_share` /
`set_priority` 要求，因此不会修改正式 E5 状态。

2026-09-16 的 A10 pilot 中，E5-STATIC 独立 report 为 `PASSED`：baseline
`worker_overlap_ms=0`，variant `worker_overlap_ms=8570.544`、两个并发 worker、总份额
`0.8`，三轮观测干扰率为 `0.222769/0.218472/0.246678`。按三轮最大值乘 `1.25`
并向上取工程档位，门槛冻结为 `0.32`，决策见
`configs/gates/e5-static-interference-calibration.json`。后续干净提交 `9a4321d`
独立确认测得三轮 `0.071733/0.058150/0.188928`，均值 `0.106270`，正式 gate
为 `PASSED`。同一 pilot 没有被同时用于标定和验收。详细来源、镜像和哈希见
[E6 与 E5-STATIC 单 A10 实验记录](../validation/e5-e6-single-a10-2026-09-16.md)。

A10 实验前 readiness 另有独立 `A10-FULL` campaign。它在 E1 已证明的 exact-device 主链上，
通过 scoped registry 调用 cooperative worker，要求 checkpoint 存在，并把 PyTorch allocator 的
allocated/reserved bytes 从 worker → bootstrap → registry → hardware driver → Gate 证据链
带回。offload 后 allocated bytes 必须下降，resume 后必须回升；仅有状态布尔值不能通过。
`make gpu-a10-readiness` 再组合 H2 与 DRA 恢复后的 E1，但不改变 E1–E8 发布矩阵。

正式 E6 使用同一个 scoped worker registry，把 `pause`、`checkpoint`、`offload`、`reload`
和 `resume` 作为五个独立、幂等、generation-fenced 的动作执行。代表性 CUDA tensor workload 会
实际序列化并重新加载 tensor checkpoint；Gate 从每个动作的独立 receipt 计算延迟，并继续
要求 offload 后 allocator bytes 下降、reload 后恢复。执行入口为：

```bash
make gpu-down
make gpu-restore-dra
make gpu-up
make gpu-e6-action-cost
```

2026-09-16 的 A10 pilot 中，E6 独立 report 为 `PASSED`，三轮 measurement 最大值分别为
pause `46.907 ms`、checkpoint `711.072 ms`、reload `301.490 ms`。按“最大值乘
`1.25` 后向上取工程档位”的方法，正式门槛冻结为 `60/900/400 ms`，决策见
`configs/gates/e6-action-cost-calibration.json`。后续干净提交 `13d0f34` 的独立确认运行
测得 `54.479/695.910/272.408 ms`，三项规则全部 `PASSED`；同一 pilot 没有被同时用于
标定和验收。

仓库提供 `scripts/tgsrl-hardware-environment-driver`。目标环境从
`configs/hardware/environment.example.json` 派生本地配置，至少指定 Gateway URL、固定
Kubernetes context/namespace、workload Job 模板和容器内 trace 导出命令。driver 的私有状态按
`campaign_id/experiment_id/run_key` 持久化，重复 request ID 只回放同一 receipt。配置文件只引用
凭据所在环境，不允许内嵌 token、password 或 kubeconfig。

仓库内 executor 固定读取各 scenario 的 `execution_plan`，并对每个 baseline/variant 的
warmup/measurement iteration 依次执行 `provision`、`launch`、`verify_device_identity`、
`apply_action`、`apply_worker_action`、`inject_fault`、`recover_fault`、`measure`、`stop` 和
`cleanup` 中声明的步骤。
失败后仍会单独执行 cleanup。目标集群只需要提供一个原子 environment driver；它接受：

```text
--request <absolute JSON request path>
--response <absolute JSON response path>
```

request 包含 immutable campaign/scenario/workload lock、run key、step index、operation 和可选
action/fault ID；response 必须使用 `tgsrl.io/hardware-driver-response/v1alpha1`，回显 request ID，
并返回 `SUCCEEDED` 与该原子操作产生的 events/artifacts。driver 负责连接目标 Kubernetes/DRA
环境、执行单个操作和读取事实，不得自行重排或省略实验步骤。主 runner 负责：

- 校验 gate-tools、executor、driver、campaign、gate manifest 和 scenario 的 SHA-256；
- 拒绝输出目录逃逸、缺失 artifact、错误 evidence 等级和旧 commit 报告；
- 调用既有 ingest，复制成自包含 evidence bundle；
- 保存 executor/driver stdout/stderr 摘要与失败进度；
- 任一实验失败时停止后续实验；全部执行后使用 `--require-pass` 做发布准入。

仓库不内置集群凭据、模型、数据或厂商环境命令。替换 environment driver 是部署环境配置，
不要求修改场景顺序、Scheduler、Runtime、Operator 或 Gate 证据协议。

### 仓库 driver 的环境边界

- `provision` 通过 Gateway 创建并准入 Job；`bind` 通过 Start 触发 Scheduler，不直接创建
  Kubernetes workload；
- `launch` 必须同时观察到 Runtime `RUNNING`、目标 Pod `Ready` 和 managed-worker identity；
- `verify_device_identity` 在 DRA 模式下从 Pod status 定位生成的 ResourceClaim，再对账
  Scheduler Binding、claim allocation + 最新 ResourceSlice 与 Pod 内 `nvidia-smi -L`，
  Full GPU/MIG 分别要求正确 DeviceClass；在 HAMi 模式下对账 Node registration、Pod
  `use-gpuuuid`、实际 allocation UUID/memory/core 与 worker 可见 UUID；
- `measure` 执行配置中的容器内只读 trace 导出命令，只接收身份匹配的真实 worker NDJSON；
- `apply_worker_action` 只接受当前 JobRunBundle 中一个 exact sandbox/generation，并使用
  bootstrap 注入的 scoped token 调 registry；`bind` 仍只能走 Scheduler。A10 offload/resume
  还要求动作前后真实 GPU allocator bytes；
- `stop` 通过 Gateway lifecycle API；`cleanup` 仅删除该 run 的 JobRunBundle 及其精确命名的
  Job、Workload、ResourceClaimTemplate，不使用 label-wide 或 namespace-wide 删除；Pod 生成的
  ResourceClaim 由 owner 生命周期回收；逐一完成身份校验和子资源删除后，driver 移除本次
  JobRunBundle 的保护 finalizer 并幂等删除 marker，避免外置 Operator 重启使 cleanup 挂起；
- `rebind`、`set_share`、`set_priority` 等自适应动作必须配置环境 hook。hook 只负责向已有
  Runtime/Scheduler observation 入口提交信号并返回 authority receipt，driver 随后等待新的成功
  Scheduler decision；没有 hook 时 fail closed，绝不 patch ResourceClaim 或 Binding；
- 故障注入同样使用显式、按 fault ID 配置的 hook，仓库默认不携带集群破坏命令。

环境 hook 接受 `${REQUEST_PATH}` / `${RESPONSE_PATH}`，response 使用
`tgsrl.io/hardware-driver-hook-response/v1alpha1`。会形成 Scheduler Action 的 hook 必须回传
`authority: scheduler-observation`；`checkpoint`、`reload`、`rollback` 这类训练进程操作必须回传
`authority: managed-worker-control` 和非空 receipt ID；fault hook 必须回传
`authority: target-environment`。
hook 必须以 request ID 实现幂等；driver 或 runner 在响应丢失、超时或重启后可能重放同一请求。

## 证据流

```text
目标环境执行 E<n>
  -> 原始 baseline/variant trace
  -> service/worker logs + SHA-256
  -> 单项 report.json（含 experiment_id）
  -> campaign-ingest E<n>
  -> 固化 scenario + trace + logs + report
  -> campaign-evaluate --require-pass
```

导入单项证据：

```bash
uv run --frozen python scripts/gate-tools.py campaign-ingest E1 \
  --campaign configs/gates/e1-e8.json \
  --reports-dir .cache/tgsrl/e1-e8 \
  --report /path/to/e1/report.json
```

汇总当前状态：

```bash
make gate-campaign
```

发布准入使用严格模式：

```bash
uv run --frozen python scripts/gate-tools.py campaign-evaluate \
  --campaign configs/gates/e1-e8.json \
  --reports-dir .cache/tgsrl/e1-e8 \
  --output .cache/tgsrl/e1-e8/campaign-report.json \
  --require-pass
```

## Fail-closed 规则

- CPU、Mock、Synthetic 或普通 CUDA reference workload 不能满足任一 E1–E8 的最低证据。
- 报告、trace、service log 和 scenario 均使用 SHA-256 校验；导入结果是自包含 evidence bundle。
- baseline 与 variant 都必须有完整 warmup/measurement iteration 和相同 workload lock。
- 每个 measurement iteration 都必须带唯一 service job/run、Scheduler decision/plan 和
  managed-worker identity；需要精确设备身份时，两侧每轮都必须证明 Scheduler UUID、DRA
  allocation UUID 与 worker 可见 UUID 完全一致。
- 故障场景必须同时记录 `fault_injected` 和 `fault_recovered`，不能只证明故障被触发。
- E8 必须使用 `GPU_MULTI_NODE`，baseline/variant 至少包含相同的两个真实 node identity。
- `calibration_required: true` 的规则没有数值门槛时结果为 `BLOCKED`；只有将经过评审的
  阈值写入 campaign 并重新校验后，才可能得到 `PASSED`。

取得真实证据后，可生成只读标定报告：

```bash
make gate-campaign-calibrate
```

输出列出每条未标定规则的 observed value、证据等级和 report digest，但不会修改
`configs/gates/e1-e8.json`。阈值仍需基于多次 baseline/variant 运行人工评审后提交；缺报告的规则
保留在 `missing` 中，状态为 `INCOMPLETE`。E6 的标定决策已经单独保存在
`configs/gates/e6-action-cost-calibration.json`，后续确认运行不得修改该文件。

## 当前验证边界

仓库 CI 会验证 campaign schema、证据降级、设备身份分叉、故障证据缺失、场景 digest、
日志复制和未校准阈值的 fail-closed 行为。`make gate-cpu-integration` 已验证完整本地服务链，
但仍输出 `CPU_INTEGRATION/NOT_RUN`。E1 已在 `68f5aea` 之前和最终 readiness 中取得真实
单节点 GPU 证据；E2–E8 的 MIG、正式 workload、故障、性能与多节点结果仍必须在目标环境
执行后再导入。整体推进顺序见[毕设推进状态](../project-progress.md)。
