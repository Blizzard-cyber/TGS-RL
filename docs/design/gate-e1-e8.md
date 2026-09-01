# E1–E8 硬件验证 Campaign

本设计把发布前的硬件验证拆成八个相互独立、可追溯的实验。它建立的是**证据合同和
验收入口**，不是硬件通过声明。没有目标 GPU/Kubernetes 环境报告时，实验状态必须保持
`NOT_RUN`；证据结构错误、设备身份不一致或故障事件不完整时为 `INVALID`。

## 实验矩阵

| 实验 | 目标 | 最低证据 | 关键要求 | 当前阈值状态 |
|---|---|---|---|---|
| E1 | Full GPU 精确设备执行 | GPU 单节点 | Scheduler、DRA allocation 与 worker UUID 一致；bind 成功 | action success 已锁定 |
| E2 | MIG 精确设备执行 | GPU 单节点 | MIG profile、parent UUID、设备身份一致；bind/rebind 成功 | action success 已锁定 |
| E3 | 吞吐与 Valuable Useful GPU | GPU 单节点 | 同模型、数据、seed、节点；采集 GPU active/useful time | throughput 下限已锁定；VUG 待校准 |
| E4 | policy lag、staleness 与 ESS | GPU 单节点 | 注入 policy update delay；采集真实 batch quality | 待校准 |
| E5 | 共置干扰隔离 | GPU 单节点 | 注入 competing workload；执行 share/priority 控制 | 待校准 |
| E6 | lifecycle 动作代价 | GPU 单节点 | pause/checkpoint/offload/reload/resume 全部有 receipt | 待校准 |
| E7 | 故障与事务恢复 | GPU 单节点 | worker exit、response loss 均有 injected/recovered 事件 | action success 已锁定；恢复时间待校准 |
| E8 | 多节点稳定性与收敛 | GPU 多节点 | 至少两节点；node loss 恢复；两侧节点集合一致 | throughput 下限已锁定；收敛质量待校准 |

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

仓库提供 `scripts/tgsrl-hardware-environment-driver`。目标环境从
`configs/hardware/environment.example.json` 派生本地配置，至少指定 Gateway URL、固定
Kubernetes context/namespace、workload Job 模板和容器内 trace 导出命令。driver 的私有状态按
`campaign_id/experiment_id/run_key` 持久化，重复 request ID 只回放同一 receipt。配置文件只引用
凭据所在环境，不允许内嵌 token、password 或 kubeconfig。

仓库内 executor 固定读取各 scenario 的 `execution_plan`，并对每个 baseline/variant 的
warmup/measurement iteration 依次执行 `provision`、`launch`、`verify_device_identity`、
`apply_action`、`inject_fault`、`recover_fault`、`measure`、`stop` 和 `cleanup` 中声明的步骤。
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
- `verify_device_identity` 对账 Scheduler Binding、ResourceClaim allocation + 最新 ResourceSlice
  以及 Pod 内 `nvidia-smi -L`，Full GPU/MIG 分别要求正确 DeviceClass；
- `measure` 执行配置中的容器内只读 trace 导出命令，只接收身份匹配的真实 worker NDJSON；
- `stop` 通过 Gateway lifecycle API；`cleanup` 仅删除该 run 的 JobRunBundle 及其精确命名的
  Job、Workload、ResourceClaim，不使用 label-wide 或 namespace-wide 删除；
- `rebind`、`set_share`、`set_priority` 等自适应动作必须配置环境 hook。hook 只负责向已有
  Runtime/Scheduler observation 入口提交信号并返回 authority receipt，driver 随后等待新的成功
  Scheduler decision；没有 hook 时 fail closed，绝不 patch ResourceClaim 或 Binding；
- 故障注入同样使用显式、按 fault ID 配置的 hook，仓库默认不携带集群破坏命令。

环境 hook 接受 `${REQUEST_PATH}` / `${RESPONSE_PATH}`，response 使用
`tgsrl.io/hardware-driver-hook-response/v1alpha1`。动作 hook 必须回传
`authority: scheduler-observation`；fault hook 必须回传 `authority: target-environment`。
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
保留在 `missing` 中，状态为 `INCOMPLETE`。

## 当前验证边界

仓库 CI 会验证 campaign schema、证据降级、设备身份分叉、故障证据缺失、场景 digest、
日志复制和未校准阈值的 fail-closed 行为。`make gate-cpu-integration` 验证完整本地服务链，
但仍输出 `CPU_INTEGRATION/NOT_RUN`。E1–E8 的真实 GPU、MIG、MPS、Kubernetes、veRL
训练与收敛结果必须在目标环境执行后再导入。
