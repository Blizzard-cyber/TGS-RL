# HAMi vGPU 接入指南

本指南说明 TGS-RL 如何把 Scheduler 选择的 NVIDIA 物理 GPU UUID 和分数份额交给
[HAMi](https://github.com/Project-HAMi/HAMi) 兑现。当前接入复用 HAMi 的公开 Kubernetes
资源与注解协议，不复制 HAMi 源码，也不让 HAMi 取代 TGS-RL 的任务、运行时或决策权威。

> **当前验证边界**：NVIDIA A10 上的单 workload H1 已通过，证明 `0.4` 请求能兑现为
> `40%` core 和 `9211 MiB`，且 Scheduler、HAMi、worker UUID 一致。详见
> [H1 验证记录](../validation/h1-hami-vgpu-2026-09-12.md)。该证据不覆盖双 workload
> 并发隔离、动态改份额或训练收益。

## 解决什么问题

MIG 是部分 NVIDIA 设备提供的硬件切片能力，不是统一调度的前置条件。不支持或未启用
MIG 的设备仍应正常进入资源池：

| 需求 | 推荐资源方式 | 隔离与动态能力 |
|---|---|---|
| 独占一张物理卡 | Full GPU + NVIDIA DRA | 设备独占，UUID 可精确验证 |
| 单卡分数算力与显存 | HAMi vGPU | 软件层共享，适合不支持 MIG 的卡 |
| 动态修改 CUDA 计算份额 | NVIDIA MPS | 需要节点侧 MPS PID、共享目录和硬件读回 |
| 硬件级切片 | MIG + NVIDIA DRA | 仅适用于支持 MIG 且已创建实例的设备 |

TGS-RL 的统一模型是“每张卡独立声明能力”，而不是“整个集群只能选一种 GPU 模式”。

## 责任边界

```mermaid
flowchart LR
  N[nvidia-smi 物理卡] --> S[Scheduler]
  S -->|Binding.device_ids + share| O[Operator]
  H[HAMi node 注册注解] --> O
  O -->|UUID/节点/份额校验| P[Kubernetes Pod 模板]
  P -->|资源与 use-gpuuuid 注解| HS[HAMi Scheduler]
  HS -->|实际分配注解| R[Operator readback]
  R -->|UUID 完全一致| W[managed worker]
  W -->|可见设备再次核对| RT[Runtime observation]
```

| 组件 | 负责 | 不负责 |
|---|---|---|
| TGS-RL Scheduler | 选择设备 UUID、份额、generation，维护 reservation 与 Decision | 重新实现 HAMi 的节点打分或设备注入 |
| TGS-RL Operator | 发现 HAMi inventory，编译 Pod，回读实际 UUID，失败时阻断状态推进 | 在 Scheduler 之外另选一张卡 |
| HAMi | 调度 Pod、分配 vGPU、注入 CUDA 设备并发布分配注解 | 管理 TGS-RL Job/Run/Trace |
| worker bootstrap | 核对进程可见 UUID，注册 PID/control endpoint，上报退出 | 模拟或修改设备分配 |

## 当前支持合同

TGS-RL 的 `hami-vgpu` profile 当前只接受：

- NVIDIA 物理 GPU UUID，格式为 `GPU-...`；
- 一个 Binding 对应一张物理 GPU；
- `accelerator_units` 在 `(0, 1]`；
- HAMi `hami-core` 模式；
- 节点 `hami.io/node-nvidia-register` 中存在完整、健康且唯一的 UUID 记录；
- managed-worker bootstrap 已启用；
- Pod 启动后存在 `hami.io/vgpu-devices-allocated`，且回读 UUID、显存 MiB 和 core
  百分比与 Binding/编译期请求完全一致。

份额向上取整为整数百分比。例如 `accelerator_units: 0.4` 会编译为：

```yaml
metadata:
  annotations:
    nvidia.com/use-gpuuuid: GPU-xxxxxxxx
    nvidia.com/vgpu-mode: hami-core
    tgsrl.io/hami-core-percent: "40"
    tgsrl.io/hami-memory-mib: "9211" # 以 23028 MiB 的卡为例
spec:
  schedulerName: hami-scheduler
  nodeSelector:
    kubernetes.io/hostname: gpu-node-01
  containers:
    - resources:
        requests:
          nvidia.com/gpu: "1"
          nvidia.com/gpucores: "40"
          nvidia.com/gpumem-percentage: "40"
        limits:
          nvidia.com/gpu: "1"
          nvidia.com/gpucores: "40"
          nvidia.com/gpumem-percentage: "40"
```

当前 TGS-RL 使用同一个份额同时约束 core 和 memory percentage。需要分别表达显存与算力时，
应先扩展 `ResourceVector`/Intent 契约，不要在 Operator 中私自推导两个不同数值。

## 集群前置条件

生产集群应由管理员按
[HAMi 官方安装文档](https://project-hami.io/docs/get-started/deploy-with-helm/) 安装并锁定版本。
仓库只为专用单节点 Minikube 验证环境提供可逆的 H1 安装辅助脚本，不会由 Operator 在运行时
隐式安装 HAMi。无论采用哪种方式，至少确认：

1. NVIDIA 驱动、容器运行时和 NVIDIA Container Toolkit 正常；
2. HAMi scheduler、admission webhook 与 device plugin 均为 Ready；
3. 目标 GPU 节点已由 HAMi 纳管；
4. 节点发布 `hami.io/node-nvidia-register`；
5. `nvidia.com/gpu`、`nvidia.com/gpucores` 和
   `nvidia.com/gpumem-percentage` 能被集群识别；
6. Kueue 的 ResourceFlavor/ClusterQueue 已包含上述资源配额；
7. workload Pod 显式使用 `schedulerName: hami-scheduler`，admission webhook 正常；
8. TGS-RL Operator ServiceAccount 可以 `list` Node，并可 `get/list/watch` workload Pod。

先做只读检查：

```bash
kubectl get pods -n kube-system | grep -E 'hami-scheduler|hami-device-plugin'
kubectl get nodes -o custom-columns=\
'NAME:.metadata.name,HAMI_GPU:.status.allocatable.nvidia\\.com/gpu'
kubectl get node <gpu-node> \
  -o jsonpath='{.metadata.annotations.hami\.io/node-nvidia-register}'; echo
```

注册记录必须包含物理 UUID、split count、显存、core limit、型号、健康状态和模式。TGS-RL
支持 HAMi 当前 JSON 格式，也兼容旧的 7/9 字段冒号分隔格式；字段缺失、UUID 重复、设备不健康
或模式不是 `hami-core` 时不会开放 `hami-vgpu` 精确兑现。

## TGS-RL 配置

### Scheduler

NVIDIA Driver v2 推荐使用能力感知模式：

```text
-nvidia-driver-v2
-nvidia-partition-mode=auto
```

`auto` 的规则：

- 未启用 MIG 的卡发布为 Full GPU UUID；
- 已启用 MIG 的卡只发布已存在的 MIG 子设备；
- 不同时发布同一物理卡的整卡和 MIG 容量；
- Full GPU 设备不会声明 MIG 专属的 `rebind/recreate`；
- MPS 不会自动启动，仍需显式选择 `mps`。

Scheduler 只负责设备与份额决策。是否使用 DRA 或 HAMi 兑现由 Operator 根据目标集群
inventory 决定。

### Operator

`-gpu-profile` 接受逗号分隔的有序偏好。推荐在同时提供 DRA 和 HAMi 的集群使用：

```text
-gpu-profile=kubernetes-dra,hami-vgpu
```

选择按每个 Binding 独立进行：

1. 整数份额且 UUID 存在于 DRA inventory 时选择 `kubernetes-dra`；
2. 单卡 `(0,1]` 份额且 UUID 存在于 HAMi inventory 时选择 `hami-vgpu`；
3. 当前候选无法精确兑现时继续尝试下一个 profile；
4. 所有候选均不满足时 fail closed，不退化为按数量随机分配。

HAMi 与 DRA 路径都要求 managed-worker bootstrap。完整参数还需要不可变 bootstrap 镜像、
worker registry URL 和共享 HMAC signing key，见 [Operator 指南](operator.md)。

Helm 建议通过 values 文件设置，避免命令行对逗号做二次解析：

```yaml
operator:
  controller:
    gpuProfile: "kubernetes-dra,hami-vgpu"
    workerBootstrap:
      enabled: true
      installerImage: "registry.example.com/tgsrl/worker-bootstrap@sha256:<digest>"
      registryURL: "http://tgsrl-scheduler:50091"
      registrySigningKeySecret: "tgsrl-worker-registry"
      verifyDeviceIdentities: true
```

若集群只提供 HAMi，可将 `gpuProfile` 设为 `"hami-vgpu"`。旧
`volcano-hami` 仅保留 `volcano.sh/gpu` 数量资源兼容，不具备 Scheduler UUID 的 typed
inventory/readback，不应作为新的精确设备方案。

## 分配与回读

一次 HAMi Binding 的关键证据是：

```text
Scheduler Binding.device_ids
  = Operator 发现的 node registration UUID
  = Pod nvidia.com/use-gpuuuid
  = Pod hami.io/vgpu-devices-allocated
  = bootstrap 在容器内观察到的 UUID
```

查看实际 Pod：

```bash
kubectl get pods -n <namespace> -l job-name=<job-name> -o jsonpath=\
'{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.hami\.io/vgpu-devices-allocated}{"\n"}{end}'
```

Operator 只有在注解中解析到唯一且匹配的 UUID 后，才把资源分配视为已完成。多个 Pod
报告不同集合、注解格式损坏、UUID 不一致或实际 memory/core 份额不等于编译期请求都会返回
错误，阻止 `BOUND/RUNNING`。

## 本地验证

macOS 或无 GPU 开发机只能验证代码合同：

```bash
GOCACHE=/tmp/tgsrl-go-cache go test \
  ./operator-go/compiler \
  ./operator-go/kube \
  ./operator-go/bundleadapter \
  ./operator-go/worker \
  ./scheduler-go/provider/nvidia

UV_CACHE_DIR=/tmp/tgsrl-uv-cache uv run --frozen pytest tests/api -q
npm --prefix console test -- --run
npm --prefix console run test:browser
```

这些测试覆盖 typed inventory、profile fallback、非 MIG 物理卡分数投影、Pod allocation UUID 回读、
异构资源页面和响应式布局，但不能证明真实 CUDA 隔离或共享有效。

## H1 单卡分数 GPU 验证

H1 是独立于正式 E1–E8 发布矩阵的 realization smoke。它只回答：一张物理 NVIDIA GPU 上，
Scheduler 选定的 UUID 和 `0.4` 份额能否被 HAMi 精确兑现，并被真实 CUDA worker 与 Trace
确认。它不证明双 workload 共置隔离、性能收益或模型收敛。

在 E1 已通过、namespace 无运行中 TGS-RL workload 且 GPU 控制面已停止后执行：

```bash
make gpu-down
make gpu-prepare-hami
make gpu-hami-status

export TGSRL_OPERATOR_GPU_PROFILES=hami-vgpu
make gpu-up
make gpu-hami-smoke
```

`gpu-prepare-hami` 从 HAMi 官方 GitHub Release 获取并锁定 chart `2.10.0` 及其 SHA-256；
启用 `TGSRL_NETWORK_PROFILE=cn` 时只替换下载传输地址，仍校验同一摘要。脚本停用 Minikube 自带 NVIDIA
Device Plugin，安装 HAMi、标记测试节点并创建独立 Kueue `hami` queue。脚本拒绝在
namespace 仍有 JobRunBundle、Workload、Job 或 Pod 时切换；安装前记录节点标签、HAMi
注解和原 GPU capacity。安装失败或显式恢复时，只有原 Device Plugin Ready、capacity 恢复且
HAMi 残留注解清理完成，脚本才删除恢复状态并返回成功。

H1 必须同时满足：

```text
Scheduler Binding UUID
= HAMi Node registration UUID
= Pod use-gpuuuid
= Pod allocated UUID
= worker-visible UUID

requested core/memory == HAMi allocated core/memory
```

结果写入：

```text
.cache/tgsrl/hami-smoke/h1-hami-vgpu/report.json
.cache/tgsrl/hami-smoke/h1-hami-vgpu/baseline-trace.json
.cache/tgsrl/hami-smoke/h1-hami-vgpu/variant-trace.json
```

测试完成后先停止控制面，再恢复 DRA 所需的原 NVIDIA Device Plugin：

```bash
make gpu-down
make gpu-restore-dra
```

不要用 MIG 测试阻塞不具备该能力的设备。后续分开验证：

1. **双 workload 共享**：绑定同一物理 UUID、请求互补份额，验证并发隔离和干扰；
2. **MPS**：单独验证 server PID、active-thread percentage 写入/readback 和显存释放边界；
3. **MIG**：获得支持型号后再执行 E2，验证 DeviceClass、parent UUID 和 rebind。

HAMi smoke 的原始输出仍应写入 `.cache/tgsrl/`，只将脱敏报告和必要日志放入 `handoff/`
供开发机复核。测试未运行时保持 `NOT_RUN`，失败时保留原始错误，不把 HAMi Pod 成功调度
等同于训练质量或性能收益。

## 常见故障

| 现象 | 优先检查 |
|---|---|
| Operator 启动时报没有可兑现 profile | `-gpu-profile` 顺序、Node 注册注解、DRA ResourceSlice、bootstrap 配置 |
| Binding UUID 不在 HAMi inventory | Scheduler 与 Kubernetes 是否看到同一物理集群，UUID 是否稳定 |
| 编译提示 HAMi 只支持一张卡 | 当前 Binding 不能用 `hami-vgpu`；把 DRA 放入 fallback，或拆成单卡 RuntimeUnit |
| Pod Pending | HAMi scheduler/webhook、Kueue quota、三个 NVIDIA 扩展资源、node selector |
| Pod Active 但 Runtime 未 RUNNING | `hami.io/vgpu-devices-allocated`、bootstrap readiness、registry token |
| allocation UUID mismatch | 立即停止；检查 `use-gpuuuid`、HAMi 分配注解和旧 Pod，不要绕过核验 |
| 容器看不到预期卡 | CDI/device-plugin 注入与 bootstrap `nvidia-smi -L`；不要只信 Pod phase |
| 非 MIG 设备被判定不可调度 | 不应按型号或 MIG 能力拒绝；检查 Scheduler `auto` 是否发布 Full GPU UUID |

## 尚未支持

- Operator 或生产部署自动安装、升级或配置 HAMi；仓库脚本只服务于专用 H1 Minikube；
- 在一个 Binding 中通过 HAMi 分配多张物理 GPU；
- 分别配置 core percentage 与 memory percentage；
- 由 TGS-RL 操作 HAMi dynamic MIG；
- 将 `volcano-hami` 旧 profile 当作精确 UUID 路径；
- 用本地 mock、Pod phase 或单次 CUDA 成功替代正式性能与收敛实验。
