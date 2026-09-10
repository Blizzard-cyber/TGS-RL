# 单机 GPU 全链路 Smoke

本指南用于一台全新的 Linux GPU 机器。目标不是测毕业设计性能，而是先获得一条可复现的
端到端证据：

```text
浏览器 / CLI
  -> Gateway -> Job Controller -> Runtime
  -> Scheduler 选择真实 GPU UUID
  -> Operator 创建 Kueue Workload + ResourceClaim + Job
  -> NVIDIA DRA 分配同一 UUID
  -> bootstrap 校验依赖和可见 UUID并注册真实 PID
  -> CUDA worker 上报 Trace
  -> Runtime / Console 展示结果
```

首轮只验证 **单节点、单 worker、单张 Full GPU、E1**。它刻意不打开 MIG、MPS、多节点和性能阈值，
避免在第一次联调中混入多个独立变量。
这里的 worker 会加载真实 `verl/ray/torch/vllm` 包、运行真实 CUDA matmul 并走第一方 veRL
control/trace adapter，但使用最小 `SmokeTrainer`，不是完整模型训练；训练收益和收敛实验仍属于
后续 E3–E8。

## 1. 主机要求

| 项目 | 硬要求 | 预检方式 |
|---|---|---|
| 操作系统 | Ubuntu 22.04/24.04，x86_64 | `uname -m`、`/etc/os-release` |
| GPU | 一张或多张 NVIDIA GPU；首轮 workload 只申请一张 | `nvidia-smi -L` |
| 驱动 | 580.95.05 或更高（CUDA 13.0 Update 2） | `nvidia-smi` |
| 磁盘 | 建议至少 150 GiB 可用 | `df -h` |
| 内存 | 建议至少 32 GiB | `free -h` |
| 网络 | 可访问 GitHub、Docker Hub、registry.k8s.io、get.helm.sh | 安装/拉取阶段 fail-fast |
| 镜像仓库 | 机器与 Minikube 节点都能 pull/push | 使用你自己的 `TGSRL_IMAGE_REGISTRY` |

首轮仍是单节点、单 worker、单个 Full GPU claim。Scheduler 可以发现主机上的多张卡并从中
选择一张；这不等于多节点或多 worker 验证。

## 2. 克隆后安装工具

```bash
git clone https://github.com/Blizzard-cyber/TGS-RL.git
cd TGS-RL
make gpu-install-host
```

NVIDIA 580.95.05+ 内核驱动必须由机器提供方提前安装。默认情况下，该命令不会替换 Docker 或 NVIDIA
Container Toolkit；缺少时会停止并给出显式开关：

```bash
TGSRL_INSTALL_DOCKER=1 TGSRL_INSTALL_NVIDIA_TOOLKIT=1 make gpu-install-host
```

脚本随后安装并校验 kubectl 1.35.1、Minikube 1.38.1、Helm 4.2.4、uv 0.12.7，生成
NVIDIA CDI spec，并用 `uv.lock` 准备 Python 3.12.14 环境。kubectl、Minikube 和 Helm
下载都做 SHA-256 校验。安装会调用 `sudo`，且可能配置 Docker/NVIDIA apt source；安装后
重新登录一次，确认当前用户可以直接执行 `docker info`。该步骤不会拉取项目 Docker 镜像。
如果 Docker 与 Toolkit 已经由机器管理员准备好，直接执行无开关版本即可；脚本只完成配置、
校验和其余锁定工具安装。若两者都缺失，再使用上面的双开关命令。

## 3. 创建本地 GPU 集群

```bash
make gpu-create-cluster
make gpu-prepare-cluster
```

第一条命令创建 GPU-enabled Minikube；第二条安装锁定版本：

- Kubernetes `v1.35.1`；
- Kueue `v0.19.2`；
- NVIDIA DRA driver `0.5.0`；
- `tgsrl-system` namespace；
- DRA DeviceClass 到 `tgsrl.io/gpu` 的 Kueue quota mapping；
- `default` LocalQueue、ClusterQueue 和 ResourceFlavor。

这些版本来自 `compatibility/bom/runtime.yaml` 和官方兼容要求：NVIDIA DRA 0.5.0 要求
Kubernetes 1.34.2+、NVIDIA 580+ 驱动与支持 CDI 的 runtime；本指南进一步固定到
Kubernetes 1.35.1、CUDA 13.0 Update 2 所需的 580.95.05+ 驱动。

`gpu-create-cluster` 使用 Minikube 的 NVIDIA CDI 模式（`--gpus=nvidia.com`）。
`gpu-prepare-cluster` 会下载 Kueue/NFD/DRA chart 与镜像，因此应在具备外网或镜像代理的
环境中显式执行；仓库不会在 clone、doctor 或普通测试时自动拉取它们。

然后创建仅供本机外置 Operator 使用的 24 小时短期凭据：

```bash
make gpu-configure-access
```

凭据写入 `.cache/tgsrl/gpu-kubeconfig`，已被 `.gitignore` 排除。它只拥有 TGS-RL
namespace 写权限和 DRA/Node discovery 只读权限。不要提交或复制该文件。

## 4. 准备不可变镜像

默认基础镜像是仓库中固定 digest 的 CUDA 13.0.2 devel 镜像。也可以传入另一个经过人工
核对的 CUDA 基础镜像，但必须使用 digest。workload 内的 `verl==0.9.0`、`ray==2.58.0`、
`torch==2.13.0+cu130` 和 `vllm==0.28.0` 由 hash lock 安装；如果目标 GPU/CUDA 不支持这组
版本，应先更新 compatibility BOM、输入约束和 lock，而不是临时跳过版本检查。

```bash
export TGSRL_IMAGE_REGISTRY=registry.example.com/your-user/tgsrl
export DOCKER_CONFIG=$PWD/.cache/tgsrl/docker
mkdir -p "$DOCKER_CONFIG"
docker login registry.example.com
# 可选：export TGSRL_VERL_BASE_IMAGE=registry.example.com/approved/cuda@sha256:<64-hex>
make gpu-build-images
make gpu-configure-registry
```

`gpu-build-images` 只构建并推送 bootstrap 和 GPU smoke workload；六个控制面服务由
`compose.yaml` 在宿主机本地构建。脚本把推送后的 immutable digest 写入
`.cache/tgsrl/gpu-images.env`。`gpu-configure-registry` 把专用 `DOCKER_CONFIG` 中的凭据
复制为 namespace imagePullSecret；它不会读取或改写默认的 `~/.docker/config.json`。
如果仓库确实允许匿名拉取，可以跳过该命令，并在预检前显式设置
`TGSRL_PUBLIC_IMAGE_REGISTRY=1`。

这里没有浮动的 veRL tag。测试证据必须能指出实际执行的 workload digest；Dockerfile 会
执行 `pip check` 和版本校验，bootstrap 也会在 worker 启动前再次检查
`verl/ray/torch/vllm`。GPU workload 使用独立依赖环境，因为 vLLM 0.28.0 的 CUDA 13 依赖链
要求 protobuf 6.x，而 TGS-RL 控制面仍锁定 protobuf 5.29.x。两者只通过 protobuf wire
contract 和 HTTP registry 通信，不共享 Python site-packages。

## 5. 生成配置并启动前后端

配置脚本默认从 Minikube 节点的 default route 推导 Pod 可访问的宿主机地址；特殊网络下
可以显式覆盖 `TGSRL_HOST_GATEWAY`，但不能填写 `127.0.0.1`：

```bash
make gpu-render-config
set -a
source .cache/tgsrl/gpu-runtime.env
set +a
make gpu-preflight
make gpu-up
```

`gpu-preflight` 默认使用 `--pull=never` 检查 Docker GPU runtime，不会静默下载测试镜像。
第一次运行前显式拉取 BOM 中固定 digest 的 CUDA base，或设置
`TGSRL_ALLOW_IMAGE_PULL=1` 允许该预检下载；要改用另一镜像时通过
`TGSRL_GPU_RUNTIME_TEST_IMAGE=repository@sha256:...` 覆盖。确实不需要独立 Docker GPU check 时可显式
设置 `TGSRL_SKIP_DOCKER_GPU_CHECK=1`，但最终 DRA workload 仍必须通过。`gpu-preflight` 可以在 `gpu-up` 前执行；
`gpu-up` 启动完成后还会单独检查 registry `/healthz`。

默认预检镜像可显式准备为：

```bash
docker pull nvidia/cuda:13.0.2-base-ubuntu24.04@sha256:2ab6381d970b211fb93853796dc6707eb8a72575a375c422b17cf4d8b2641701
```

`gpu-up` 在 Linux host network 上启动六个组件。Scheduler 只读挂载主机 NVIDIA 设备和
library，用 `full` 模式发现真实 UUID；Operator 使用短期 kubeconfig 创建 workload；Pod 通过
`${TGSRL_WORKER_REGISTRY_URL}` 回连 Scheduler registry。Console 地址：
<http://127.0.0.1:4173>。
为避免同一状态目录上启动两套写入者，`gpu-up` 遇到已运行控制面会 fail closed；先用
`make gpu-status` 查明状态。历史 Kubernetes 对象由 generation、run identity 和 ownership label
隔离，遇到残留对象时先归档并让 campaign cleanup 处理，不要直接批量删除不明对象。

如果 `gpu-preflight` 失败，不要继续。它检查：

- 主机 `nvidia-smi` 和 Docker GPU runtime；
- NVIDIA CDI、Kubernetes/Kueue/DRA 锁定版本；
- `gpu.nvidia.com` DeviceClass；
- ResourceSlice 中 `type=gpu` 与非空 UUID；
- namespace、LocalQueue、imagePullSecret 和外置 Operator 实际 RBAC。

## 6. 执行 E1 全链路 smoke

```bash
make gpu-status
make gpu-smoke
```

成功条件不是“Pod Running”这么简单，而是同时满足：

1. Gateway 创建、准入并启动真实 Job/Run；
2. Scheduler 产生非 fallback `bind` Decision；
3. Binding 中是 `GPU-…` UUID；
4. ResourceClaim 使用 `gpu.nvidia.com` 并分配同一 UUID；
5. Pod Ready；bootstrap 注册 PID、generation、binding 和 worker endpoint；
6. worker 内 `nvidia-smi -L` 只看到同一 GPU；
7. CUDA matmul 真正执行，worker trace 含 `sample_consumed` 与 `workload_completed`；
8. stop 通过服务 API 完成，Operator 清理对应资源；
9. `.cache/tgsrl/gpu-smoke/` 形成可归档 report、trace 和日志。

查看现场：

```bash
kubectl get workload,resourceclaim,job,pod -n tgsrl-system -o wide
kubectl describe workload -n tgsrl-system
docker compose -f compose.yaml -f compose.gpu.yaml logs --tail=200
```

## 7. 停止、重跑与故障定位

```bash
make gpu-down
```

该命令保留 named volumes 和 `.cache/tgsrl/gpu-smoke` 证据。若要重建验证基线，请先归档
证据，再显式执行本地 reset；不要在未保存证据时删除 volumes。

| 症状 | 优先检查 |
|---|---|
| `nvidia-smi not found` | Compose GPU override 是否挂载主机 binary/library |
| Workload 一直 Pending | Kueue `deviceClassMappings`、LocalQueue、ClusterQueue quota |
| ResourceClaim Pending | DRA driver Pod、DeviceClass、ResourceSlice typed attributes |
| `InvalidImageName` | `artifactUri` 是否为 `repository@sha256:…` |
| `ImagePullBackOff` | 专用 `DOCKER_CONFIG` 是否登录、`make gpu-configure-registry` 是否成功 |
| bootstrap missing module | workload image 内 `verl/ray/torch/vllm` 版本或安装失败 |
| bootstrap registration timeout | Pod 到宿主机 `:50091` 网络、host firewall |
| device identity mismatch | Scheduler、DRA allocation、worker `nvidia-smi -L` 三方 UUID |
| 有 Trace 但不是 GPU 证据 | 确认 `dataKind=LIVE`、CUDA 可用且 evidence 为 `GPU_SINGLE_NODE` |

## 8. 首轮通过后的顺序

E1 通过只证明全流程打通，不代表毕业实验结论成立。下一步按顺序执行：

```text
E2 MIG identity
  -> E3 throughput / VUG calibration
  -> E4 staleness / ESS
  -> E5 co-location interference
  -> E6 pause/checkpoint/offload/reload cost
  -> E7 recovery
  -> E8 multi-node convergence
```

其中 E3–E8 的 `threshold: null` 必须用真实 baseline 数据标定并人工审定；首轮 smoke 不会
自动把这些门禁改成 PASS。
