# 快速上手

本指南让你在本机启动六个服务，并完成第一个任务的生命周期。默认是 **CPU Mock Provider +
fake Operator**：请求经过真实服务，但 workload 是逻辑对象，不执行模型训练或 GPU 命令。

需要理解系统再运行，可先看[系统设计](design/system-design.md)；无 Docker 的源码开发方式见
[开发指南](maintainers/development.md)，GPU 主机使用[GPU Smoke 指南](guides/gpu-smoke.md)。

## 1. 准备与启动

需要 Git、Docker Engine 和 Docker Compose v2。执行 `docker info` 确认 daemon 可用，
执行 `docker compose version` 确认 Compose 可用。宿主机不需要 Python、Go、Node.js 或 CUDA。

```bash
git clone https://github.com/Blizzard-cyber/TGS-RL.git
cd TGS-RL
docker compose up -d --build --wait
docker compose ps
```

首次启动会拉取镜像和安装依赖。失败时先看报错及 `docker compose logs --tail=100`，
不要通过删除数据卷来解决依赖或网络问题。

| 入口 | 用途 |
|---|---|
| <http://127.0.0.1:4173> | 中文 Console |
| <http://127.0.0.1:8080/health> | Gateway 依赖诊断 |
| <http://127.0.0.1:4173/openapi.json> | 同源 OpenAPI 文档 |
| <http://127.0.0.1:9090/metrics> | Scheduler 指标 |

所有宿主机端口绑定 loopback。请勿直接把未鉴权的 Gateway/gRPC 暴露到公网。

## 2. 先跑一次完整检查

```bash
docker compose --profile tools run --rm --no-deps smoke
```

通过标准是输出 `"status": "ok"`，而不只是容器显示 healthy。检查包含：

```text
创建任务 → 准入 → 启动 → 暂停 → 恢复 → 停止
                    ├─ Scheduler Decision
                    ├─ 两个 Sandbox 终态
                    ├─ Synthetic 多轨 Trace 写入与查询
                    └─ Scheduler allocation 回收
```

脚本使用 [`configs/cpu-job.example.json`](../configs/cpu-job.example.json) 创建独立任务，
不会删除已有用户任务。注入的 Trace 明确标为 `DATA_KIND_SYNTHETIC`，用于验证展示和关联，
不代表真实推理时长或 GPU 利用率。

## 3. 自己提交第一个任务

### 方式 A：中文 Console

1. 打开任务中心，进入创建任务的 JSON 输入。
2. 粘贴 `configs/cpu-job.example.json` 的完整内容，校验后创建。
3. 对新任务执行准入，得到一次 Run；再对该 Run 执行启动。
4. 查看 Run、Operation、Sandbox 和 Decision，等待观察态进入 `RUNNING`。
5. 每次等待前一操作完成后，再执行暂停、恢复或停止。

### 方式 B：容器内 CLI

下面命令在已挂载示例和 Python 源码的 Runtime 容器内运行 CLI，通过服务名访问 Gateway，
不在容器内重新安装项目。先在当前终端定义快捷函数：

```bash
tgsrl_local() {
  docker compose exec -T \
    -e PYTHONPATH=/workspace/gateway-python:/workspace/gen/python \
    runtime python -m tgsrl_gateway "$@" --base-url http://gateway:8080
}
```

创建操作的幂等键固定，重复执行会返回同一结果；想创建另一个任务时使用新键，并修改示例名称。

```bash
tgsrl_local validate-job \
  --job configs/cpu-job.example.json \
  --idempotency-key quickstart-validate-1

tgsrl_local create-job \
  --job configs/cpu-job.example.json \
  --idempotency-key quickstart-create-1
```

把创建响应的 `job.jobId` 填入变量，再准入：

```bash
JOB_ID='<创建响应中的 job.jobId>'
tgsrl_local admit-job "$JOB_ID" \
  --idempotency-key quickstart-admit-1
```

把准入响应的 `operation.runId` 填入变量，再启动：

```bash
RUN_ID='<准入响应中的 operation.runId>'
tgsrl_local job-command "$JOB_ID" "$RUN_ID" start \
  --idempotency-key quickstart-start-1

tgsrl_local list-runs "$JOB_ID"
tgsrl_local list-operations \
  --job-id "$JOB_ID" --run-id "$RUN_ID"
```

**命令被接受不等于已启动。** 应同时看到 Run 为 `JOB_RUN_STATE_RUNNING`、对应 Operation 为
`OPERATION_STATE_SUCCEEDED`，以及 Sandbox 的真实观察态收敛。出现 FAILED 时检查错误详情；
长时间处于过渡态时检查 Operator 和后端，不能反复换幂等键重发来掩盖问题。

结束示例任务：

```bash
tgsrl_local job-command "$JOB_ID" "$RUN_ID" stop \
  --idempotency-key quickstart-stop-1
```

完整命令、HTTP 和 SDK 示例见[接口指南](guides/api-and-console.md)。

### 示例字段应如何理解

| 字段 | 本机示例 | 真实训练时 |
|---|---|---|
| `runtime` 组件 | `fake` | 已适配的框架、执行后端、trainer 与 rollout engine |
| `imageDigest` | 合成 digest，不可拉取 | 与 `artifactUri=repository@sha256:...` 一致的真实镜像 |
| `executionContract` | 单阶段、Synthetic 合同 | 由 adapter 构造并生成 canonical contract ID；勿手改内容后保留旧 ID |
| `desiredUnits` | 2 | 所需可调度实例数；逻辑角色可展开多个副本 |
| `resourcesPerUnit` | CPU 与内存 | 经 Provider 和目标 backend 支持的资源 |
| `dataKind` | `DATA_KIND_SYNTHETIC` | 按数据真实来源填写，不通过改标签升级证据 |

## 4. 查看 Trace 和资源

- **任务中心**：Run 状态、Operation 与操作错误。
- **链路追踪**：训练/请求/执行器/worker 多轨时间轴及 span 关联。
- **事件时间线**：产品生命周期与运行事件；不是所有 JobEvent 都是训练 span。
- **调度决策**：候选、拒绝原因、Plan、ActionResult 和 fallback。
- **运行沙箱**：具体 Sandbox、Binding、generation 与设备。
- **算力资源**：Scheduler 的设备账本；默认 CPU 模式没有 NVIDIA 卡是正常现象。

手动创建 fake 任务不会自动产生真实训练 span。想检查丰富时间轴，先执行第 2 步的 smoke；
想获得真实 worker Trace，使用 CPU process Gate 或 GPU workload。缺少 duration 的事件显示为
时间点，不会被补成虚构耗时。

## 5. 停止、重启与数据

```bash
docker compose stop                  # 保留容器和数据
docker compose up -d --wait           # 再次启动
docker compose --profile tools run --rm --no-deps smoke
```

四个 named volumes 保存 Scheduler、Runtime、Job Controller、Operator 状态。
`docker compose down` 删除容器/网络但保留这些卷；仅在确认放弃所有状态时使用
`CONFIRM_RESET=1 make local-reset`。不要删除数据来绕过恢复失败。

fake backend 对象位于内存，重启不等于复活之前的训练进程。恢复后仍应核对 Operation、
Sandbox、Decision 和 allocation；组件恢复职责见[配置与恢复](guides/configuration-and-recovery.md)。

## 常见问题

| 现象 | 检查方式 |
|---|---|
| Docker daemon 不可达 | 启动 Docker Engine/Desktop，确认 `docker info`；仅安装 CLI 不够 |
| 镜像/依赖下载失败 | 检查代理、registry 与 DNS；不要通过换随机版本绕过锁文件 |
| Gateway `degraded` | 核对依赖地址；Runtime 空资源探测可能返回 `grpc_probe:unknown`，继续检查实际操作 |
| Job 创建后没有 Decision | 创建不是启动；先准入，再对 Run 执行 start |
| 任务停在 STARTING | 查看 Operator 日志、Sandbox、Decision；health 不覆盖整个控制链 |
| Trace 为空 | 检查数据来源、Run 筛选和是否上报 worker Trace；fake 任务不是训练进程 |
| 端口冲突 | 默认占用 4173/8080/9090/50051/50061/50071/50081；不同 project name 仅隔离卷/网络，不改变端口 |
| 多份 clone 同时运行 | 在 Compose override 中修改所有宿主机 published ports；容器内 target 保持服务名和原端口 |

## 下一步

- [毕设推进状态](project-progress.md)：查看工程、实验和论文分别推进到哪一步。
- [系统设计](design/system-design.md)：理解职责、身份与状态模型。
- [开发指南](maintainers/development.md)：从源码启动，执行真实子进程验证。
- [GPU Smoke](guides/gpu-smoke.md)：在 NVIDIA 主机验证全链路。
- [A10 Readiness](guides/a10-readiness.md)：在毕业设计实验前验证 lifecycle、共享、恢复与 Helm。
- [部署指南](guides/deployment.md)：了解 Kubernetes/Helm 的实际前置条件。
