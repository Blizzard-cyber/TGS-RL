# 维护者开发指南

本页面向修改源码、协议或构建流程的维护者。运行产品栈请先阅读
[快速上手](../getting-started.md)。第一次修改核心调用链前，先阅读
[源码导读与维护边界](code-walkthrough.md)。

## 常用检查

```bash
make test             # Go、Python、API、Console 与 Proto round-trip
make lint             # Go vet、Buf lint、Ruff 与 mypy
make race             # Go race detector
make test-performance # 非 race Scheduler/Provider P95 回归预算
make demo             # Python → Scheduler → Mock Provider 最小进程演示
make product-e2e      # 完整后端产品流与恢复检查
make check-generated  # 验证 Proto 生成物
make check-governance # 校验 SBOM、兼容性证据与 patch ledger
make check-public-content # 扫描工作树与可达历史中的私有链接、路径和凭据样式
```

Scheduler benchmark 可用于分析算法变化：

```bash
go test ./scheduler-go/scheduler -run '^$' -bench BenchmarkEvaluateSimulation -benchmem
```

`make test-performance` 通过 `performance` build tag 在独立、非 race 进程中执行稳定的
Scheduler 与 Provider P95 回归预算。它只用于阻断明显代码退化，不是产品 SLA，也不能替代
锁定 workload 的真实环境 Gate 数据。普通 `make test`/`make race` 不执行墙钟断言。

完整本地回归：

```bash
git diff --check
make lint
make test
make race
make test-performance
make demo > /tmp/tgsrl-demo.json
make product-e2e
make check-generated
docker compose config -q
make check-governance
make check-public-content
```

`make check-public-content` 同时扫描工作树与全部可达历史；合并前默认扫描必须通过。CI 的
governance job 使用完整历史（`fetch-depth: 0`）执行同一检查，不接受仅扫描当前工作树的
结果替代。

`make demo` 只覆盖最小调度闭环；`make product-e2e` 覆盖完整后端流程但不启动浏览器
Console。Compose 或[快速上手](../getting-started.md)中的手动命令可启动六个组件。

## 代码入口

| 系统 | 主要目录 | 可执行入口 |
|---|---|---|
| Job & Product Control | `gateway-python/`、`job-controller-go/`、`console/` | `tgsrl-gateway`、`job-controller-go/cmd/job-controller`、Vite |
| Runtime, Trace & Experiments | `runtime-python/`、`adapters/` | `tgsrl-runtime` |
| Scheduling & Infrastructure Control | `scheduler-go/`、`operator-go/`、`cmd/operator/` | Scheduler main、Operator main |
| Shared contracts and storage | `proto/`、`gen/`、`storage/` | Buf generators and repository packages |

具体启动入口以 `Makefile` 和各命令的 `--help` 为准。

## 修改 Proto

`proto/tgsrl/v1/` 是跨语言消息的唯一来源，不要手工编辑 `gen/`。

```bash
make proto
git diff -- gen/
make check-generated
make proto-roundtrip
```

兼容规则：

- 新增字段使用新的 field number；
- 删除字段时同时 reserve number 和 name；
- 枚举保留 `UNKNOWN = 0`；
- 破坏性变更必须更新 schema release 并运行 Buf breaking check；
- Go 与 Python 对同一契约的校验语义必须一致；
- 不维护与 Proto 同义的手写影子 DTO。

## 数据与迁移

- Scheduler 的 checkpoint/journal、Job Controller 的 snapshot/journal 和 Operator cursor
  都应使用原子替换并保持向后可诊断的失败行为。
- Runtime SQLite schema 位于 `runtime-python/tgsrl_runtime/storage/migrations/`；新增迁移后
  运行 `scripts/check-migrations.sh`。
- 持久化测试必须覆盖重启读取、损坏输入、幂等写入和 cursor 边界。
- 不应把“数据已落盘”表述为“工作流一定自动续跑”；恢复语义要逐组件验证。

## 依赖与生成物

- Go 依赖由 `go.mod` / `go.sum` 锁定；
- Python 依赖由 `pyproject.toml` / `uv.lock` 锁定；
- Console 依赖由 `console/package.json` / `console/package-lock.json` 锁定；
- 工具链与运行时依赖记录在 `compatibility/bom/runtime.yaml`；
- 下游 patch 登记在 `upstream/PATCHES.md`；
- 不使用 `latest` 等浮动版本表达可复现构建。

## CI

CI 分别验证：

- Proto lint、生成物一致性和兼容性；
- Scheduler、Job Controller、Operator、共享 storage 的 Go 测试与 race；
- 独立非 race Scheduler/Provider P95 回归预算；
- Runtime、Adapter、Gateway/SDK/HTTP API 的 Python lint、类型检查与测试；
- Console 的类型检查、lint、测试和构建；
- 跨语言 Proto round-trip 与最小进程演示；
- 完整后端产品流、重启恢复和幂等性；
- SBOM、兼容性证据、patch ledger 与公开内容检查。

这些检查证明本地契约和控制流，不代表真实 GPU、Kubernetes 集群或训练性能已经验证。

## 文档约定

- README 和 `docs/guides/` 只描述用户当前可执行的行为；
- 架构、状态权威和恢复边界写入 `docs/design/`；
- 可用性与验证级别以 `docs/reference/current-capabilities.md` 为准；
- 公开文档不得包含内部链接、内部文档 ID、访问凭据、个人信息或私有环境数据。

## 提交边界

- 逐文件暂存实现、长期回归测试和对应文档，不使用 `git add .`；
- 测试应保护原子性、幂等、状态机、安全、恢复或协议等长期契约；失败时必须能定位一个
  需要维护的行为，而不是只证明测试替身、固定文案或当前 JSON 常量没有变化；
- 只服务于一次调试、已被现有门禁覆盖、仅观察输出或依赖本机 wall-clock 的临时测试不提交；
  同类小测试优先并入已有文件，开发期脚本验证完成后删除；
- 测试 fake 默认放在 `_test.go` 或测试目录。`job-controller-go/runtimeclient/fake.go` 是共享给
  Controller 与 Service 两个测试包的有意例外；不要再把单包 fake 加入生产源码；
- `WORKLOG.local.md`、`.cache/`、`.tmp/`、`bin/`、虚拟环境、依赖目录、构建目录、
  coverage、trace、数据库、journal 和日志属于本地产物；
- 提交前同时查看 `git status --short`、`git diff --stat`、`git diff --check` 和
  `git status --short --ignored`，确认没有漏掉源码，也没有带入本地产物。
