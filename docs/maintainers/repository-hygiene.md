# 仓库卫生与发布内容

本页定义干净 clone 必须包含什么、哪些内容只能保留在本机。`make check-repository`
执行其中可以确定性检查的规则。

## 必须提交

| 类别 | 路径 | 原因 |
|---|---|---|
| 源码与测试 | `scheduler-go/`、`runtime-python/`、`operator-go/`、`job-controller-go/`、`gateway-python/`、`console/src/`、`tests/` | 产品行为与回归覆盖 |
| Wire contract | `proto/tgsrl/v1/`、`buf.yaml`、`buf.gen.yaml` | 跨语言唯一事实来源 |
| 生成契约 | `gen/go/`、`gen/python/`、`api/openapi.json` | clone 后无需先生成代码即可构建 |
| 依赖锁 | `go.sum`、`uv.lock`、`console/package-lock.json`、`configs/hardware/gpu-requirements.lock` | 控制面与隔离 GPU workload 可复现解析 |
| 运行兼容性 | `compatibility/`、`configs/`、`upstream/` | 能力、策略、场景与 patch 来源 |
| 数据库迁移 | `runtime-python/tgsrl_runtime/storage/migrations/` | 旧状态可确定性升级 |
| 部署契约 | Dockerfiles、`compose.yaml`、`compose.gpu.yaml`、`deploy/` | 本机与 Kubernetes 打包 |
| 开源入口 | `README.md`、`LICENSE`、`CONTRIBUTING.md`、`SECURITY.md`、`.gitattributes` | 首次运行、许可证、贡献、安全与跨平台行为 |
| 脱敏示例 | `.example.*` 文件，例如 `configs/hardware/environment.example.json` | 展示输入结构而不携带真实环境数据 |
| 临时协作文件 | `WORKLOG.local.md`、`handoff/`（脱敏后的 Gate 报告与日志） | 开发机与 GPU 测试机通过 GitHub 互相拉取的临时交接材料；发布前删除 |

`WORKLOG.local.md` 与 `handoff/` 是开发机（无 GPU）和测试机（有 GPU）之间的临时交接约定：
测试机把脱敏后的 E1-E8 报告和日志放入 `handoff/`，开发机拉取后修复。二者是临时脚手架，
确定发布前应删除，并把 `/WORKLOG.local.md` 重新加入 `.gitignore`。真实凭据、kubeconfig、
签名 key 和 `configs/hardware/environment.json` 仍然禁止提交；`evidence/`、`reports/`、
`artifacts/` 仍被 `make check-repository` 拒绝，测试结果统一放入 `handoff/`。

生成的 Proto、OpenAPI 和确定性 SBOM 是有意纳入版本控制的产物。先修改其源文件，再按文档
命令重新生成，并在同一个变更中提交源文件和产物。

## 禁止提交

| 类别 | 示例 |
|---|---|
| 依赖与环境 | `node_modules/`、`.venv/`、`venv/` |
| 缓存 | `.cache/`、`__pycache__/`、`.pytest_cache/`、`.mypy_cache/`、`.ruff_cache/`、`.hypothesis/` |
| 构建与测试输出 | `bin/`、`dist/`、`build/`、coverage、Playwright 报告与测试结果 |
| 运行状态 | SQLite、journal、checkpoint、PID/socket 文件与日志 |
| Gate 证据 | `artifacts/`、`evidence/`、`reports/`、原始 Trace 与下载归档 |
| 真实环境配置 | `.env`、kubeconfig、registry 登录文件、`configs/hardware/environment.json`、`values.production.yaml` |
| Secret | 私钥、证书、凭据、访问令牌和生产签名 key |
| 工作站文件 | `.DS_Store`、IDE 目录、swap 文件 |

提交前执行：

```bash
git status --short
git status --short --ignored
git diff --check
make check-repository
make check-docs
make check-public-content
```

如果被 ignore 的文件实际包含源码，先查明命中规则的原因。除非路径已审查且 ignore 规则已经
收窄或记录，不要使用 `git add -f`。仓库检查会扫描源码和配置根目录下被忽略的类源码文件，
专门拦截“只在作者机器上可运行”的问题。

## Git、Docker、Python 包是三个边界

| 边界 | 权威输入 | 检查重点 |
|---|---|---|
| Git 发布内容 | `.gitignore`、tracked 文件、提交 diff | 凭据、数据库、缓存、无用临时脚本不提交 |
| Docker context | `.dockerignore`、各 Dockerfile 的 COPY | Git ignore 不生效；末尾重新包含 configs 后必须再次排除本机配置 |
| Python sdist/wheel | `pyproject.toml` package-data、包发现 | SQL 迁移和许可证随包发布，不依赖 checkout/egg-info 残留 |

运行打包回归：

```bash
uv run --frozen pytest tests/governance/test_packaging.py
```

它从干净的打包源目录构建 sdist，再从 sdist 构建 wheel，隔离安装后创建数据库、写入和重开读取。
Docker ignore 规则检查不等于实际 BuildKit context 检查；镜像仍需在获授权的构建环境验证。

## 清理原则

- 保留正在使用的 Mock/fake、Synthetic 与 reference workload，它们承担产品演示或契约测试。
- 删除失去调用方且已被新文档承接的重复长文，先更新所有入口链接。
- 不为减少文件数合并职责不同的组件，也不删除生成代码、迁移、锁文件。
- 缓存/依赖/构建输出无需上传，但不是必须现场删除；原始 GPU 证据、数据库和凭据必须保留。
- 避免全仓 `git clean` 或无差别删除 `.cache`。有需要时只清理可再生且已确认归属的具体路径。
- `handoff/` 中仅放脱敏结果摘要和必要日志；原始证据留在测试机，结果记录 source SHA 与复现命令。
