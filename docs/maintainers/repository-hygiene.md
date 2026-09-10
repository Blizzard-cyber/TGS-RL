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
| 工作站文件 | `.DS_Store`、IDE 目录、swap 文件和 `WORKLOG.local.md` |

提交前执行：

```bash
git status --short --untracked-files=all
git status --short --ignored
git diff --check
make check-repository
make check-docs
make check-public-content
```

如果被 ignore 的文件实际包含源码，先查明命中规则的原因。除非路径已审查且 ignore 规则已经
收窄或记录，不要使用 `git add -f`。仓库检查会扫描源码和配置根目录下被忽略的类源码文件，
专门拦截“只在作者机器上可运行”的问题。
