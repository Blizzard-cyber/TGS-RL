# 验证记录

本目录保存已经完成的硬件与集成验证摘要。每份记录都绑定具体 commit、环境、镜像、
命令和证据哈希；结论只适用于记录中声明的范围，不自动外推到其他 GPU 型号、集群拓扑、
训练 workload 或后续代码版本。

## 记录索引

| 记录 | 覆盖范围 | 不覆盖 |
|---|---|---|
| [E1 单节点 Full GPU](e1-full-gpu-2026-09-12.md) | NVIDIA DRA Full GPU、exact UUID、CUDA worker、Trace 和资源清理 | MIG、MPS、多节点、完整训练收益 |
| [H1 HAMi 单 worker](h1-hami-vgpu-2026-09-12.md) | 单 worker 固定分数 GPU 份额兑现和 UUID 回读 | 并发、公平性、OOM、动态份额 |
| [H2 HAMi 双 worker 并发](h2-hami-concurrency-2026-09-13.md) | 两个 worker 共享同一物理 GPU、份额回读和执行重叠 | 隔离上界、动态 share/priority、训练收益 |
| [A10 工程与 Helm 验收](a10-readiness-2026-09-14.md) | 单节点 A10 lifecycle、H2、DRA 恢复 E1、六服务 Helm install/upgrade | 其他集群、MIG/MPS、多节点、生产 HA |
| [E6 与 E5-STATIC 单 A10](e5-e6-single-a10-2026-09-16.md) | E6 lifecycle latency 阈值确认；E5-STATIC 固定 HAMi 份额干扰阈值确认 | 正式 E5 动态 share/priority、完整 veRL 训练、多节点收敛 |

## 使用规则

- 公开能力以[支持范围与限制](../reference/current-capabilities.md)为准。
- 发布验证合同以[E1-E8 硬件验证 Campaign](../design/gate-e1-e8.md)和 `configs/gates/`
  为准。
- 原始运行输出、Trace、日志和集群快照保留在 ignored 的 `.cache/` 或本地证据目录中。
- 新增验证记录必须说明 commit、环境、运行入口、证据等级、关键哈希，以及该记录不能证明的内容。
