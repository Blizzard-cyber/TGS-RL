export const dataKindOptions = [
  { value: 'all', label: '全部数据' },
  { value: 'live', label: '真实运行' },
  { value: 'replay', label: '回放数据' },
  { value: 'synthetic', label: '合成数据' },
] as const;

export const simulationOptions = [
  { value: 'ready', label: '正常' },
  { value: 'empty', label: '空数据' },
  { value: 'error', label: '错误' },
  { value: 'forbidden', label: '无权限' },
  { value: 'gpu-unavailable', label: '加速卡不可用' },
  { value: 'degraded', label: '部分降级' },
] as const;

export function toQueryFilters(input: Record<string, string | undefined>) {
  return Object.fromEntries(Object.entries(input).filter((entry): entry is [string, string] => Boolean(entry[1])));
}

export function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    timeZone: 'Asia/Shanghai',
  }).format(new Date(value));
}

const labels: Record<string, string> = {
  healthy: '健康',
  ok: '正常',
  degraded: '降级',
  stalled: '停滞',
  pending: '等待中',
  running: '运行中',
  paused: '已暂停',
  stopped: '已停止',
  succeeded: '已成功',
  failed: '失败',
  cancelled: '已取消',
  terminated: '已终止',
  requested: '已请求',
  bound: '已绑定',
  sleeping: '休眠中',
  unknown: '未知',
  progressing: '处理中',
  ready: '就绪',
  busy: '繁忙',
  down: '离线',
  info: '信息',
  warn: '警告',
  critical: '严重',
  applied: '已应用',
  active: '使用中',
  releasing: '释放中',
  released: '已释放',
  fallback: '回退',
  selected: '已选择',
  feasible: '可行',
  sync: '同步',
  partially_async: '部分异步',
  fully_async: '完全异步',
  simulation: '模拟',
  replay: '回放',
  live: '真实运行',
  synthetic: '合成数据',
  bind: '绑定',
  release: '释放',
  set_share: '调整份额',
  set_priority: '调整优先级',
  resize: '调整资源',
  pause: '暂停',
  resume: '恢复',
  sleep: '休眠',
  offload: '卸载',
  rebind: '重新绑定',
  recreate: '重建',
  rolled_back: '已回滚',
  skipped: '已跳过',
  job: '任务',
  queue: '队列',
  sandbox: '沙箱',
  device: '设备',
  runtime: '运行单元',
  feeds: '输入',
  'runs-in': '运行于',
  'scheduled-on': '调度至',
  'blocked-by': '受阻于',
  'phase-started': '阶段开始',
  'phase-completed': '阶段完成',
  'sample-produced': '样本生成',
  'sample-consumed': '样本消费',
  'policy-published': '策略发布',
  backpressure: '背压变化',
  'safe-point': '到达安全点',
  'decision-applied': '决策应用',
  decision_applied: '决策应用',
  phase_started: '阶段开始',
  phase_completed: '阶段完成',
  sample_produced: '样本生成',
  sample_consumed: '样本消费',
  policy_published: '策略发布',
  backpressure_changed: '背压变化',
  safe_point_reached: '到达安全点',
};

export function titleCase(value: string) {
  const normalized = value
    .replace(/^JOB_RUN_STATE_/, '')
    .replace(/^JOB_STATE_/, '')
    .replace(/^RUNTIME_STATE_/, '')
    .replace(/^EXPERIMENT_STATE_/, '')
    .replace(/^ALLOCATION_STATE_/, '')
    .replace(/^DEVICE_HEALTH_/, '')
    .toLowerCase();
  return labels[normalized] ?? value.replace(/[_-]/g, ' ');
}

export function dataKindLabel(value: string) {
  return labels[value.toLowerCase()] ?? value;
}

export function commandLabel(value: string) {
  const commandLabels: Record<string, string> = {
    start: '启动',
    pause: '暂停',
    resume: '恢复',
    stop: '停止',
    retry: '重试',
    terminate: '强制终止',
  };
  return commandLabels[value] ?? titleCase(value);
}
