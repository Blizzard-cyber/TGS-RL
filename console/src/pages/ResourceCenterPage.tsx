import { useMemo, useState } from 'react';
import type { AcceleratorDevice, AcceleratorMode } from '../api/types';
import { useApiClient } from '../app/apiContext';
import { useQuery } from '../app/hooks';
import { formatTimestamp, simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { Panel, Pill, ShellFrame } from '../components/primitives';

const modeLabels: Record<AcceleratorMode, string> = {
  native: '厂商原生',
  full: '整卡',
  hami: 'HAMi 共享',
  mps: 'MPS 动态份额',
  mig: 'MIG 硬件切片',
};

const modeDescriptions: Record<AcceleratorMode, string> = {
  native: '由对应厂商 Provider 与设备插件定义的资源形态',
  full: '独占物理加速器，适合大模型和强隔离任务',
  hami: '由 HAMi 按显存和算力比例兑现，不依赖 MIG 能力',
  mps: '动态调整 CUDA 计算份额，需节点侧 MPS 进程接线',
  mig: '硬件级切片，要求 GPU 型号与运行环境均支持',
};

const kindLabels: Record<AcceleratorDevice['kind'], string> = {
  gpu: 'GPU',
  npu: 'NPU',
  tpu: 'TPU',
  custom: '自定义加速器',
};

const nvidiaModes: AcceleratorMode[] = ['full', 'hami', 'mps', 'mig'];

function formatBytes(bytes: number) {
  if (!Number.isFinite(bytes) || bytes <= 0) return '未上报';
  return `${(bytes / 1024 ** 3).toFixed(bytes >= 10 * 1024 ** 3 ? 0 : 1)} GiB`;
}

function DeviceCard({
  device,
  selected,
  onSelect,
}: {
  device: AcceleratorDevice;
  selected: boolean;
  onSelect: () => void;
}) {
  const used = Math.max(0, device.capacity - device.allocatable);
  return (
    <button
      type="button"
      className={`resource-device-card${selected ? ' selected' : ''}`}
      aria-pressed={selected}
      onClick={onSelect}
    >
      <header>
        <div className="resource-device-title">
          <span className={`device-health status-${device.health}`} />
          <div>
            <strong>{device.name}</strong>
            <code>{device.id}</code>
          </div>
        </div>
        <Pill tone={device.health === 'ready' ? 'good' : device.health === 'degraded' ? 'warn' : 'critical'}>
          {titleCase(device.health)}
        </Pill>
      </header>
      <div className="resource-device-meta">
        <span>类型 <strong>{kindLabels[device.kind]}</strong></span>
        <span>节点 <strong>{device.node}</strong></span>
        <span>设备内存 <strong>{formatBytes(device.memoryBytes)}</strong></span>
        <span>已分配 <strong>{Math.round(device.utilization * 100)}%</strong></span>
      </div>
      <div className="resource-capacity-bar" aria-label={`已分配 ${Math.round(device.utilization * 100)}%`}>
        <i style={{ width: `${Math.round(device.utilization * 100)}%` }} />
      </div>
      <footer>
        <span>当前调度分区</span>
        <strong>{modeLabels[device.activeMode]}</strong>
        <small>{used.toFixed(2)} / {device.capacity.toFixed(2)} 单元</small>
      </footer>
    </button>
  );
}

export function ResourceCenterPage() {
  const client = useApiClient();
  const [mode, setMode] = useState('ready');
  const [selectedId, setSelectedId] = useState('');
  const [modeFilter, setModeFilter] = useState<'all' | AcceleratorMode>('all');
  const { result, retry } = useQuery(
    (signal) => client.getResources({ filters: toQueryFilters({ mode }), signal }),
    [client, mode],
  );
  const devices = useMemo(
    () => (result.data?.devices ?? []).filter((device) => modeFilter === 'all' || device.availableModes.includes(modeFilter)),
    [modeFilter, result.data?.devices],
  );
  const selected = devices.find((device) => device.id === selectedId) ?? devices[0];
  const allocations = (result.data?.allocations ?? []).filter(
    (allocation) => !selected || allocation.deviceIds.includes(selected.id),
  );
  const nodes = new Set((result.data?.devices ?? []).map((device) => device.node)).size;
  const totalMemory = (result.data?.devices ?? []).reduce((sum, device) => sum + device.memoryBytes, 0);
  const activeAllocations = (result.data?.allocations ?? []).filter((allocation) =>
    allocation.state.includes('ACTIVE'),
  ).length;

  return (
    <ShellFrame
      title="算力资源"
      subtitle="查看 Scheduler 资源账本，并按设备能力选择整卡、共享或硬件切片。"
      actions={
        <div className="control-row">
          <label>
            <span>调度方式</span>
            <select value={modeFilter} onChange={(event) => setModeFilter(event.target.value as typeof modeFilter)}>
              <option value="all">全部方式</option>
              <option value="native">厂商原生</option>
              <option value="full">整卡</option>
              <option value="hami">HAMi 共享</option>
              <option value="mps">MPS 动态份额</option>
              <option value="mig">MIG 硬件切片</option>
            </select>
          </label>
          <SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} />
        </div>
      }
    >
      <SurfaceStateBoundary result={result} retry={retry}>
        {(data) => (
          <>
            <section className="resource-summary" aria-label="算力资源摘要">
              <article><span>加速设备</span><strong>{data?.devices.length ?? 0}</strong><small>{nodes} 个节点</small></article>
              <article><span>设备总内存</span><strong>{formatBytes(totalMemory)}</strong><small>物理与切片容量</small></article>
              <article><span>活动分配</span><strong>{activeAllocations}</strong><small>{data?.pendingUnits ?? 0} 个单元等待</small></article>
              <article><span>资源版本</span><strong>R{data?.revision ?? 0}</strong><small>{data?.observedAt ? formatTimestamp(data.observedAt) : '尚未上报'}</small></article>
            </section>

            <div className="resource-workbench">
              <Panel
                className="resource-inventory-panel"
                title="设备池"
                subtitle="每张卡独立声明调度能力；不支持 MIG 的设备仍可通过整卡或共享方式参与调度。"
                actions={<span className="resource-count">{devices.length} 个可见设备</span>}
              >
                <div className="resource-device-grid">
                  {devices.map((device) => (
                    <DeviceCard
                      key={device.id}
                      device={device}
                      selected={selected?.id === device.id}
                      onSelect={() => setSelectedId(device.id)}
                    />
                  ))}
                  {devices.length === 0 ? (
                    <div className="resource-filter-empty">
                      <strong>当前没有符合条件的设备</strong>
                      <span>该能力可能不适用于当前硬件。请选择其他调度方式，或检查 Scheduler 资源快照。</span>
                    </div>
                  ) : null}
                </div>
              </Panel>

              <aside className="resource-detail-column">
                <Panel title="能力矩阵" subtitle={selected ? `当前选择：${selected.name}；最终方式由 Operator 回读确认` : '请选择设备'}>
                  {selected ? (
                    <div className="capability-matrix">
                      {(selected.kind === 'gpu' && selected.provider.toLowerCase() === 'nvidia'
                        ? nvidiaModes
                        : ['native'] as AcceleratorMode[]
                      ).map((entry) => {
                        const supported = selected.availableModes.includes(entry);
                        const active = selected.activeMode === entry;
                        return (
                          <article key={entry} className={`${supported ? 'supported' : 'unsupported'}${active ? ' active' : ''}`}>
                            <div>
                              <strong>{modeLabels[entry]}</strong>
                              <span>{modeDescriptions[entry]}</span>
                            </div>
                            <em>{active ? '当前使用' : supported ? '可选择' : '不适用'}</em>
                          </article>
                        );
                      })}
                    </div>
                  ) : null}
                </Panel>
                <Panel title="设备身份" subtitle="调度、资源分配与进程注册使用同一身份。">
                  {selected ? (
                    <dl className="resource-identity">
                      <div><dt>设备 UUID</dt><dd>{selected.id}</dd></div>
                      <div><dt>设备类型</dt><dd>{kindLabels[selected.kind]}</dd></div>
                      <div><dt>所在节点</dt><dd>{selected.node}</dd></div>
                      <div><dt>资源提供方</dt><dd>{selected.provider}</dd></div>
                      <div><dt>驱动版本</dt><dd>{selected.driverVersion || '未上报'}</dd></div>
                      <div><dt>能力声明</dt><dd>{selected.capabilities.join(', ') || '未上报'}</dd></div>
                      {selected.profile ? <div><dt>MIG 规格</dt><dd>{selected.profile}</dd></div> : null}
                      {selected.parentUuid ? <div><dt>父卡 UUID</dt><dd>{selected.parentUuid}</dd></div> : null}
                    </dl>
                  ) : null}
                </Panel>
              </aside>
            </div>

            <Panel title="当前资源分配" subtitle="从 Scheduler 资源账本读取，按设备 UUID 与运行单元关联。">
              <div className="table-scroll">
                <table className="data-table resource-allocation-table">
                  <thead><tr><th>任务 / 阶段</th><th>运行身份</th><th>设备</th><th>份额</th><th>设备内存</th><th>状态</th></tr></thead>
                  <tbody>
                    {allocations.map((allocation) => (
                      <tr key={allocation.id}>
                        <td><strong>{allocation.jobId}</strong><span>{allocation.stageId || '未标注阶段'}</span></td>
                        <td><code>{allocation.sandboxId || allocation.runId || allocation.id}</code><span>第 {allocation.generation} 代</span></td>
                        <td><code>{allocation.deviceIds.join(', ')}</code></td>
                        <td>{Math.round(allocation.share * 100)}%</td>
                        <td>{formatBytes(allocation.memoryBytes)}</td>
                        <td><Pill tone={allocation.state.includes('ACTIVE') ? 'good' : 'warn'}>{titleCase(allocation.state)}</Pill></td>
                      </tr>
                    ))}
                    {allocations.length === 0 ? <tr><td colSpan={6} className="table-empty">当前设备没有活动分配。</td></tr> : null}
                  </tbody>
                </table>
              </div>
            </Panel>

            <section className="resource-policy-note">
              <strong>调度原则</strong>
              <span>Scheduler 只选择设备身份、资源与能力，厂商 Provider 和 Operator adapter 负责兑现并回读。当前 NVIDIA 可使用 DRA、HAMi、MPS 或 MIG；同一物理设备不得在多个资源域重复计量。</span>
            </section>
          </>
        )}
      </SurfaceStateBoundary>
    </ShellFrame>
  );
}
