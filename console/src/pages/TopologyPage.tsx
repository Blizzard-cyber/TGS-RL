import { useMemo, useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery, useResolvedRunScope } from '../app/hooks';
import { formatTimestamp, simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { Panel, Pill, ShellFrame } from '../components/primitives';

export function TopologyPage() {
  const client = useApiClient();
  const [mode, setMode] = useState('ready');
  const [showGpuOnly, setShowGpuOnly] = useState(false);
  const { jobId, runId, setRunId, setScopedParams, jobsQuery } = useResolvedRunScope(client, mode);
  const { result, retry } = useQuery((signal) => jobId ? client.getTopology(jobId, { filters: toQueryFilters({ mode, require_gpu: showGpuOnly ? 'true' : undefined, run_id: runId || undefined }), signal }) : Promise.resolve({ state: 'empty' as const, message: '输入任务编号后即可查看资源拓扑。' }), [client, jobId, mode, showGpuOnly, runId]);
  const filteredNodes = useMemo(() => { const nodes = result.data?.nodes ?? []; return showGpuOnly ? nodes.filter((node) => node.gpu) : nodes; }, [result.data?.nodes, showGpuOnly]);
  const visibleNodeIds = new Set(filteredNodes.map((node) => node.id));
  const allNodeIds = new Set((result.data?.nodes ?? []).map((node) => node.id));
  const filteredEdges = (result.data?.edges ?? []).filter((edge) => allNodeIds.has(edge.from) && allNodeIds.has(edge.to) && visibleNodeIds.has(edge.from) && visibleNodeIds.has(edge.to));
  const topologyLayers = [
    { kind: 'queue', label: '调度队列', description: '准入与优先级' },
    { kind: 'job', label: '训练任务', description: '策略与运行范围' },
    { kind: 'sandbox', label: '运行沙箱', description: '进程与资源边界' },
    { kind: 'device', label: '计算设备', description: '最终资源落点' },
  ] as const;
  return <ShellFrame title="资源拓扑" subtitle="沿任务、运行单元、沙箱和设备查看调度关系与资源压力。" actions={<div className="control-row"><label><span>任务</span><select aria-label="任务编号" value={jobId} onChange={(event) => { const selected = jobsQuery.result.data?.find((job) => job.id === event.target.value); setScopedParams({ jobId: selected?.id || undefined, runId: selected?.currentRunId }); }}><option value="">请选择任务</option>{jobId && !(jobsQuery.result.data ?? []).some((job) => job.id === jobId) ? <option value={jobId}>{jobId}</option> : null}{(jobsQuery.result.data ?? []).map((job) => <option key={job.id} value={job.id}>{job.name}</option>)}</select></label><label><span>运行编号</span><input value={runId} onChange={(event) => setRunId(event.target.value || undefined)} placeholder="run-…" /></label><SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} /><label className="toggle"><input type="checkbox" checked={showGpuOnly} onChange={(event) => setShowGpuOnly(event.target.checked)} /><span>仅看加速卡链路</span></label></div>}>
    <div className="topology-workspace"><Panel title="资源分层" subtitle="按调度链路从左到右定位资源最终落点。" className="topology-map-panel" actions={result.data ? <div className="topology-snapshot-meta"><code>{result.data.manifestId ?? '未记录清单'}</code><span>{formatTimestamp(result.data.lastUpdated)}</span></div> : null}><SurfaceStateBoundary result={result} retry={retry}>{() => <div className="topology-flow">{topologyLayers.map((layer) => { const layerNodes = filteredNodes.filter((node) => node.kind === layer.kind); return <section className="topology-layer" key={layer.kind}><header><div><strong>{layer.label}</strong><small>{layer.description}</small></div><span>{layerNodes.length}</span></header><div className="topology-layer-nodes">{layerNodes.length > 0 ? layerNodes.map((node) => { const utilization = node.utilization === undefined ? undefined : Math.round(node.utilization * 100); return <article key={node.id} className={`topology-node status-${node.status}`}><div className="topology-node-heading"><span className="topology-status-dot" /><strong>{node.label}</strong><Pill tone={node.status === 'ready' ? 'good' : node.status === 'busy' ? 'warn' : 'critical'}>{titleCase(node.status)}</Pill></div><code>{node.id}</code>{utilization !== undefined ? <div className="topology-utilization"><div><span>利用率</span><strong>{utilization}%</strong></div><span className="utilization-meter"><i style={{ width: `${utilization}%` }} /></span></div> : null}<footer><span>{node.gpu ? '加速卡资源' : '处理器资源'}</span>{node.share !== undefined ? <span>份额 {Math.round(node.share * 100)}%</span> : null}</footer></article>; }) : <div className="topology-layer-empty">本层暂无节点</div>}</div></section>; })}</div>}</SurfaceStateBoundary></Panel>
    <Panel title="绑定关系" subtitle="逐条核对逻辑对象与实际资源之间的有向关系。" className="topology-edge-panel"><div className="topology-edge-list">{filteredEdges.map((edge, index) => <article key={`${edge.from}-${edge.to}-${edge.relation}`} className="topology-edge"><span>{String(index + 1).padStart(2, '0')}</span><div><strong>{edge.from} → {edge.to}</strong><small>{titleCase(edge.relation)}</small></div></article>)}</div></Panel></div>
  </ShellFrame>;
}
