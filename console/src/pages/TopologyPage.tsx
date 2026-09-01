import { useMemo, useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery, useRunScopedSearchParams } from '../app/hooks';
import { simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { Panel, Pill, ShellFrame } from '../components/primitives';

export function TopologyPage() {
  const client = useApiClient();
  const [mode, setMode] = useState('ready');
  const [showGpuOnly, setShowGpuOnly] = useState(false);
  const { jobId, runId, setJobId, setRunId } = useRunScopedSearchParams();
  const { result, retry } = useQuery((signal) => jobId ? client.getTopology(jobId, { filters: toQueryFilters({ mode, require_gpu: showGpuOnly ? 'true' : undefined, run_id: runId || undefined }), signal }) : Promise.resolve({ state: 'empty' as const, message: '输入任务编号后即可查看资源拓扑。' }), [client, jobId, mode, showGpuOnly, runId]);
  const filteredNodes = useMemo(() => { const nodes = result.data?.nodes ?? []; return showGpuOnly ? nodes.filter((node) => node.gpu) : nodes; }, [result.data?.nodes, showGpuOnly]);
  const visibleNodeIds = new Set(filteredNodes.map((node) => node.id));
  const allNodeIds = new Set((result.data?.nodes ?? []).map((node) => node.id));
  const filteredEdges = (result.data?.edges ?? []).filter((edge) => allNodeIds.has(edge.from) && allNodeIds.has(edge.to) && visibleNodeIds.has(edge.from) && visibleNodeIds.has(edge.to));
  return <ShellFrame title="资源拓扑" subtitle="沿任务、运行单元、沙箱和设备查看调度关系与资源压力。" actions={<div className="control-row"><label><span>任务编号</span><input value={jobId} onChange={(event) => setJobId(event.target.value || undefined)} placeholder="job-…" /></label><label><span>运行编号</span><input value={runId} onChange={(event) => setRunId(event.target.value || undefined)} placeholder="run-…" /></label><SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} /><label className="toggle"><input type="checkbox" checked={showGpuOnly} onChange={(event) => setShowGpuOnly(event.target.checked)} /><span>仅看加速卡链路</span></label></div>}>
    <div className="two-column"><Panel title="拓扑快照" subtitle="高亮繁忙、降级和不可用节点。"><SurfaceStateBoundary result={result} retry={retry}>{() => <div className="topology-grid">{filteredNodes.map((node) => <article key={node.id} className={`topology-node status-${node.status}`}><div className="list-card-header"><div><h3>{node.label}</h3><code>{node.id}</code></div><Pill tone={node.status === 'ready' ? 'good' : node.status === 'busy' ? 'warn' : 'critical'}>{titleCase(node.status)}</Pill></div><div className="tag-row"><Pill>{titleCase(node.kind)}</Pill><Pill tone={node.gpu ? 'warn' : 'neutral'}>{node.gpu ? '加速卡' : '处理器'}</Pill></div>{node.utilization !== undefined || node.share !== undefined ? <p className="body-copy">{node.utilization !== undefined ? `利用率 ${Math.round(node.utilization * 100)}%` : '暂无利用率'}{node.share !== undefined ? ` · 份额 ${Math.round(node.share * 100)}%` : ''}</p> : null}</article>)}</div>}</SurfaceStateBoundary></Panel>
    <Panel title="连接关系" subtitle="从任务入口到实际运行位置的有向关系。"><div className="stack-list">{filteredEdges.map((edge) => <article key={`${edge.from}-${edge.to}-${edge.relation}`} className="list-card"><div className="list-card-header"><h3>{edge.from} → {edge.to}</h3><Pill>{titleCase(edge.relation)}</Pill></div></article>)}</div></Panel></div>
  </ShellFrame>;
}
