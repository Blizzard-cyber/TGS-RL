import { useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery, useRunScopedSearchParams } from '../app/hooks';
import { dataKindOptions, formatTimestamp, simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { DataTable, Panel, Pill, ShellFrame } from '../components/primitives';

export function SandboxViewPage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const { jobId, runId, setJobId, setRunId } = useRunScopedSearchParams();
  const { result, retry } = useQuery((signal) => jobId ? client.listSandboxes(jobId, { filters: toQueryFilters({ data_kind: dataKind !== 'all' ? dataKind : undefined, mode, run_id: runId || undefined }), signal }) : Promise.resolve({ state: 'empty' as const, message: '输入任务编号后即可查看运行沙箱。' }), [client, dataKind, mode, jobId, runId]);
  return <ShellFrame title="运行沙箱" subtitle="检查代际栅栏、资源绑定、安全点以及卸载状态。" actions={<div className="control-row"><label><span>任务编号</span><input value={jobId} onChange={(event) => setJobId(event.target.value || undefined)} placeholder="job-…" /></label><label><span>运行编号</span><input value={runId} onChange={(event) => setRunId(event.target.value || undefined)} placeholder="run-…" /></label><label><span>数据来源</span><select value={dataKind} onChange={(event) => setDataKind(event.target.value)}>{dataKindOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label><SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} /></div>}>
    <Panel title="沙箱清单" subtitle="运行状态、设备绑定与控制安全条件统一展示。"><SurfaceStateBoundary result={result} retry={retry}>{(data) => <DataTable columns={['沙箱编号', '状态', '代际', '资源绑定', '更新时间']} rows={(data?.sandboxes ?? []).map((sandbox) => [sandbox.id, <div key={`${sandbox.id}-state-block`}><Pill>{titleCase(sandbox.state)}</Pill><div className="tag-row compact"><Pill tone={sandbox.gpuAttached ? 'warn' : 'neutral'}>{sandbox.gpuAttached ? '加速卡' : '处理器'}</Pill><Pill tone={sandbox.safePoint ? 'good' : 'warn'}>{sandbox.safePoint ? '安全点' : '非安全点'}</Pill>{sandbox.offloaded ? <Pill tone="critical">已卸载</Pill> : null}</div></div>, String(sandbox.generation), `${sandbox.nodeLabel} · ${sandbox.bindingSummary}`, formatTimestamp(sandbox.updatedAt)])} emptyLabel="当前任务和来源下没有沙箱记录。" />}</SurfaceStateBoundary></Panel>
  </ShellFrame>;
}
