import { useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery, useRunScopedSearchParams } from '../app/hooks';
import { dataKindOptions, formatTimestamp, simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { Panel, Pill, ShellFrame, SourceBadge } from '../components/primitives';

export function TimelinePage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const { jobId, runId, setJobId, setRunId } = useRunScopedSearchParams();
  const { result, retry } = useQuery((signal) => jobId ? client.listTimeline(jobId, { filters: toQueryFilters({ data_kind: dataKind !== 'all' ? dataKind : undefined, mode, run_id: runId || undefined }), signal }) : Promise.resolve({ state: 'empty' as const, message: '输入任务编号后即可查看事件时间线。' }), [client, dataKind, mode, jobId, runId]);

  return (
    <ShellFrame title="事件时间线" subtitle="查看任务生命周期、组件状态和控制操作；训练链路事件请前往“链路追踪”。" actions={<div className="control-row"><label><span>任务编号</span><input value={jobId} onChange={(event) => setJobId(event.target.value || undefined)} placeholder="job-…" /></label><label><span>运行编号</span><input value={runId} onChange={(event) => setRunId(event.target.value || undefined)} placeholder="run-…" /></label><label><span>数据来源</span><select value={dataKind} onChange={(event) => setDataKind(event.target.value)}>{dataKindOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label><SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} /></div>}>
      <Panel title="控制事件流" subtitle="每条记录均保留来源类型、严重程度与运行范围。">
        <SurfaceStateBoundary result={result} retry={retry}>{(data) => <div className="timeline-list">{(data?.events ?? []).map((event) => <article key={event.id} className={`timeline-event severity-${event.severity}`}><div className="timeline-rail" /><div className="timeline-body"><div className="list-card-header"><div><h3>{titleCase(event.type)}</h3><p>{event.jobId} · 序列 {event.sequence} · {formatTimestamp(event.occurredAt)}</p></div><SourceBadge kind={event.dataKind} /></div><div className="tag-row"><Pill>{titleCase(event.type)}</Pill><Pill tone={event.severity === 'critical' ? 'critical' : event.severity === 'warn' ? 'warn' : 'neutral'}>{titleCase(event.severity)}</Pill><Pill>{event.phase}</Pill></div><p className="body-copy">{event.summary}</p>{event.decisionId || event.sandboxId ? <p className="body-copy subtle">{event.decisionId ? `决策 ${event.decisionId}。` : ''}{event.sandboxId ? `沙箱 ${event.sandboxId}。` : ''}</p> : null}</div></article>)}</div>}</SurfaceStateBoundary>
      </Panel>
    </ShellFrame>
  );
}
