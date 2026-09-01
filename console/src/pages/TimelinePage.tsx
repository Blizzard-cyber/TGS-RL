import { useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery, useResolvedRunScope } from '../app/hooks';
import { dataKindOptions, formatTimestamp, simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { DataTable, Panel, Pill, ShellFrame, SourceBadge } from '../components/primitives';

export function TimelinePage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const { jobId, runId, setJobId, setRunId } = useResolvedRunScope(client, mode);
  const { result, retry } = useQuery(
    (signal) =>
      jobId
        ? client.listTimeline(jobId, {
            filters: toQueryFilters({
              data_kind: dataKind !== 'all' ? dataKind : undefined,
              mode,
              run_id: runId || undefined,
            }),
            signal,
          })
        : Promise.resolve({ state: 'empty' as const, message: '请选择任务后查看事件时间线。' }),
    [client, dataKind, mode, jobId, runId],
  );

  return (
    <ShellFrame
      title="事件时间线"
      subtitle="按发生顺序审计生命周期、组件状态和控制操作。"
      actions={
        <div className="control-row">
          <label><span>任务编号</span><input value={jobId} onChange={(event) => setJobId(event.target.value || undefined)} placeholder="job-…" /></label>
          <label><span>运行编号</span><input value={runId} onChange={(event) => setRunId(event.target.value || undefined)} placeholder="run-…" /></label>
          <label><span>数据来源</span><select value={dataKind} onChange={(event) => setDataKind(event.target.value)}>{dataKindOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
          <SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} />
        </div>
      }
    >
      <Panel title="控制事件" subtitle="训练阶段耗时请在“链路追踪”中查看。">
        <SurfaceStateBoundary result={result} retry={retry}>
          {(data) => (
            <DataTable
              columns={['时间', '事件', '阶段', '级别 / 来源', '说明']}
              rows={(data?.events ?? []).map((event) => [
                <div key={`${event.id}-time`} className="table-primary"><strong>{formatTimestamp(event.occurredAt)}</strong><code>序列 {event.sequence}</code></div>,
                <div key={`${event.id}-event`} className="table-primary"><strong>{titleCase(event.type)}</strong><code>{event.id}</code></div>,
                event.phase,
                <div key={`${event.id}-source`} className="status-stack"><Pill tone={event.severity === 'critical' ? 'critical' : event.severity === 'warn' ? 'warn' : 'neutral'}>{titleCase(event.severity)}</Pill><SourceBadge kind={event.dataKind} /></div>,
                <div key={`${event.id}-detail`} className="table-detail"><span>{event.summary}</span>{event.decisionId ? <code>决策 {event.decisionId}</code> : null}{event.sandboxId ? <code>沙箱 {event.sandboxId}</code> : null}</div>,
              ])}
              emptyLabel="当前任务和运行没有控制事件。"
            />
          )}
        </SurfaceStateBoundary>
      </Panel>
    </ShellFrame>
  );
}
