import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import type { TraceEventRecord } from '../api/types';
import { useApiClient } from '../app/apiContext';
import { useQuery, useResolvedRunScope } from '../app/hooks';
import { jobDecisionsPath, jobSandboxesPath } from '../app/routes';
import { dataKindOptions, simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { MetricCard, Panel, ShellFrame, SourceBadge } from '../components/primitives';

type TrackKind = 'trainer' | 'request' | 'executor' | 'worker';

const trackKinds: TrackKind[] = ['trainer', 'request', 'executor', 'worker'];
const trackLabels: Record<TrackKind, { title: string; description: string }> = {
  trainer: { title: '训练阶段', description: '训练步骤与阶段切换' },
  request: { title: '请求处理', description: '请求与智能体循环' },
  executor: { title: '推理调度', description: '引擎排队与批处理' },
  worker: { title: '工作进程', description: '前向计算与设备活动' },
};

function attribute(event: TraceEventRecord, ...keys: string[]) {
  for (const key of keys) {
    if (event.attributes[key]) return event.attributes[key];
  }
  return '';
}

function traceTitle(event: TraceEventRecord) {
  return attribute(event, 'display_name', 'span_name') || titleCase(event.type);
}

function durationMs(event: TraceEventRecord) {
  const milliseconds = attribute(event, 'duration_ms', 'durationMs');
  if (milliseconds && Number.isFinite(Number(milliseconds))) return Math.max(0, Number(milliseconds));
  const microseconds = attribute(event, 'duration_us', 'durationUs');
  if (microseconds && Number.isFinite(Number(microseconds))) return Math.max(0, Number(microseconds) / 1_000);
  const nanoseconds = attribute(event, 'duration_ns', 'durationNs');
  if (nanoseconds && Number.isFinite(Number(nanoseconds))) return Math.max(0, Number(nanoseconds) / 1_000_000);
  return 0;
}

function formatDuration(value: number) {
  if (value >= 60_000) return `${(value / 60_000).toFixed(1)} 分钟`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(2)} 秒`;
  if (value >= 1) return `${value.toFixed(2)} 毫秒`;
  return `${Math.round(value * 1_000)} 微秒`;
}

function trackKind(event: TraceEventRecord): TrackKind {
  const text = [
    event.stageId,
    event.phaseId,
    attribute(event, 'component', 'track', 'span_name'),
    event.type,
  ].join(' ').toLowerCase();
  if (/worker|forward|execute_model/.test(text)) return 'worker';
  if (/executor|engine|schedule/.test(text)) return 'executor';
  if (/request|agent|tool|chat|prompt/.test(text)) return 'request';
  return 'trainer';
}

function trackName(event: TraceEventRecord) {
  const name =
    attribute(event, 'track', 'worker_id', 'request_id', 'component') ||
    event.stageId ||
    event.phaseId ||
    trackLabels[trackKind(event)].title;
  return (
    { trainer: '训练器', request: '请求', executor: '执行器', worker: '工作进程' } as Record<string, string>
  )[name.toLowerCase()] ?? name;
}

export function TraceExplorerPage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const [selectedEventId, setSelectedEventId] = useState('');
  const [zoom, setZoom] = useState(1);
  const [collapsed, setCollapsed] = useState<Set<TrackKind>>(new Set());
  const { jobId, runId, traceId, setRunId, setTraceId, setScopedParams, jobsQuery } =
    useResolvedRunScope(client, mode);

  const { result, retry } = useQuery(
    (signal) =>
      jobId
        ? client.listTraces(jobId, {
            limit: 500,
            filters: toQueryFilters({
              run_id: runId || undefined,
              trace_id: traceId || undefined,
              data_kind: dataKind !== 'all' ? dataKind : undefined,
              mode,
            }),
            signal,
          })
        : Promise.resolve({ state: 'empty' as const, message: '请选择任务后查看链路事件。' }),
    [client, dataKind, jobId, mode, runId, traceId],
  );
  const events = useMemo(() => result.data?.events ?? [], [result.data?.events]);
  const durationEvents = events.filter((event) => durationMs(event) > 0);
  const longest = [...durationEvents].sort(
    (left, right) => durationMs(right) - durationMs(left),
  )[0];
  const longestRequest = [...durationEvents]
    .filter((event) => trackKind(event) === 'request')
    .sort((left, right) => durationMs(right) - durationMs(left))[0];
  const selected =
    events.find((event) => event.id === selectedEventId) ?? longestRequest ?? longest ?? events.at(-1);
  const selectedRequestId = selected ? attribute(selected, 'request_id') : '';

  const tracks = useMemo(() => {
    const grouped = new Map<
      string,
      { kind: TrackKind; name: string; events: TraceEventRecord[] }
    >();
    for (const event of events) {
      const kind = trackKind(event);
      const name = trackName(event);
      const key = `${kind}:${name}`;
      const group = grouped.get(key) ?? { kind, name, events: [] };
      group.events.push(event);
      grouped.set(key, group);
    }
    return [...grouped.values()].sort(
      (left, right) =>
        trackKinds.indexOf(left.kind) - trackKinds.indexOf(right.kind) ||
        left.name.localeCompare(right.name),
    );
  }, [events]);

  const timeRange = useMemo(() => {
    const values = events.map((event) => Date.parse(event.occurredAt)).filter(Number.isFinite);
    const start = values.length ? Math.min(...values) : 0;
    const end = events.reduce(
      (latest, event) => Math.max(latest, Date.parse(event.occurredAt) + durationMs(event)),
      start,
    );
    return { start, span: Math.max(end - start, 1) };
  }, [events]);

  const traceCount = new Set(events.map((event) => event.traceId).filter(Boolean)).size;
  const correlations = useMemo(() => {
    const grouped = new Map<string, TraceEventRecord[]>();
    for (const event of events) {
      const requestId = attribute(event, 'request_id');
      if (!requestId) continue;
      const group = grouped.get(requestId) ?? [];
      group.push(event);
      grouped.set(requestId, group);
    }
    return [...grouped.entries()]
      .map(([requestId, related]) => ({
        requestId,
        related,
        firstOccurredAt: related[0]?.occurredAt ?? '',
        executorIds: [
          ...new Set(related.map((event) => attribute(event, 'executor_id')).filter(Boolean)),
        ],
        workerIds: [
          ...new Set(related.map((event) => attribute(event, 'worker_id')).filter(Boolean)),
        ],
        batchSizes: [
          ...new Set(related.map((event) => attribute(event, 'batch_size')).filter(Boolean)),
        ],
        duration:
          Math.max(
            ...related.map((event) => Date.parse(event.occurredAt) + durationMs(event)),
          ) - Math.min(...related.map((event) => Date.parse(event.occurredAt))),
      }))
      .sort((left, right) => Date.parse(left.firstOccurredAt) - Date.parse(right.firstOccurredAt));
  }, [events]);
  const selectedCorrelation = correlations.find(
    (correlation) => correlation.requestId === selectedRequestId,
  );

  const largestGap = useMemo(() => {
    const ordered = [...events].sort(
      (left, right) => Date.parse(left.occurredAt) - Date.parse(right.occurredAt),
    );
    let largest: { duration: number; from: TraceEventRecord; to: TraceEventRecord } | undefined;
    let latest = ordered[0];
    let latestEnd = latest ? Date.parse(latest.occurredAt) + durationMs(latest) : 0;
    for (const event of ordered.slice(1)) {
      const start = Date.parse(event.occurredAt);
      const gap = start - latestEnd;
      if (latest && gap > 0 && (!largest || gap > largest.duration)) {
        largest = { duration: gap, from: latest, to: event };
      }
      const end = start + durationMs(event);
      if (end > latestEnd) {
        latest = event;
        latestEnd = end;
      }
    }
    return largest;
  }, [events]);

  const timeTicks = [0, 0.25, 0.5, 0.75, 1].map((ratio) =>
    formatDuration(timeRange.span * ratio),
  );
  const toggleTrack = (kind: TrackKind) =>
    setCollapsed((current) => {
      const next = new Set(current);
      if (next.has(kind)) next.delete(kind);
      else next.add(kind);
      return next;
    });
  const eventStyle = (event: TraceEventRecord) => {
    const start = ((Date.parse(event.occurredAt) - timeRange.start) / timeRange.span) * 100;
    const duration = durationMs(event);
    return {
      left: `${Math.min(98, Math.max(0, start))}%`,
      width: duration
        ? `${Math.max(1.4, Math.min(100 - start, (duration / timeRange.span) * 100))}%`
        : undefined,
    };
  };

  return (
    <ShellFrame
      title="链路追踪"
      subtitle="沿同一时间轴定位请求长尾、推理排队和工作进程空泡。"
      actions={
        <div className="control-row">
          <label>
            <span>任务</span>
            <select
              value={jobId}
              onChange={(event) =>
                setScopedParams({ jobId: event.target.value || undefined, runId: undefined, traceId: undefined })
              }
            >
              <option value="">请选择任务</option>
              {jobId && !(jobsQuery.result.data ?? []).some((job) => job.id === jobId) ? (
                <option value={jobId}>{jobId}</option>
              ) : null}
              {(jobsQuery.result.data ?? []).map((job) => (
                <option key={job.id} value={job.id}>{job.name}</option>
              ))}
            </select>
          </label>
          <label><span>运行编号</span><input value={runId} onChange={(event) => setRunId(event.target.value || undefined)} placeholder="run-…" /></label>
          <label><span>链路编号</span><input value={traceId} onChange={(event) => setTraceId(event.target.value || undefined)} placeholder="trace-…" /></label>
          <label><span>数据来源</span><select value={dataKind} onChange={(event) => setDataKind(event.target.value)}>{dataKindOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
          <SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} />
        </div>
      }
    >
      <SurfaceStateBoundary result={result} retry={retry}>
        {() => (
          <>
            <section className="metric-grid trace-metrics">
              <MetricCard label="链路" value={String(traceCount)} tone="good" />
              <MetricCard label="事件" value={String(events.length)} />
              <MetricCard label="持续片段" value={String(durationEvents.length)} />
              <MetricCard label="最长耗时" value={longest ? formatDuration(durationMs(longest)) : '暂无'} tone={longest ? 'warn' : 'neutral'} />
            </section>
            <div className="trace-toolbar">
              <div><strong>全链路时间轨</strong><span>{events.length <= 1 && !longest ? '单一时点' : `范围 ${formatDuration(timeRange.span)}`}</span></div>
              <label><span>时间轴密度</span><input type="range" min="1" max="4" step="0.5" value={zoom} onChange={(event) => setZoom(Number(event.target.value))} /></label>
            </div>
            {longest || largestGap ? (
              <div className="trace-findings">
                {longest ? <div><span>最长片段</span><strong>{trackName(longest)}</strong><small>{traceTitle(longest)} · {formatDuration(durationMs(longest))}</small></div> : null}
                {largestGap ? <div><span>最大空泡</span><strong>{formatDuration(largestGap.duration)}</strong><small>{traceTitle(largestGap.from)} → {traceTitle(largestGap.to)}</small></div> : null}
              </div>
            ) : null}
            {correlations.length ? (
              <section className="trace-correlations" aria-label="跨轨调用关联">
                <div className="trace-correlation-heading"><div><strong>跨轨调用关联</strong><span>选择请求，高亮关联片段。</span></div><em>{correlations.length} 条请求</em></div>
                <div className="trace-correlation-list">
                  {correlations.map((correlation) => (
                    <button
                      type="button"
                      key={correlation.requestId}
                      className={selectedRequestId === correlation.requestId ? 'active' : ''}
                      onClick={() => {
                        const eventId = correlation.related.find((event) => trackKind(event) === 'request')?.id ?? correlation.related[0]?.id;
                        if (eventId) setSelectedEventId(eventId);
                      }}
                    >
                      <span className="correlation-node request"><small>请求</small><strong>{correlation.requestId}</strong></span><b>→</b>
                      <span className="correlation-node executor"><small>执行器</small><strong>{correlation.executorIds.join('、') || '未上报'}</strong></span><b>→</b>
                      <span className="correlation-node worker"><small>工作进程</small><strong>{correlation.workerIds.join('、') || '未上报'}</strong></span>
                      <span className="correlation-meta">{formatDuration(correlation.duration)}{correlation.batchSizes.length ? ` · 批量 ${correlation.batchSizes.join('/')}` : ''}</span>
                    </button>
                  ))}
                </div>
              </section>
            ) : null}
            <div className="trace-layout">
              <Panel className="trace-panel" title="性能时间轴" subtitle="同一请求的片段会联动高亮。">
                <div className="trace-ruler">{timeTicks.map((tick, index) => <span key={index}>{tick}</span>)}</div>
                <div className="trace-waterfall">
                  {trackKinds.map((kind) => {
                    const rows = tracks.filter((track) => track.kind === kind);
                    const isCollapsed = collapsed.has(kind);
                    return (
                      <section key={kind} className="track-group">
                        <button type="button" className="track-group-header" onClick={() => toggleTrack(kind)} aria-expanded={!isCollapsed}>
                          <span>{isCollapsed ? '＋' : '－'}</span><strong>{trackLabels[kind].title}</strong><small>{trackLabels[kind].description}</small><em>{rows.reduce((sum, row) => sum + row.events.length, 0)} 个事件</em>
                        </button>
                        {!isCollapsed ? (rows.length ? rows.map((track) => (
                          <div className="trace-track" style={{ minWidth: `${174 + zoom * 720}px` }} key={`${kind}:${track.name}`}>
                            <div className="track-label"><strong>{track.name}</strong><small>{track.events.length} 个片段</small></div>
                            <div className="track-canvas">
                              {track.events.map((event) => {
                                const duration = durationMs(event);
                                const related = Boolean(selectedRequestId && attribute(event, 'request_id') === selectedRequestId);
                                return (
                                  <button type="button" aria-label={`${trackName(event)} ${traceTitle(event)}`} key={event.id} className={`trace-span track-${kind}${duration ? '' : ' instant'}${selected?.id === event.id ? ' selected' : ''}${related ? ' related' : ''}`} style={eventStyle(event)} title={`${traceTitle(event)}${duration ? ` · ${formatDuration(duration)}` : ' · 瞬时事件'}${attribute(event, 'batch_size') ? ` · 批量 ${attribute(event, 'batch_size')}` : ''}`} onClick={() => setSelectedEventId(event.id)}>
                                    <span>{duration ? traceTitle(event) : ''}</span>
                                  </button>
                                );
                              })}
                            </div>
                          </div>
                        )) : <div className="empty-track">当前未采集该层级事件</div>) : null}
                      </section>
                    );
                  })}
                </div>
                <div className="trace-legend"><span><i className="legend-bar trainer" />训练阶段</span><span><i className="legend-bar request" />请求处理</span><span><i className="legend-bar executor" />推理调度</span><span><i className="legend-bar worker" />工作进程</span><span><i className="legend-diamond" />瞬时事件</span></div>
              </Panel>
              <Panel className="trace-inspector-panel" title="事件详情" subtitle="当前选中片段。">
                {selected ? (
                  <div className="trace-inspector">
                    <div className="trace-inspector-hero"><div><span className="eyebrow">序列 {selected.sequence}</span><h3>{traceTitle(selected)}</h3><code>{selected.traceId || '链路编号未记录'}</code></div><SourceBadge kind={selected.dataKind} /></div>
                    <dl className="detail-grid">
                      <div><dt>轨道</dt><dd>{trackLabels[trackKind(selected)].title} / {trackName(selected)}</dd></div><div><dt>耗时</dt><dd>{durationMs(selected) ? formatDuration(durationMs(selected)) : '瞬时事件'}</dd></div>
                      <div><dt>运行</dt><dd>{selected.runId}</dd></div><div><dt>阶段</dt><dd>{selected.stageId || selected.phaseId || '—'}</dd></div>
                      <div><dt>批量大小</dt><dd>{attribute(selected, 'batch_size') || '—'}</dd></div><div><dt>设备</dt><dd>{attribute(selected, 'device_id', 'device_ids') || '—'}</dd></div>
                      <div><dt>片段编号</dt><dd>{attribute(selected, 'span_id') || '—'}</dd></div><div><dt>上级片段</dt><dd>{attribute(selected, 'parent_span_id') || '—'}</dd></div>
                      <div><dt>代际</dt><dd>{selected.generation}</dd></div><div><dt>安全点</dt><dd>{selected.safePoint ? '是' : '否'}</dd></div>
                    </dl>
                    {selectedCorrelation ? <div className="correlation-chain"><span>请求 {selectedCorrelation.requestId}</span><b>→</b><span>执行器 {selectedCorrelation.executorIds.join('、') || '未上报'}</span><b>→</b><span>工作进程 {selectedCorrelation.workerIds.join('、') || '未上报'}</span></div> : <div className="correlation-chain"><span>请求 未记录</span><b>→</b><span>决策 {selected.decisionId || '未关联'}</span><b>→</b><span>沙箱 {selected.sandboxId || '未关联'}</span></div>}
                    <div className="trace-links">{selected.decisionId ? <Link className="button primary" to={`${jobDecisionsPath(selected.jobId, selected.runId)}&decisionId=${encodeURIComponent(selected.decisionId)}`}>查看关联决策</Link> : null}{selected.sandboxId ? <Link className="button" to={jobSandboxesPath(selected.jobId, selected.runId)}>查看运行沙箱</Link> : null}</div>
                    <details className="raw-attributes"><summary>查看全部事件属性（{Object.keys(selected.attributes).length}）</summary><div className="attribute-list">{Object.keys(selected.attributes).length ? Object.entries(selected.attributes).map(([key, value]) => <div key={key}><code>{key}</code><span>{value}</span></div>) : <p className="table-empty">该事件没有附加属性。</p>}</div></details>
                  </div>
                ) : null}
              </Panel>
            </div>
            {events.length > 0 && durationEvents.length === 0 ? <div className="trace-notice"><strong>当前事件没有持续时间字段</strong><span>时间轴使用菱形标记真实发生时刻；接入持续时间属性后会自动显示耗时条。</span></div> : null}
          </>
        )}
      </SurfaceStateBoundary>
    </ShellFrame>
  );
}
