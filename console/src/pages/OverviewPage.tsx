import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { useApiClient } from '../app/apiContext';
import { useQuery } from '../app/hooks';
import { jobDetailPath, jobTracesPath } from '../app/routes';
import { dataKindOptions, formatTimestamp, simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { Icon, Panel, Pill, ShellFrame } from '../components/primitives';

export function OverviewPage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const { result, retry } = useQuery(
    (signal) => client.listOverview({
      filters: toQueryFilters({ data_kind: dataKind !== 'all' ? dataKind : undefined, mode }),
      signal,
    }),
    [client, dataKind, mode],
  );
  const visibleJobs = useMemo(
    () =>
      [...(result.data?.jobs ?? [])]
        .sort((left, right) => {
          const leftActive = left.state === 'running' || left.state === 'paused' ? 1 : 0;
          const rightActive = right.state === 'running' || right.state === 'paused' ? 1 : 0;
          return rightActive - leftActive || Date.parse(right.updatedAt) - Date.parse(left.updatedAt);
        })
        .slice(0, 10),
    [result.data?.jobs],
  );

  return (
    <ShellFrame
      title="运行总览"
      subtitle="先看异常和运行态，再下钻到任务、链路与调度证据。"
      actions={
        <div className="control-row">
          <label>
            <span>数据来源</span>
            <select value={dataKind} onChange={(event) => setDataKind(event.target.value)}>
              {dataKindOptions.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
          </label>
          <SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} />
        </div>
      }
    >
      <SurfaceStateBoundary result={result} retry={retry}>
        {(data) => {
          const jobs = data?.jobs ?? [];
          const activeJobs = jobs.filter((job) => job.state === 'running' || job.state === 'paused').length;
          const unhealthyJobs = jobs.filter((job) => job.health !== 'healthy').length;
          const statusItems = [
            { label: '任务总数', value: String(jobs.length), tone: 'neutral' },
            { label: '活跃任务', value: String(activeJobs), tone: 'good' },
            { label: '需关注', value: String(unhealthyJobs), tone: unhealthyJobs ? 'warn' : 'good' },
            { label: '实验', value: String(data?.experiments.length ?? 0), tone: 'neutral' },
            { label: '网关', value: titleCase(data?.systemHealth.status ?? 'unknown'), tone: data?.systemHealth.status === 'ok' ? 'good' : 'critical' },
            { label: '协议', value: data?.capabilities.protocolVersion || '—', tone: 'neutral' },
          ] as const;
          return (
            <>
              <section className="status-strip" aria-label="系统状态摘要">
                {statusItems.map((item) => (
                  <div key={item.label} className={`status-item tone-${item.tone}`}>
                    <span>{item.label}</span><strong>{item.value}</strong>
                  </div>
                ))}
              </section>
              <div className="overview-layout">
                <Panel className="overview-jobs" title="任务运行态" subtitle="按当前状态快速进入任务或链路。">
                  <div className="table-scroll">
                    <table className="data-table job-table">
                      <thead><tr><th>任务</th><th>状态</th><th>运行</th><th>队列 / 单元</th><th>更新时间</th><th aria-label="操作" /></tr></thead>
                      <tbody>{visibleJobs.map((job) => (
                        <tr key={job.id}>
                          <td><strong>{job.name}</strong><code>{job.id}</code></td>
                          <td><div className="status-stack"><Pill tone={job.health === 'healthy' ? 'good' : job.health === 'degraded' ? 'warn' : 'critical'}>{titleCase(job.health)}</Pill><span>{titleCase(job.state)}</span></div></td>
                          <td><code>{job.currentRunId || '尚未创建'}</code><span>{titleCase(job.rolloutMode)}</span></td>
                          <td><strong>{job.queue}</strong><span>{job.desiredUnits} 个单元</span></td>
                          <td>{formatTimestamp(job.updatedAt)}</td>
                          <td><div className="row-actions"><Link className="button" to={jobDetailPath(job.id, job.currentRunId)}>详情</Link>{job.currentRunId ? <Link className="button primary" to={jobTracesPath(job.id, job.currentRunId)}><Icon name="trace" />链路</Link> : null}</div></td>
                        </tr>
                      ))}</tbody>
                    </table>
                  </div>
                  {jobs.length > visibleJobs.length ? <div className="panel-footer"><span>仅展示最需关注的最近 10 项</span><Link to="/jobs">查看全部 {jobs.length} 项</Link></div> : null}
                </Panel>
                <aside className="overview-rail">
                  <Panel title="异常与提示" subtitle="优先处理影响运行的事项。">
                    <div className="alert-list">{(data?.alerts ?? []).map((alert) => (
                      <article key={alert.id} className={`alert-row tone-${alert.tone}`}>
                        <span className="alert-mark" />
                        <div><strong>{alert.title}</strong><p>{alert.detail}</p></div>
                        <Pill tone={alert.tone === 'info' ? 'neutral' : alert.tone}>{titleCase(alert.tone)}</Pill>
                      </article>
                    ))}</div>
                  </Panel>
                  <Panel title="最近决策" subtitle="最新调度结果与回退状态。">
                    <div className="compact-list">{(data?.decisions ?? []).slice(0, 6).map((decision) => (
                      <article key={decision.id}><div><strong>{decision.id}</strong><p>{decision.summary}</p></div><Pill tone={decision.fallback ? 'critical' : 'good'}>{decision.fallback ? '回退' : '应用'}</Pill></article>
                    ))}</div>
                  </Panel>
                </aside>
              </div>
            </>
          );
        }}
      </SurfaceStateBoundary>
    </ShellFrame>
  );
}
