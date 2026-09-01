import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useApiClient } from '../app/apiContext';
import { useQuery } from '../app/hooks';
import { jobDetailPath, jobTracesPath } from '../app/routes';
import { dataKindOptions, simulationOptions, toQueryFilters, titleCase } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { Icon, MetricCard, Panel, Pill, ShellFrame, SourceBadge } from '../components/primitives';

export function OverviewPage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const { result, retry } = useQuery((signal) => client.listOverview({ filters: toQueryFilters({ data_kind: dataKind !== 'all' ? dataKind : undefined, mode }), signal }), [client, dataKind, mode]);

  return (
    <ShellFrame title="运行总览" subtitle="从任务状态出发，快速进入一次运行的链路追踪、调度决策和资源现场。" actions={<div className="control-row"><label><span>数据来源</span><select value={dataKind} onChange={(event) => setDataKind(event.target.value)}>{dataKindOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label><SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} /></div>}>
      <SurfaceStateBoundary result={result} retry={retry}>
        {(data) => (
          <>
            <section className="metric-grid">{(data?.metrics ?? []).map((item) => <MetricCard key={item.label} {...item} />)}</section>
            <div className="two-column overview-main">
              <Panel title="当前任务" subtitle="真实运行、回放与合成数据始终保持边界。">
                <div className="stack-list">{(data?.jobs ?? []).map((job) => (
                  <article key={job.id} className="list-card job-summary-card">
                    <div className="list-card-header"><div><h3>{job.name}</h3><code>{job.id}</code></div><SourceBadge kind={job.dataKind} /></div>
                    <div className="tag-row"><Pill tone={job.health === 'healthy' ? 'good' : job.health === 'degraded' ? 'warn' : 'critical'}>{titleCase(job.health)}</Pill><Pill>{titleCase(job.state)}</Pill><Pill>{titleCase(job.rolloutMode)}</Pill></div>
                    <p className="body-copy">队列 {job.queue} · 策略 {job.policyVersion || '未设置'} · 期望单元 {job.desiredUnits}</p>
                    <div className="card-actions"><Link className="button primary" to={jobDetailPath(job.id, job.currentRunId)}>进入任务</Link>{job.currentRunId ? <Link className="button" to={jobTracesPath(job.id, job.currentRunId)}><Icon name="trace" />查看链路</Link> : null}</div>
                  </article>
                ))}</div>
              </Panel>
              <Panel title="控制面提示" subtitle="由当前健康状态与调度证据生成的可操作提醒。">
                <div className="stack-list">{(data?.alerts ?? []).map((alert) => <article key={alert.id} className={`list-card alert-card tone-${alert.tone}`}><div className="list-card-header"><h3>{alert.title}</h3><Pill tone={alert.tone === 'info' ? 'neutral' : alert.tone}>{titleCase(alert.tone)}</Pill></div><p className="body-copy">{alert.detail}</p></article>)}</div>
              </Panel>
            </div>
            <div className="two-column">
              <Panel title="最近决策" subtitle="保留成功计划和显式回退，便于追溯资源变化。"><div className="stack-list">{(data?.decisions ?? []).map((decision) => <article key={decision.id} className="list-card"><div className="list-card-header"><div><h3>{decision.id}</h3><code>{decision.jobId}</code></div><Pill tone={decision.fallback ? 'critical' : 'good'}>{decision.fallback ? '已回退' : '已应用'}</Pill></div><p className="body-copy">{decision.summary}</p></article>)}</div></Panel>
              <Panel title="实验对比" subtitle="基线和变体按数据来源、策略版本独立展示。"><div className="stack-list">{(data?.experiments ?? []).map((experiment) => <article key={experiment.id} className="list-card"><div className="list-card-header"><div><h3>{experiment.name}</h3><code>{experiment.id}</code></div><Pill tone={experiment.state === 'completed' ? 'good' : experiment.state === 'failed' ? 'critical' : 'warn'}>{titleCase(experiment.state)}</Pill></div><p className="body-copy">{experiment.summary}</p></article>)}</div></Panel>
            </div>
          </>
        )}
      </SurfaceStateBoundary>
    </ShellFrame>
  );
}
