import { useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery } from '../app/hooks';
import { dataKindOptions, simulationOptions, toQueryFilters, titleCase } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { MetricCard, Panel, Pill, ShellFrame, SourceBadge } from '../components/primitives';

export function OverviewPage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const { result, retry } = useQuery(
    (signal) =>
      client.listOverview({
        filters: toQueryFilters({
          data_kind: dataKind !== 'all' ? dataKind : undefined,
          mode,
        }),
        signal,
      }),
    [client, dataKind, mode],
  );

  return (
    <ShellFrame
      title="Overview"
      subtitle="Operations-grade summary spanning jobs, decisions, experiments, and retained control-plane alerts."
      actions={
        <div className="control-row">
          <label>
            <span>Source</span>
            <select value={dataKind} onChange={(event) => setDataKind(event.target.value)}>
              {dataKindOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} />
        </div>
      }
    >
      <SurfaceStateBoundary result={result} retry={retry}>
        {(data) => (
          <>
            <section className="metric-grid">
              {(data?.metrics ?? []).map((item) => (
                <MetricCard key={item.label} {...item} />
              ))}
            </section>
            <div className="two-column">
              <Panel title="Active jobs" subtitle="Live, replay, and synthetic workloads stay visually distinct.">
                <div className="stack-list">
                  {(data?.jobs ?? []).map((job) => (
                    <article key={job.id} className="list-card">
                      <div className="list-card-header">
                        <div>
                          <h3>{job.name}</h3>
                          <p>{job.id}</p>
                        </div>
                        <SourceBadge kind={job.dataKind} />
                      </div>
                      <div className="tag-row">
                        <Pill tone={job.health === 'healthy' ? 'good' : job.health === 'degraded' ? 'warn' : 'critical'}>
                          {titleCase(job.health)}
                        </Pill>
                        <Pill>{titleCase(job.state)}</Pill>
                        <Pill>{titleCase(job.rolloutMode)}</Pill>
                      </div>
                      <p className="body-copy">
                        Queue {job.queue}, policy {job.policyVersion}, units {job.activeUnits}/{job.desiredUnits}.
                      </p>
                    </article>
                  ))}
                </div>
              </Panel>
              <Panel title="Control-plane alerts" subtitle="Actionable summaries derived from retained scheduling signals.">
                <div className="stack-list">
                  {(data?.alerts ?? []).map((alert) => (
                    <article key={alert.id} className={`list-card tone-${alert.tone}`}>
                      <div className="list-card-header">
                        <h3>{alert.title}</h3>
                        <Pill tone={alert.tone === 'info' ? 'neutral' : alert.tone}>{titleCase(alert.tone)}</Pill>
                      </div>
                      <p className="body-copy">{alert.detail}</p>
                    </article>
                  ))}
                </div>
              </Panel>
            </div>
            <div className="two-column">
              <Panel title="Latest decisions" subtitle="Recent scheduling results, including explicit fallback decisions.">
                <div className="stack-list">
                  {(data?.decisions ?? []).map((decision) => (
                    <article key={decision.id} className="list-card">
                      <div className="list-card-header">
                        <div>
                          <h3>{decision.id}</h3>
                          <p>{decision.jobId}</p>
                        </div>
                        <Pill tone={decision.fallback ? 'critical' : 'good'}>
                          {decision.fallback ? 'Fallback' : 'Applied'}
                        </Pill>
                      </div>
                      <p className="body-copy">{decision.summary}</p>
                    </article>
                  ))}
                </div>
              </Panel>
              <Panel title="Experiment compare" subtitle="Comparisons remain separated by source kind and policy version.">
                <div className="stack-list">
                  {(data?.experiments ?? []).map((experiment) => (
                    <article key={experiment.id} className="list-card">
                      <div className="list-card-header">
                        <div>
                          <h3>{experiment.name}</h3>
                          <p>{experiment.id}</p>
                        </div>
                        <Pill tone={experiment.state === 'completed' ? 'good' : experiment.state === 'failed' ? 'critical' : 'warn'}>
                          {titleCase(experiment.state)}
                        </Pill>
                      </div>
                      <p className="body-copy">{experiment.summary}</p>
                    </article>
                  ))}
                </div>
              </Panel>
            </div>
          </>
        )}
      </SurfaceStateBoundary>
    </ShellFrame>
  );
}
