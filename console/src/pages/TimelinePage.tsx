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
  const { jobId, runId, setRunId } = useRunScopedSearchParams();
  const { result, retry } = useQuery(
    (signal) =>
      client.listTimeline(jobId, {
        filters: toQueryFilters({
          data_kind: dataKind !== 'all' ? dataKind : undefined,
          mode,
          run_id: runId || undefined,
        }),
        signal,
      }),
    [client, dataKind, mode, jobId, runId],
  );

  return (
    <ShellFrame
      title="Timeline"
      subtitle="Trace-first chronology across phase transitions, sample flow, policy publications, and decision applications."
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
          <label>
            <span>Job</span>
            <input value={jobId} readOnly />
          </label>
          <label>
            <span>Run filter</span>
            <input
              value={runId}
              onChange={(event) => {
                setRunId(event.target.value || undefined);
              }}
              placeholder="run-live-017-a"
            />
          </label>
          <SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} />
        </div>
      }
    >
      <Panel title="Event stream" subtitle="Each retained event carries an explicit data-kind marker and severity.">
        <SurfaceStateBoundary result={result} retry={retry}>
          {(data) => (
            <div className="timeline-list">
              {(data?.events ?? []).map((event) => (
                <article key={event.id} className={`timeline-event severity-${event.severity}`}>
                  <div className="timeline-rail" />
                  <div className="timeline-body">
                    <div className="list-card-header">
                      <div>
                        <h3>{event.title}</h3>
                        <p>
                          {event.jobId} · seq {event.sequence} · {formatTimestamp(event.occurredAt)}
                        </p>
                      </div>
                      <SourceBadge kind={event.dataKind} />
                    </div>
                    <div className="tag-row">
                      <Pill>{titleCase(event.type)}</Pill>
                      <Pill tone={event.severity === 'critical' ? 'critical' : event.severity === 'warn' ? 'warn' : 'neutral'}>
                        {titleCase(event.severity)}
                      </Pill>
                      <Pill>{event.phase}</Pill>
                    </div>
                    <p className="body-copy">{event.summary}</p>
                    {event.decisionId || event.sandboxId ? (
                      <p className="body-copy subtle">
                        {event.decisionId ? `Decision ${event.decisionId}. ` : ''}
                        {event.sandboxId ? `Sandbox ${event.sandboxId}.` : ''}
                      </p>
                    ) : null}
                  </div>
                </article>
              ))}
            </div>
          )}
        </SurfaceStateBoundary>
      </Panel>
    </ShellFrame>
  );
}
