import { useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery } from '../app/hooks';
import { dataKindOptions, formatTimestamp, simulationOptions, toQueryFilters, titleCase } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { AsyncState, DataTable, Panel, Pill, SelectCardButton, ShellFrame, SourceBadge } from '../components/primitives';

export function ExperimentComparePage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const [selectedExperimentId, setSelectedExperimentId] = useState('');
  const { result, retry } = useQuery(
    (signal) =>
      client.listExperiments({
        filters: toQueryFilters({
          data_kind: dataKind !== 'all' ? dataKind : undefined,
          mode,
        }),
        signal,
      }),
    [client, dataKind, mode],
  );

  const selectedExperiment =
    result.data?.find((entry) => entry.id === selectedExperimentId) ?? result.data?.[0];

  return (
    <ShellFrame
      title="Experiment Compare"
      subtitle="Cross-run analysis that preserves live, replay, and synthetic boundaries while exposing comparable metrics."
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
      <div className="two-column">
        <Panel title="Experiments" subtitle="Select an experiment aggregate to compare its constituent runs.">
          <SurfaceStateBoundary result={result} retry={retry}>
            {(data) => (
              <div className="stack-list">
                {(data ?? []).map((experiment) => (
                  <SelectCardButton
                    key={experiment.id}
                    selected={selectedExperiment?.id === experiment.id}
                    onClick={() => setSelectedExperimentId(experiment.id)}
                  >
                    <div className="list-card-header">
                      <div>
                        <h3>{experiment.name}</h3>
                        <p>
                          {experiment.id} · {formatTimestamp(experiment.createdAt)}
                        </p>
                      </div>
                      <Pill tone={experiment.state === 'completed' ? 'good' : experiment.state === 'failed' ? 'critical' : 'warn'}>
                        {titleCase(experiment.state)}
                      </Pill>
                    </div>
                    <p className="body-copy">{experiment.summary}</p>
                  </SelectCardButton>
                ))}
              </div>
            )}
          </SurfaceStateBoundary>
        </Panel>
        <Panel title="Run comparison" subtitle="Metrics remain attached to their source kind instead of collapsing into a mixed aggregate.">
          {selectedExperiment ? (
            <>
              <div className="list-card">
                <div className="list-card-header">
                  <div>
                    <h3>{selectedExperiment.name}</h3>
                    <p>{selectedExperiment.id}</p>
                  </div>
                  <Pill tone={selectedExperiment.state === 'completed' ? 'good' : selectedExperiment.state === 'failed' ? 'critical' : 'warn'}>
                    {titleCase(selectedExperiment.state)}
                  </Pill>
                </div>
                <p className="body-copy">{selectedExperiment.summary}</p>
              </div>
              <DataTable
                columns={['Run', 'Source', 'Policy', 'Config', 'Metrics']}
                rows={selectedExperiment.runs.map((run) => [
                  <div key={`${run.id}-meta`}>
                    <strong>{run.label}</strong>
                    <div className="subtle">{run.runId}</div>
                  </div>,
                  <SourceBadge key={`${run.id}-source`} kind={run.dataKind} />,
                  run.policyVersion,
                  run.configHash,
                  <div key={`${run.id}-metrics`} className="metric-inline-list">
                    {run.metrics.map((metric) => (
                      <span key={metric.label}>
                        {metric.label}: {metric.value}
                      </span>
                    ))}
                  </div>,
                ])}
                emptyLabel="No runs were retained for this experiment."
              />
            </>
          ) : (
            <AsyncState state="empty" message="Select an experiment to compare retained runs." />
          )}
        </Panel>
      </div>
    </ShellFrame>
  );
}
