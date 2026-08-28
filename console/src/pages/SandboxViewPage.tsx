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
  const { jobId, runId, setRunId } = useRunScopedSearchParams();
  const { result, retry } = useQuery(
    (signal) =>
      client.listSandboxes(jobId, {
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
      title="Sandbox View"
      subtitle="Runtime sandboxes with generation fences, binding summaries, and safe-point visibility."
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
      <Panel title="Sandbox inventory" subtitle="GPU availability and degraded runtime conditions are exposed directly in state rendering.">
        <SurfaceStateBoundary result={result} retry={retry}>
          {(data) => (
            <DataTable
              columns={['Sandbox', 'State', 'Generation', 'Binding', 'Updated']}
              rows={(data?.sandboxes ?? []).map((sandbox) => [
                sandbox.id,
                <div key={`${sandbox.id}-state-block`}>
                  <Pill>{titleCase(sandbox.state)}</Pill>
                  <div className="tag-row compact">
                    <Pill tone={sandbox.gpuAttached ? 'warn' : 'neutral'}>{sandbox.gpuAttached ? 'GPU' : 'CPU'}</Pill>
                    <Pill tone={sandbox.safePoint ? 'good' : 'warn'}>{sandbox.safePoint ? 'Safe point' : 'Unsafe'}</Pill>
                    {sandbox.offloaded ? <Pill tone="critical">Offloaded</Pill> : null}
                  </div>
                </div>,
                String(sandbox.generation),
                `${sandbox.nodeLabel} · ${sandbox.bindingSummary}`,
                formatTimestamp(sandbox.updatedAt),
              ])}
              emptyLabel="No sandboxes matched the current job and source filters."
            />
          )}
        </SurfaceStateBoundary>
      </Panel>
    </ShellFrame>
  );
}
