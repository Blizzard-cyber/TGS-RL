import { useMemo, useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery, useRunScopedSearchParams } from '../app/hooks';
import { simulationOptions, titleCase, toQueryFilters } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { Panel, Pill, ShellFrame } from '../components/primitives';

export function TopologyPage() {
  const client = useApiClient();
  const [mode, setMode] = useState('ready');
  const [showGpuOnly, setShowGpuOnly] = useState(false);
  const { jobId, runId, setRunId } = useRunScopedSearchParams();
  const { result, retry } = useQuery(
    (signal) =>
      client.getTopology(jobId, {
        filters: toQueryFilters({
          mode,
          require_gpu: showGpuOnly ? 'true' : undefined,
          run_id: runId || undefined,
        }),
        signal,
      }),
    [client, jobId, mode, showGpuOnly, runId],
  );

  const filteredNodes = useMemo(() => {
    const nodes = result.data?.nodes ?? [];
    return showGpuOnly ? nodes.filter((node) => node.gpu) : nodes;
  }, [result.data?.nodes, showGpuOnly]);

  const visibleNodeIds = new Set(filteredNodes.map((node) => node.id));
  const allNodeIds = new Set((result.data?.nodes ?? []).map((node) => node.id));
  const filteredEdges = (result.data?.edges ?? []).filter(
    (edge) =>
      allNodeIds.has(edge.from) &&
      allNodeIds.has(edge.to) &&
      visibleNodeIds.has(edge.from) &&
      visibleNodeIds.has(edge.to),
  );

  return (
    <ShellFrame
      title="Topology"
      subtitle="Queue, job, sandbox, runtime, and device relationships rendered as a compact scheduling graph."
      actions={
        <div className="control-row">
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
          <label className="toggle">
            <input type="checkbox" checked={showGpuOnly} onChange={(event) => setShowGpuOnly(event.target.checked)} />
            <span>GPU path only</span>
          </label>
        </div>
      }
    >
      <div className="two-column">
        <Panel title="Graph snapshot" subtitle="A trace-inspired block map that highlights contested paths and degraded nodes.">
          <SurfaceStateBoundary result={result} retry={retry}>
            {() => (
              <div className="topology-grid">
                {filteredNodes.map((node) => (
                  <article key={node.id} className={`topology-node status-${node.status}`}>
                    <div className="list-card-header">
                      <div>
                        <h3>{node.label}</h3>
                        <p>{node.id}</p>
                      </div>
                      <Pill tone={node.status === 'ready' ? 'good' : node.status === 'busy' ? 'warn' : 'critical'}>
                        {titleCase(node.status)}
                      </Pill>
                    </div>
                    <div className="tag-row">
                      <Pill>{titleCase(node.kind)}</Pill>
                      <Pill tone={node.gpu ? 'warn' : 'neutral'}>{node.gpu ? 'GPU' : 'CPU'}</Pill>
                    </div>
                    <p className="body-copy">
                      Utilization {Math.round((node.utilization ?? 0) * 100)}%
                      {node.share !== undefined ? ` · share ${Math.round(node.share * 100)}%` : ''}
                    </p>
                  </article>
                ))}
              </div>
            )}
          </SurfaceStateBoundary>
        </Panel>
        <Panel title="Edges" subtitle="Directional relationships from queue ingress to runtime placement.">
          <div className="stack-list">
            {filteredEdges.map((edge) => (
              <article key={`${edge.from}-${edge.to}-${edge.relation}`} className="list-card">
                <div className="list-card-header">
                  <h3>
                    {edge.from} → {edge.to}
                  </h3>
                  <Pill>{titleCase(edge.relation)}</Pill>
                </div>
              </article>
            ))}
          </div>
        </Panel>
      </div>
    </ShellFrame>
  );
}
