import { useDeferredValue, useEffect, useRef, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useApiClient } from '../app/apiContext';
import { useQuery, useRunScopedSearchParams } from '../app/hooks';
import { jobDecisionsPath, jobSandboxesPath, jobTimelinePath, jobTopologyPath } from '../app/routes';
import { dataKindOptions, formatTimestamp, simulationOptions, toQueryFilters, titleCase } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { AsyncState, DataTable, MetricCard, Panel, Pill, SelectCardButton, ShellFrame, SourceBadge } from '../components/primitives';
import type { JobDetailResponse, QueryResult } from '../api/types';

interface LoadedJobDetail extends JobDetailResponse {
  requestedRunId: string;
}

function buildReplayDraft(jobId?: string) {
  return JSON.stringify(
    {
      replayId: 'replay-console-new',
      ...(jobId ? { jobId } : {}),
      displayName: 'Console Replay',
    },
    null,
    2,
  );
}

export function JobDetailPage() {
  const client = useApiClient();
  const navigate = useNavigate();
  const { jobId: routeJobId } = useParams();
  const { runId: selectedRunId, setRunId } = useRunScopedSearchParams();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const [jobDraft, setJobDraft] = useState(
    JSON.stringify(
      {
        jobId: 'job-console-new',
        displayName: 'Console Created Job',
        algorithm: 'PPO',
        dataKind: 'DATA_KIND_LIVE',
      },
      null,
      2,
    ),
  );
  const [replayDraftState, setReplayDraftState] = useState({
    jobId: routeJobId ?? '',
    value: buildReplayDraft(routeJobId),
  });
  const [replayId, setReplayId] = useState('replay-console-new');
  const [controlMessage, setControlMessage] = useState<string>('');
  const [controlError, setControlError] = useState<string>('');
  const [pendingAction, setPendingAction] = useState<string>('');
  const activeControlRequest = useRef<symbol | null>(null);
  const pendingRunSelection = useRef<{ jobId: string; runId: string } | null>(null);

  const jobsQuery = useQuery(
    (signal) =>
      client.listJobs({
        limit: 50,
        filters: toQueryFilters({
          data_kind: dataKind !== 'all' ? dataKind : undefined,
          mode,
        }),
        signal,
      }),
    [client, dataKind, mode],
  );

  const selectedJobId = routeJobId ?? jobsQuery.result.data?.[0]?.id ?? '';
  const requestedRunId = selectedRunId;
  const replayDraft =
    replayDraftState.jobId === selectedJobId
      ? replayDraftState.value
      : buildReplayDraft(selectedJobId || undefined);

  useEffect(() => {
    if (!routeJobId && selectedJobId) {
      void navigate(`/jobs/${selectedJobId}`, { replace: true });
    }
  }, [navigate, routeJobId, selectedJobId]);

  const deferredJobId = useDeferredValue(selectedJobId);
  const detailQuery = useQuery<LoadedJobDetail>(
    async (signal) => {
      if (!deferredJobId) {
        return { state: 'empty' as const, message: 'Select a job to inspect retained detail.' };
      }
      const requestedRunIdForQuery = requestedRunId;
      const result = await client.getJobDetail(
        deferredJobId,
        {
          filters: toQueryFilters({
            mode,
            require_gpu: 'true',
            run_id: requestedRunIdForQuery || undefined,
          }),
          signal,
        },
      );
      return result.data
        ? { ...result, data: { ...result.data, requestedRunId: requestedRunIdForQuery } }
        : result as QueryResult<LoadedJobDetail>;
    },
    [client, deferredJobId, mode, requestedRunId],
  );
  const currentDetail =
    (detailQuery.result.state === 'ready' || detailQuery.result.state === 'degraded') &&
    detailQuery.result.data?.job.id === selectedJobId &&
    detailQuery.result.data.requestedRunId === requestedRunId
      ? detailQuery.result.data
      : undefined;
  const selectedRunBelongsToJob = Boolean(
    currentDetail?.runs.some((run) => run.id === selectedRunId && run.jobId === selectedJobId),
  );
  const commandRunId = selectedRunBelongsToJob ? selectedRunId : '';
  const controlBusy = Boolean(pendingAction);

  useEffect(() => {
    if (!currentDetail) {
      return;
    }
    const resolvedRunId = currentDetail.selectedRunId ?? '';
    const pendingSelection = pendingRunSelection.current;
    if (pendingSelection?.jobId === selectedJobId && pendingSelection.runId !== selectedRunId) {
      return;
    }
    if (pendingSelection) {
      pendingRunSelection.current = null;
    }
    if (resolvedRunId !== selectedRunId) {
      setRunId(resolvedRunId || undefined);
    }
  }, [currentDetail, selectedJobId, selectedRunId, setRunId]);

  async function runControlAction(actionKey: string, task: () => Promise<{ state: string; data?: { message?: string; id?: string }; message?: string }>) {
    if (activeControlRequest.current) {
      return;
    }
    const request = Symbol(actionKey);
    activeControlRequest.current = request;
    setPendingAction(actionKey);
    setControlError('');
    setControlMessage('');
    try {
      const result = await task();
      if (activeControlRequest.current !== request) {
        return;
      }
      if (result.state === 'ready') {
        setControlMessage(result.data?.message ?? `${actionKey} succeeded.`);
      } else {
        setControlError(result.message ?? `${actionKey} failed.`);
      }
    } catch (error) {
      if (activeControlRequest.current === request) {
        setControlError(error instanceof Error ? error.message : `${actionKey} failed.`);
      }
    } finally {
      if (activeControlRequest.current === request) {
        activeControlRequest.current = null;
        setPendingAction('');
      }
    }
  }

  return (
    <ShellFrame
      title="Job Detail"
      subtitle="Investigate one job’s retained state, decisions, runtime sandboxes, and current control-plane health."
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
        <Panel title="Jobs" subtitle="Select a retained job to inspect its runtime and decision surfaces.">
          <SurfaceStateBoundary result={jobsQuery.result} retry={jobsQuery.retry}>
            {(jobs) => (
              <div className="stack-list">
                {(jobs ?? []).map((job) => (
                  <SelectCardButton
                    key={job.id}
                    selected={job.id === selectedJobId}
                    onClick={() => {
                      void navigate(`/jobs/${job.id}`);
                    }}
                  >
                    <div className="list-card-header">
                      <div>
                        <h3>{job.name}</h3>
                        <p>{job.id}</p>
                      </div>
                      <SourceBadge kind={job.dataKind} />
                    </div>
                    <div className="tag-row">
                      <Pill>{titleCase(job.state)}</Pill>
                      <Pill tone={job.health === 'healthy' ? 'good' : job.health === 'degraded' ? 'warn' : 'critical'}>
                        {titleCase(job.health)}
                      </Pill>
                    </div>
                  </SelectCardButton>
                ))}
              </div>
            )}
          </SurfaceStateBoundary>
        </Panel>
        <Panel title="Selected job" subtitle="Decisions and sandboxes stay tied to the chosen job boundary.">
          <SurfaceStateBoundary result={detailQuery.result} retry={detailQuery.retry}>
            {(detail) =>
              detail ? (
                <>
                  <div className="list-card">
                    <div className="list-card-header">
                      <div>
                        <h3>{detail.job.name}</h3>
                        <p>{detail.job.id}</p>
                      </div>
                      <SourceBadge kind={detail.job.dataKind} />
                    </div>
                    <div className="tag-row">
                      <Pill>{detail.job.algorithm}</Pill>
                      <Pill>{titleCase(detail.job.rolloutMode)}</Pill>
                      <Pill tone={detail.job.gpuRequired ? 'warn' : 'neutral'}>
                        {detail.job.gpuRequired ? 'GPU required' : 'No accelerator requested'}
                      </Pill>
                    </div>
                    <p className="body-copy">
                      Queue {detail.job.queue}, owner {detail.job.owner}, created {formatTimestamp(detail.job.createdAt)}.
                    </p>
                  </div>
                  <section className="metric-grid">
                    {detail.metrics.map((item) => (
                      <MetricCard key={item.label} {...item} />
                    ))}
                  </section>
                  <div className="tag-row">
                    <Link to={jobTimelinePath(detail.job.id, detail.selectedRunId)} className="button">
                      Timeline
                    </Link>
                    <Link to={jobTopologyPath(detail.job.id, detail.selectedRunId)} className="button">
                      Topology
                    </Link>
                    <Link to={jobSandboxesPath(detail.job.id, detail.selectedRunId)} className="button">
                      Sandboxes
                    </Link>
                    <Link to={jobDecisionsPath(detail.job.id, detail.selectedRunId)} className="button">
                      Decisions
                    </Link>
                  </div>
                  <section className="stack-list" aria-labelledby="job-runs-heading">
                    <div>
                      <h3 id="job-runs-heading">Runs ({detail.runs.length})</h3>
                      <p className="body-copy">All retained runs are available; select one to scope detail and commands.</p>
                    </div>
                    <DataTable
                      columns={['Run', 'State', 'Started', 'Policy']}
                      rows={detail.runs.map((run) => [
                        <button
                          key={`${run.id}-select`}
                          type="button"
                          className="button"
                          aria-pressed={run.id === detail.selectedRunId}
                          onClick={() => {
                            pendingRunSelection.current = { jobId: detail.job.id, runId: run.id };
                            setRunId(run.id);
                          }}
                        >
                          {run.id}{run.id === detail.selectedRunId ? ' (selected)' : ''}
                        </button>,
                        <Pill key={`${run.id}-state`}>{titleCase(run.state)}</Pill>,
                        formatTimestamp(run.startedAt ?? run.createdAt),
                        run.policyVersion,
                      ])}
                      emptyLabel="No runs retained for this job."
                    />
                  </section>
                  <DataTable
                    columns={['Decision', 'Outcome', 'Stage', 'Plan']}
                    rows={detail.decisions.map((decision) => [
                      <Link
                        key={`${decision.id}-link`}
                        to={`${jobDecisionsPath(detail.job.id, detail.selectedRunId)}${detail.selectedRunId ? '&' : '?'}decisionId=${encodeURIComponent(decision.id)}`}
                      >
                        {decision.id}
                      </Link>,
                      <Pill key={`${decision.id}-outcome`} tone={decision.fallback ? 'critical' : 'good'}>
                        {decision.fallback ? 'Fallback' : 'Applied'}
                      </Pill>,
                      decision.stageId,
                      decision.selectedPlanId ?? 'none',
                    ])}
                    emptyLabel="No decisions retained for this job."
                  />
                  <DataTable
                    columns={['Sandbox', 'State', 'Node', 'Binding']}
                    rows={detail.sandboxes.map((sandbox) => [
                      sandbox.id,
                      <Pill key={`${sandbox.id}-state`}>{titleCase(sandbox.state)}</Pill>,
                      sandbox.nodeLabel,
                      sandbox.bindingSummary,
                    ])}
                    emptyLabel="No sandboxes are currently associated with this job."
                  />
                </>
              ) : null
            }
          </SurfaceStateBoundary>
        </Panel>
      </div>
      <Panel title="Control Plane" subtitle="Real HTTP mutations for jobs and replays; mock mode shows explicit demonstration responses.">
        <div className="two-column">
          <div className="stack-list">
            <div className="list-card">
              <div className="list-card-header">
                <div>
                  <h3>Create Job</h3>
                  <p>Submit a raw Gateway job JSON payload through the real HTTP client.</p>
                </div>
              </div>
              <textarea
                className="code-editor"
                value={jobDraft}
                onChange={(event) => setJobDraft(event.target.value)}
                rows={10}
              />
              <div className="control-row compact">
                <button
                  className="button"
                  type="button"
                  disabled={controlBusy}
                  onClick={() =>
                    void runControlAction('create-job', async () => client.createJob(JSON.parse(jobDraft)))
                  }
                >
                  Create Job
                </button>
                <button
                  className="button"
                  type="button"
                  disabled={!selectedJobId || controlBusy}
                  onClick={() => void runControlAction('create-run', async () => client.createRun(selectedJobId))}
                >
                  Create Run
                </button>
              </div>
            </div>
            <div className="list-card">
              <div className="list-card-header">
                <div>
                  <h3>Job Run Commands</h3>
                  <p>Commands use the selected job and current run boundary.</p>
                </div>
              </div>
              <div className="tag-row">
                <Pill>{selectedJobId || 'No job selected'}</Pill>
                <Pill>{commandRunId || 'No valid run selected'}</Pill>
              </div>
              <div className="control-row compact">
                <button
                  className="button"
                  type="button"
                  disabled={!selectedJobId || controlBusy}
                  onClick={() =>
                    void runControlAction('job-admit', async () => client.admitJob(selectedJobId))
                  }
                >
                  Admit
                </button>
                {(['start', 'pause', 'resume', 'stop', 'retry', 'terminate'] as const).map((command) => (
                  <button
                    key={command}
                    className="button"
                    type="button"
                    disabled={!selectedJobId || !commandRunId || controlBusy}
                    onClick={() =>
                      void runControlAction(`job-${command}`, async () =>
                        client.applyJobCommand(selectedJobId, commandRunId, command),
                      )
                    }
                  >
                    {titleCase(command)}
                  </button>
                ))}
              </div>
            </div>
          </div>
          <div className="stack-list">
            <div className="list-card">
              <div className="list-card-header">
                <div>
                  <h3>Create Replay</h3>
                  <p>Submit replay JSON directly to the Gateway replay endpoint.</p>
                </div>
              </div>
              <textarea
                className="code-editor"
                value={replayDraft}
                onChange={(event) =>
                  setReplayDraftState({ jobId: selectedJobId, value: event.target.value })
                }
                rows={10}
              />
              <div className="control-row compact">
                <button
                  className="button"
                  type="button"
                  disabled={controlBusy}
                  onClick={() =>
                    void runControlAction('create-replay', async () => client.createReplay(JSON.parse(replayDraft)))
                  }
                >
                  Create Replay
                </button>
                <label>
                  <span>Replay ID</span>
                  <input value={replayId} onChange={(event) => setReplayId(event.target.value)} />
                </label>
              </div>
            </div>
            <div className="list-card">
              <div className="list-card-header">
                <div>
                  <h3>Replay Commands</h3>
                  <p>Direct replay control uses the real replay command route.</p>
                </div>
              </div>
              <div className="control-row compact">
                {(['start', 'pause', 'resume', 'stop', 'terminate'] as const).map((command) => (
                  <button
                    key={command}
                    className="button"
                    type="button"
                    disabled={!replayId || controlBusy}
                    onClick={() =>
                      void runControlAction(`replay-${command}`, async () =>
                        client.applyReplayCommand(replayId, command),
                      )
                    }
                  >
                    {titleCase(command)}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>
        {controlMessage ? (
          <div className="async-state state-ready" role="status" aria-live="polite">
            <div className="async-copy">
              <h3>Control action succeeded</h3>
              <p>{controlMessage}</p>
            </div>
          </div>
        ) : null}
        {controlError ? <AsyncState state="error" message={controlError} /> : null}
      </Panel>
    </ShellFrame>
  );
}
