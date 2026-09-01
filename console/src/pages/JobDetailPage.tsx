import { useDeferredValue, useEffect, useRef, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useApiClient } from '../app/apiContext';
import { useQuery, useRunScopedSearchParams } from '../app/hooks';
import { jobDecisionsPath, jobSandboxesPath, jobTimelinePath, jobTopologyPath, jobTracesPath } from '../app/routes';
import { commandLabel, dataKindOptions, formatTimestamp, simulationOptions, toQueryFilters, titleCase } from '../app/utils';
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
      displayName: '控制台回放',
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
        displayName: '控制台创建的任务',
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
        return { state: 'empty' as const, message: '请选择一个任务查看运行详情。' };
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
        setControlMessage(result.data?.message ?? `${actionKey} 已完成。`);
      } else {
        setControlError(result.message ?? `${actionKey} 执行失败。`);
      }
    } catch (error) {
      if (activeControlRequest.current === request) {
        setControlError(error instanceof Error ? error.message : `${actionKey} 执行失败。`);
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
      title="任务中心"
      subtitle="选择任务和运行，查看状态、链路追踪、调度决策、沙箱资源并执行控制操作。"
      actions={
        <div className="control-row">
          <label>
            <span>数据来源</span>
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
        <Panel title="任务列表" subtitle="选择一项任务查看完整运行现场。">
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
        <Panel title="任务详情" subtitle="所有决策、链路事件和沙箱都限定在当前任务与运行范围。">
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
                        {detail.job.gpuRequired ? '需要加速卡' : '仅需处理器'}
                      </Pill>
                    </div>
                    <p className="body-copy">
                      队列 {detail.job.queue} · 所有者 {detail.job.owner} · 创建于 {formatTimestamp(detail.job.createdAt)}
                    </p>
                  </div>
                  <section className="metric-grid">
                    {detail.metrics.map((item) => (
                      <MetricCard key={item.label} {...item} />
                    ))}
                  </section>
                  <div className="tag-row">
                    <Link to={jobTimelinePath(detail.job.id, detail.selectedRunId)} className="button">
                      事件时间线
                    </Link>
                    <Link to={jobTracesPath(detail.job.id, detail.selectedRunId)} className="button primary">
                      链路追踪
                    </Link>
                    <Link to={jobTopologyPath(detail.job.id, detail.selectedRunId)} className="button">
                      资源拓扑
                    </Link>
                    <Link to={jobSandboxesPath(detail.job.id, detail.selectedRunId)} className="button">
                      运行沙箱
                    </Link>
                    <Link to={jobDecisionsPath(detail.job.id, detail.selectedRunId)} className="button">
                      调度决策
                    </Link>
                  </div>
                  <section className="stack-list" aria-labelledby="job-runs-heading">
                    <div>
                      <h3 id="job-runs-heading">运行记录（{detail.runs.length}）</h3>
                      <p className="body-copy">选择一次运行后，详情、链路和控制操作会同步切换。</p>
                    </div>
                    <DataTable
                      columns={['运行编号', '状态', '开始时间', '策略版本']}
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
                          {run.id}{run.id === detail.selectedRunId ? '（已选择）' : ''}
                        </button>,
                        <Pill key={`${run.id}-state`}>{titleCase(run.state)}</Pill>,
                        formatTimestamp(run.startedAt ?? run.createdAt),
                        run.policyVersion,
                      ])}
                      emptyLabel="该任务没有保留运行记录。"
                    />
                  </section>
                  <DataTable
                    columns={['决策编号', '结果', '阶段', '计划']}
                    rows={detail.decisions.map((decision) => [
                      <Link
                        key={`${decision.id}-link`}
                        to={`${jobDecisionsPath(detail.job.id, detail.selectedRunId)}${detail.selectedRunId ? '&' : '?'}decisionId=${encodeURIComponent(decision.id)}`}
                      >
                        {decision.id}
                      </Link>,
                      <Pill key={`${decision.id}-outcome`} tone={decision.fallback ? 'critical' : 'good'}>
                        {decision.fallback ? '已回退' : '已应用'}
                      </Pill>,
                      decision.stageId,
                      decision.selectedPlanId ?? '无',
                    ])}
                    emptyLabel="该任务没有保留调度决策。"
                  />
                  <DataTable
                    columns={['沙箱编号', '状态', '节点', '资源绑定']}
                    rows={detail.sandboxes.map((sandbox) => [
                      sandbox.id,
                      <Pill key={`${sandbox.id}-state`}>{titleCase(sandbox.state)}</Pill>,
                      sandbox.nodeLabel,
                      sandbox.bindingSummary,
                    ])}
                    emptyLabel="当前任务没有关联沙箱。"
                  />
                </>
              ) : null
            }
          </SurfaceStateBoundary>
        </Panel>
      </div>
      <Panel title="控制操作" subtitle="通过真实 HTTP 接口管理任务与回放；模拟模式会明确标记演示响应。">
        <div className="two-column">
          <div className="stack-list">
            <div className="list-card">
              <div className="list-card-header">
                <div>
                  <h3>创建任务</h3>
                  <p>通过 Gateway 提交任务定义，JSON 字段保持协议原名。</p>
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
                  创建任务
                </button>
                <button
                  className="button"
                  type="button"
                  disabled={!selectedJobId || controlBusy}
                  onClick={() => void runControlAction('create-run', async () => client.createRun(selectedJobId))}
                >
                  新建运行
                </button>
              </div>
            </div>
            <div className="list-card">
              <div className="list-card-header">
                <div>
                  <h3>运行控制</h3>
                  <p>所有命令都作用于当前选中的任务和运行。</p>
                </div>
              </div>
              <div className="tag-row">
                <Pill>{selectedJobId || '未选择任务'}</Pill>
                <Pill>{commandRunId || '未选择有效运行'}</Pill>
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
                  准入
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
                    {commandLabel(command)}
                  </button>
                ))}
              </div>
            </div>
          </div>
          <div className="stack-list">
            <div className="list-card">
              <div className="list-card-header">
                <div>
                  <h3>创建回放</h3>
                  <p>通过 Gateway 创建独立回放，输入字段保持协议原名。</p>
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
                  创建回放
                </button>
                <label>
                  <span>回放编号</span>
                  <input value={replayId} onChange={(event) => setReplayId(event.target.value)} />
                </label>
              </div>
            </div>
            <div className="list-card">
              <div className="list-card-header">
                <div>
                  <h3>回放控制</h3>
                  <p>直接调用真实回放控制接口。</p>
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
                    {commandLabel(command)}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>
        {controlMessage ? (
          <div className="async-state state-ready" role="status" aria-live="polite">
            <div className="async-copy">
              <h3>控制操作已完成</h3>
              <p>{controlMessage}</p>
            </div>
          </div>
        ) : null}
        {controlError ? <AsyncState state="error" message={controlError} /> : null}
      </Panel>
    </ShellFrame>
  );
}
