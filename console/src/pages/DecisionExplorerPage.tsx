import { useEffect, useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery, useResolvedRunScope } from '../app/hooks';
import { formatTimestamp, simulationOptions, toQueryFilters, titleCase } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { DataTable, Panel, Pill, SelectCardButton, ShellFrame } from '../components/primitives';

export function DecisionExplorerPage() {
  const client = useApiClient();
  const [mode, setMode] = useState('ready');
  const { jobId, runId, decisionId, setRunId, setDecisionId, setScopedParams, jobsQuery } =
    useResolvedRunScope(client, mode);
  const paginationScope = `${jobId}\0${runId}`;
  const [pagination, setPagination] = useState({ scope: paginationScope, tokens: [''] });
  const pageTokens = pagination.scope === paginationScope ? pagination.tokens : [''];
  const pageToken = pageTokens.at(-1) || undefined;
  const decisionsQuery = useQuery(
    (signal) =>
      jobId
        ? client.listDecisions(jobId, {
            limit: 8,
            pageToken,
            filters: toQueryFilters({ mode, run_id: runId || undefined }),
            signal,
          })
        : Promise.resolve({
            state: 'empty' as const,
            message: '输入任务编号后即可查看调度决策。',
          }),
    [client, pageToken, jobId, mode, runId],
  );
  const hasCurrentPage =
    decisionsQuery.result.state === 'ready' || decisionsQuery.result.state === 'degraded';
  const currentPageDecisions = hasCurrentPage ? decisionsQuery.result.data ?? [] : [];
  const selectedDecisionId = hasCurrentPage
    ? currentPageDecisions.some((decision) => decision.id === decisionId)
      ? decisionId
      : currentPageDecisions[0]?.id ?? ''
    : decisionId;

  useEffect(() => {
    if (hasCurrentPage && selectedDecisionId !== decisionId) {
      setDecisionId(selectedDecisionId || undefined);
    }
  }, [decisionId, hasCurrentPage, selectedDecisionId, setDecisionId]);

  const explorerQuery = useQuery(
    (signal) =>
      selectedDecisionId && jobId
        ? client.getDecisionExplorer(jobId, selectedDecisionId, { filters: { mode }, signal })
        : Promise.resolve({ state: 'empty' as const, message: '请选择一条决策查看候选与动作。' }),
    [client, jobId, selectedDecisionId, mode],
  );
  const nextPageToken = decisionsQuery.result.pageInfo?.nextPageToken;

  return (
    <ShellFrame
      title="调度决策"
      subtitle="还原候选评分、拒绝原因、回退路径和动作执行结果。"
      actions={
        <div className="control-row">
          <label>
            <span>任务</span>
            <select aria-label="任务编号" value={jobId} onChange={(event) => { const selected = jobsQuery.result.data?.find((job) => job.id === event.target.value); setScopedParams({ jobId: selected?.id || undefined, runId: selected?.currentRunId, decisionId: undefined }); }}>
              <option value="">请选择任务</option>
              {jobId && !(jobsQuery.result.data ?? []).some((job) => job.id === jobId) ? <option value={jobId}>{jobId}</option> : null}
              {(jobsQuery.result.data ?? []).map((job) => <option key={job.id} value={job.id}>{job.name}</option>)}
            </select>
          </label>
          <label>
            <span>运行编号</span>
            <input
              value={runId}
              onChange={(event) => setRunId(event.target.value || undefined)}
              placeholder="run-…"
            />
          </label>
          <SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} />
        </div>
      }
    >
      <div className="trace-workbench">
        <Panel
          title="决策记录"
          subtitle="按服务端保留序列分页，选择一条查看完整证据。"
          actions={
            <div className="control-row compact">
              <button
                className="button"
                type="button"
                onClick={() =>
                  setPagination({
                    scope: paginationScope,
                    tokens: pageTokens.length > 1 ? pageTokens.slice(0, -1) : pageTokens,
                  })
                }
                disabled={pageTokens.length <= 1}
              >
                上一页
              </button>
              <button
                className="button"
                type="button"
                onClick={() =>
                  nextPageToken &&
                  setPagination({
                    scope: paginationScope,
                    tokens: [...pageTokens, nextPageToken],
                  })
                }
                disabled={!nextPageToken}
              >
                下一页
              </button>
            </div>
          }
        >
          <SurfaceStateBoundary result={decisionsQuery.result} retry={decisionsQuery.retry}>
            {(data) => (
              <div className="stack-list">
                {(data ?? []).map((decision) => (
                  <SelectCardButton
                    key={decision.id}
                    selected={selectedDecisionId === decision.id}
                    onClick={() => setDecisionId(decision.id)}
                  >
                    <div className="list-card-header">
                      <div>
                        <h3>{decision.id}</h3>
                        <p>序列 {decision.sequence} · {formatTimestamp(decision.decidedAt)}</p>
                      </div>
                      <Pill tone={decision.fallback ? 'critical' : 'good'}>
                        {decision.fallback ? '已回退' : '已应用'}
                      </Pill>
                    </div>
                    <p className="body-copy">{decision.summary}</p>
                  </SelectCardButton>
                ))}
              </div>
            )}
          </SurfaceStateBoundary>
        </Panel>
        <Panel title="决策证据" subtitle="可行候选、拒绝原因与动作回读。">
          <SurfaceStateBoundary result={explorerQuery.result} retry={explorerQuery.retry}>
            {(data) => (
              <>
                {data?.selectedDecision ? (
                  <div className="trace-inspector-hero">
                    <div>
                      <span className="eyebrow">调度结论</span>
                      <h3>{data.selectedDecision.id}</h3>
                      <code>{data.selectedDecision.traceId}</code>
                    </div>
                    <Pill tone={data.selectedDecision.fallback ? 'critical' : 'good'}>
                      {data.selectedDecision.fallback ? '已回退' : '已应用'}
                    </Pill>
                  </div>
                ) : null}
                <DataTable
                  columns={['候选资源', '评分', '结论', '原因']}
                  rows={(data?.candidates ?? []).map((candidate) => [
                    candidate.deviceLabel,
                    candidate.score.toFixed(2),
                    <Pill key={`${candidate.id}-selection`} tone={candidate.selected ? 'good' : 'warn'}>
                      {candidate.selected ? '已选择' : '可行'}
                    </Pill>,
                    candidate.reason,
                  ])}
                  emptyLabel="该决策没有保留候选记录。"
                />
                <DataTable
                  columns={['被拒候选', '原因', '说明']}
                  rows={(data?.rejectedCandidates ?? []).map((candidate) => [
                    candidate.id,
                    titleCase(candidate.reason.replace(/^CANDIDATE_REJECTION_REASON_/, '')),
                    candidate.detail,
                  ])}
                  emptyLabel="该决策没有被拒候选。"
                />
                <DataTable
                  columns={['动作', '沙箱', '状态', '回读详情']}
                  rows={(data?.relatedActions ?? []).map((action) => [
                    titleCase(action.type),
                    action.sandboxId,
                    <Pill
                      key={`${action.actionId}-status`}
                      tone={action.status === 'succeeded' ? 'good' : action.status === 'failed' ? 'critical' : 'warn'}
                    >
                      {titleCase(action.status)}
                    </Pill>,
                    action.detail,
                  ])}
                  emptyLabel="该决策没有动作执行记录。"
                />
              </>
            )}
          </SurfaceStateBoundary>
        </Panel>
      </div>
    </ShellFrame>
  );
}
