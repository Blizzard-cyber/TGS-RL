import { useEffect, useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery, useRunScopedSearchParams } from '../app/hooks';
import { formatTimestamp, simulationOptions, toQueryFilters, titleCase } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { DataTable, Panel, Pill, SelectCardButton, ShellFrame } from '../components/primitives';

export function DecisionExplorerPage() {
  const client = useApiClient();
  const [mode, setMode] = useState('ready');
  const [pageTokens, setPageTokens] = useState<string[]>(['']);
  const { jobId, runId, decisionId, setRunId, setDecisionId } = useRunScopedSearchParams();
  const pageToken = pageTokens[pageTokens.length - 1] || undefined;

  const decisionsQuery = useQuery(
    (signal) =>
      client.listDecisions(jobId, {
        limit: 2,
        pageToken,
        filters: toQueryFilters({
          mode,
          run_id: runId || undefined,
        }),
        signal,
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

  useEffect(() => {
    setPageTokens(['']);
  }, [jobId, runId]);

  const explorerQuery = useQuery(
    (signal) =>
      selectedDecisionId && jobId
        ? client.getDecisionExplorer(jobId, selectedDecisionId, { filters: { mode }, signal })
        : Promise.resolve({ state: 'empty' as const, message: 'Select a decision to inspect candidates.' }),
    [client, jobId, selectedDecisionId, mode],
  );

  const nextPageToken = decisionsQuery.result.pageInfo?.nextPageToken;

  return (
    <ShellFrame
      title="Decision Explorer"
      subtitle="Inspect retained decisions, candidate scoring, fallback reasoning, and action results."
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
        </div>
      }
    >
      <div className="two-column">
        <Panel
          title="Retained decisions"
          subtitle="Cursor-based pagination mirrors the server-side retained sequence boundary."
          actions={
            <div className="control-row compact">
              <button
                className="button"
                type="button"
                onClick={() => setPageTokens((value) => (value.length > 1 ? value.slice(0, -1) : value))}
                disabled={pageTokens.length <= 1}
              >
                Prev
              </button>
              <button
                className="button"
                type="button"
                onClick={() => nextPageToken && setPageTokens((value) => [...value, nextPageToken])}
                disabled={!nextPageToken}
              >
                Next
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
                        <p>
                          {decision.jobId} · {formatTimestamp(decision.decidedAt)}
                        </p>
                      </div>
                      <Pill tone={decision.fallback ? 'critical' : 'good'}>
                        {decision.fallback ? 'Fallback' : 'Applied'}
                      </Pill>
                    </div>
                    <p className="body-copy">{decision.summary}</p>
                  </SelectCardButton>
                ))}
              </div>
            )}
          </SurfaceStateBoundary>
        </Panel>
        <Panel title="Candidate analysis" subtitle="Feasible candidates, rejected candidates, and action results.">
          <SurfaceStateBoundary result={explorerQuery.result} retry={explorerQuery.retry}>
            {(data) => (
              <>
                {data?.selectedDecision ? (
                  <div className="list-card">
                    <div className="list-card-header">
                      <div>
                        <h3>{data.selectedDecision.id}</h3>
                        <p>{data.selectedDecision.jobId}</p>
                      </div>
                      <Pill tone={data.selectedDecision.fallback ? 'critical' : 'good'}>
                        {data.selectedDecision.fallback ? 'Fallback' : 'Applied'}
                      </Pill>
                    </div>
                    <div className="tag-row">
                      <Pill>{data.selectedDecision.stageId}</Pill>
                      <Pill>{data.selectedDecision.policyVersion}</Pill>
                      <Pill>{titleCase(data.selectedDecision.selectedCandidate ?? 'none')}</Pill>
                    </div>
                    <p className="body-copy">{data.selectedDecision.summary}</p>
                  </div>
                ) : null}
                <DataTable
                  columns={['Candidate', 'Score', 'Selection', 'Reason']}
                  rows={(data?.candidates ?? []).map((candidate) => [
                    candidate.deviceLabel,
                    candidate.score.toFixed(2),
                    <Pill key={`${candidate.id}-selection`} tone={candidate.selected ? 'good' : 'warn'}>
                      {candidate.selected ? 'Selected' : 'Feasible'}
                    </Pill>,
                    candidate.reason,
                  ])}
                  emptyLabel="No candidate records were retained for this decision."
                />
                <DataTable
                  columns={['Rejected candidate', 'Reason', 'Detail']}
                  rows={(data?.rejectedCandidates ?? []).map((candidate) => [
                    candidate.id,
                    titleCase(candidate.reason.replace(/^CANDIDATE_REJECTION_REASON_/, '')),
                    candidate.detail,
                  ])}
                  emptyLabel="No rejected candidate evidence was retained for this decision."
                />
                <DataTable
                  columns={['Action', 'Sandbox', 'Status', 'Detail']}
                  rows={(data?.relatedActions ?? []).map((action) => [
                    titleCase(action.type),
                    action.sandboxId,
                    <Pill key={`${action.actionId}-status`} tone={action.status === 'succeeded' ? 'good' : action.status === 'failed' ? 'critical' : 'warn'}>
                      {titleCase(action.status)}
                    </Pill>,
                    action.detail,
                  ])}
                  emptyLabel="No action results were recorded for this decision."
                />
              </>
            )}
          </SurfaceStateBoundary>
        </Panel>
      </div>
    </ShellFrame>
  );
}
