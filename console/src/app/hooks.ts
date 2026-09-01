import { useCallback, useEffect, useMemo, useState } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import type { ApiClient, QueryResult } from '../api/types';

function isAbortError(error: unknown) {
  return (
    (error instanceof DOMException && error.name === 'AbortError') ||
    (error instanceof Error && error.name === 'AbortError') ||
    (error instanceof Error && /aborted|abort/i.test(error.message))
  );
}

export function useQuery<T>(factory: (signal: AbortSignal) => Promise<QueryResult<T>>, deps: readonly unknown[]) {
  const [settled, setSettled] = useState<{ deps: readonly unknown[]; result: QueryResult<T> }>({
    deps,
    result: { state: 'loading' },
  });
  const [nonce, setNonce] = useState(0);
  const requestDeps = [...deps, nonce];
  const result =
    settled.deps.length === requestDeps.length &&
    settled.deps.every((value, index) => Object.is(value, requestDeps[index]))
      ? settled.result
      : ({ state: 'loading' } as QueryResult<T>);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    void factory(controller.signal)
      .then((value) => {
        if (active && !controller.signal.aborted) {
          setSettled({ deps: requestDeps, result: value });
        }
      })
      .catch((error: unknown) => {
        if (!active || controller.signal.aborted || isAbortError(error)) {
          return;
        }
        setSettled({
          deps: requestDeps,
          result: {
            state: 'error',
            message: error instanceof Error ? error.message : '请求失败。',
            retryable: true,
          },
        });
      });
    return () => {
      active = false;
      controller.abort();
    };
    // The caller supplies the concrete dependencies for the query factory.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce]);

  return {
    result,
    retry: () => setNonce((value) => value + 1),
  };
}

export function useRunScopedSearchParams() {
  const { jobId: routeJobId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const jobId = routeJobId ?? searchParams.get('jobId') ?? '';
  const runId = searchParams.get('runId') ?? '';
  const decisionId = searchParams.get('decisionId') ?? '';
  const traceId = searchParams.get('traceId') ?? '';

  const updateScopedParams = useCallback(
    (nextValues: { jobId?: string; runId?: string; decisionId?: string; traceId?: string }) => {
      const next = new URLSearchParams(searchParams);
      for (const [key, value] of Object.entries(nextValues)) {
        if (value) {
          next.set(key, value);
        } else {
          next.delete(key);
        }
      }
      setSearchParams(next, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  return useMemo(
    () => ({
      searchParams,
      jobId,
      runId,
      decisionId,
      traceId,
      setJobId: (value?: string) => updateScopedParams({ jobId: value }),
      setRunId: (value?: string) => updateScopedParams({ runId: value }),
      setDecisionId: (value?: string) => updateScopedParams({ decisionId: value }),
      setTraceId: (value?: string) => updateScopedParams({ traceId: value }),
      setScopedParams: updateScopedParams,
    }),
    [searchParams, jobId, runId, decisionId, traceId, updateScopedParams],
  );
}

export function useResolvedRunScope(client: ApiClient, mode: string) {
  const scope = useRunScopedSearchParams();
  const jobsQuery = useQuery(
    (signal) => client.listJobs({ limit: 200, filters: { mode }, signal }),
    [client, mode],
  );
  const selectedJob = useMemo(() => {
    const jobs = jobsQuery.result.data ?? [];
    const requested = jobs.find((job) => job.id === scope.jobId);
    return requested ?? [...jobs].sort(
      (left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt),
    )[0];
  }, [jobsQuery.result.data, scope.jobId]);

  useEffect(() => {
    if (selectedJob && (!scope.jobId || !scope.runId)) {
      scope.setScopedParams({
        jobId: selectedJob.id,
        runId: scope.runId || selectedJob.currentRunId,
      });
    }
  }, [scope, selectedJob]);

  return { ...scope, jobsQuery, selectedJob };
}
