import { useCallback, useEffect, useMemo, useState } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import type { QueryResult } from '../api/types';

function isAbortError(error: unknown) {
  return (
    (error instanceof DOMException && error.name === 'AbortError') ||
    (error instanceof Error && error.name === 'AbortError') ||
    (error instanceof Error && /aborted|abort/i.test(error.message))
  );
}

export function useQuery<T>(factory: (signal: AbortSignal) => Promise<QueryResult<T>>, deps: readonly unknown[]) {
  const [result, setResult] = useState<QueryResult<T>>({ state: 'loading' });
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    setResult({ state: 'loading' });
    void factory(controller.signal)
      .then((value) => {
        if (active && !controller.signal.aborted) {
          setResult(value);
        }
      })
      .catch((error: unknown) => {
        if (!active || controller.signal.aborted || isAbortError(error)) {
          return;
        }
        setResult({
          state: 'error',
          message: error instanceof Error ? error.message : 'Request failed.',
          retryable: true,
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

  const updateScopedParams = useCallback(
    (nextValues: { jobId?: string; runId?: string; decisionId?: string }) => {
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
      setJobId: (value?: string) => updateScopedParams({ jobId: value }),
      setRunId: (value?: string) => updateScopedParams({ runId: value }),
      setDecisionId: (value?: string) => updateScopedParams({ decisionId: value }),
      setScopedParams: updateScopedParams,
    }),
    [searchParams, jobId, runId, decisionId, updateScopedParams],
  );
}
