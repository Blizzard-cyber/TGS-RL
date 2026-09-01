import { useMemo } from 'react';
import type { ReactNode } from 'react';
import type { LoadStateKind, QueryResult } from '../api/types';
import { AsyncState } from '../components/primitives';

function isMockMode() {
  return (import.meta.env.VITE_TGSRL_API_ADAPTER ?? 'http') === 'mock';
}

export function SurfaceStateControl({
  mode,
  onChange,
  options,
}: {
  mode: string;
  onChange: (value: string) => void;
  options: ReadonlyArray<{ value: string; label: string }>;
}) {
  if (!isMockMode()) {
    return null;
  }
  return (
    <label>
      <span>模拟状态</span>
      <select value={mode} onChange={(event) => onChange(event.target.value)}>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export function SurfaceStateBoundary<T>({
  result,
  retry,
  children,
}: {
  result: QueryResult<T>;
  retry: () => void;
  children: (data: T | undefined, state: Extract<LoadStateKind, 'ready' | 'degraded'>) => ReactNode;
}) {
  const visibleState = useMemo(() => result.state, [result.state]);
  if (visibleState !== 'ready' && visibleState !== 'degraded') {
    return <AsyncState state={visibleState} message={result.message} onRetry={retry} />;
  }
  return (
    <>
      {visibleState === 'degraded' ? <AsyncState state="degraded" message={result.message} onRetry={retry} /> : null}
      {children(result.data, visibleState)}
    </>
  );
}
