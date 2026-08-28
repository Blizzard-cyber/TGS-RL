import type {
  ControlActionResult,
  DataKind,
  ExperimentSummary,
  JobSummary,
  QueryOptions,
  QueryResult,
  RunSummary,
} from './types';
import type { GatewayFilterKey } from './contracts';

export function delay(ms = 180, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    return Promise.reject(new DOMException('The operation was aborted.', 'AbortError'));
  }
  return new Promise((resolve, reject) => {
    const timeoutId = window.setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      window.clearTimeout(timeoutId);
      signal?.removeEventListener('abort', onAbort);
      reject(new DOMException('The operation was aborted.', 'AbortError'));
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export function parseSimulation(filters?: Record<string, string>) {
  const mode = filters?.mode ?? 'ready';
  if (
    mode === 'ready' ||
    mode === 'empty' ||
    mode === 'error' ||
    mode === 'forbidden' ||
    mode === 'gpu-unavailable' ||
    mode === 'degraded'
  ) {
    return mode;
  }
  return 'ready';
}

export function applySimulation<T>(mode: string, data: T, message: string): QueryResult<T> {
  switch (mode) {
    case 'empty':
      return { state: 'empty', message };
    case 'error':
      return { state: 'error', message, retryable: true };
    case 'forbidden':
      return { state: 'forbidden', message };
    case 'gpu-unavailable':
      return { state: 'gpu-unavailable', message, retryable: true };
    case 'degraded':
      return { state: 'degraded', data, message, retryable: true };
    default:
      return { state: 'ready', data };
  }
}

export function parseOpaqueOffset(pageToken?: string): number {
  if (!pageToken) {
    return 0;
  }
  if (pageToken.startsWith('offset:')) {
    const parsed = Number(pageToken.slice('offset:'.length));
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
  }
  return 0;
}

export function formatOpaqueOffset(offset: number): string {
  return `offset:${offset}`;
}

export function withPagination<T>(entries: T[], options?: QueryOptions): { data: T[]; nextCursor?: string } {
  const limit = options?.limit ?? entries.length;
  const start = parseOpaqueOffset(options?.pageToken);
  const data = entries.slice(start, start + limit);
  const next = start + limit < entries.length ? formatOpaqueOffset(start + limit) : undefined;
  return { data, nextCursor: next };
}

export function filterByDataKind<T extends { dataKind: DataKind }>(entries: T[], options?: QueryOptions): T[] {
  const requestedKind = (options?.filters?.data_kind ?? options?.filters?.dataKind) as DataKind | undefined;
  return requestedKind ? entries.filter((entry) => entry.dataKind === requestedKind) : entries;
}

export function filterJobs(entries: JobSummary[], options?: QueryOptions): JobSummary[] {
  return filterByDataKind(entries, options).filter((entry) => {
    const state = options?.filters?.state;
    return state ? entry.state === state : true;
  });
}

export function getSelectedRunId(filters?: Record<string, string>): string | undefined {
  return filters?.run_id ?? filters?.runId;
}

export function toGatewayDataKind(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  if (value.startsWith('DATA_KIND_')) {
    return value;
  }
  if (value === 'live') {
    return 'DATA_KIND_LIVE';
  }
  if (value === 'replay') {
    return 'DATA_KIND_REPLAY';
  }
  if (value === 'synthetic') {
    return 'DATA_KIND_SYNTHETIC';
  }
  return value;
}

export function pickGatewayFilters(filters: Record<string, string> | undefined, allowedKeys: readonly GatewayFilterKey[]) {
  const picked = Object.fromEntries(
    Object.entries(filters ?? {})
      .filter((entry): entry is [GatewayFilterKey, string] => allowedKeys.includes(entry[0] as GatewayFilterKey))
      .map(([key, value]) => [key, key === 'data_kind' ? toGatewayDataKind(value) ?? value : value]),
  );
  return Object.keys(picked).length ? picked : undefined;
}

export function asResult<T>(result: QueryResult<unknown>): QueryResult<T> {
  return result as QueryResult<T>;
}

export function toCamelCase(value: string): string {
  return value.replace(/_([a-z])/g, (_, char: string) => char.toUpperCase());
}

export function normalizeGatewayJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(normalizeGatewayJson);
  }
  if (!value || typeof value !== 'object') {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, entryValue]) => [
      toCamelCase(key),
      normalizeGatewayJson(entryValue),
    ]),
  );
}

export function toDataKind(value: string | undefined): DataKind {
  const normalized = value?.replace(/^DATA_KIND_/, '').toLowerCase();
  if (normalized === 'replay') {
    return 'replay';
  }
  if (normalized === 'live') {
    return 'live';
  }
  return 'synthetic';
}

export function toJobState(value: string | undefined): JobSummary['state'] {
  const normalized = value?.replace(/^JOB_STATE_/, '').toLowerCase();
  if (
    normalized === 'pending' ||
    normalized === 'running' ||
    normalized === 'paused' ||
    normalized === 'succeeded' ||
    normalized === 'failed' ||
    normalized === 'cancelled'
  ) {
    return normalized;
  }
  return 'pending';
}

export function toRolloutMode(value: string | undefined): JobSummary['rolloutMode'] {
  const normalized = value?.replace(/^ROLLOUT_MODE_/, '').toLowerCase();
  if (
    normalized === 'sync' ||
    normalized === 'partially_async' ||
    normalized === 'fully_async'
  ) {
    return normalized;
  }
  return 'sync';
}

export function toHealthFromRunState(value: string | undefined): JobSummary['health'] {
  const normalized = value?.replace(/^JOB_RUN_STATE_/, '').toLowerCase();
  if (normalized === 'failed' || normalized === 'terminated' || normalized === 'stopped') {
    return 'stalled';
  }
  if (normalized?.includes('paus') || normalized === 'waiting' || normalized === 'retrying' || normalized === 'admitting') {
    return 'degraded';
  }
  return 'healthy';
}

export function toMetricToneByHealth(health: JobSummary['health']): 'good' | 'warn' | 'critical' {
  if (health === 'healthy') {
    return 'good';
  }
  if (health === 'degraded') {
    return 'warn';
  }
  return 'critical';
}

export function toTimestamp(value: unknown): string {
  return typeof value === 'string' && value ? value : new Date(0).toISOString();
}

export function ensureArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

export function ensureObject(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

export function collectBindingDeviceIds(value: unknown): string[] {
  const input = ensureObject(value);
  const directBindings = ensureArray<Record<string, unknown>>(input.bindings).flatMap((binding) =>
    ensureArray<string>(ensureObject(binding).deviceIds),
  );
  const directBinding = ensureArray<string>(ensureObject(input.binding).deviceIds);
  const inlineDeviceIds = ensureArray<string>(input.deviceIds);
  return [...new Set([...inlineDeviceIds, ...directBinding, ...directBindings].filter(Boolean))];
}

function compareRunsByServerOrder(a: RunSummary, b: RunSummary): number {
  const attemptDiff = b.attempt - a.attempt;
  if (attemptDiff !== 0) {
    return attemptDiff;
  }
  if (a.id === b.id) {
    return 0;
  }
  return a.id < b.id ? 1 : -1;
}

export function sortRunsByServerOrder(runs: RunSummary[]): RunSummary[] {
  return [...runs].sort(compareRunsByServerOrder);
}

export function selectLatestRun(runs: RunSummary[]): RunSummary | undefined {
  return sortRunsByServerOrder(runs)[0];
}

export function deriveOverviewHealth(jobs: JobSummary[], health: Record<string, unknown>) {
  const degradedJobs = jobs.filter((job) => job.health !== 'healthy').length;
  return [
    degradedJobs > 0
      ? {
          id: 'overview-degraded-jobs',
          title: 'Jobs degraded',
          tone: 'warn' as const,
          detail: `${degradedJobs} retained jobs are not fully healthy.`,
        }
      : {
          id: 'overview-jobs-healthy',
          title: 'Jobs healthy',
          tone: 'info' as const,
          detail: 'Retained jobs are currently healthy.',
        },
    {
      id: 'overview-system-status',
      title: `Gateway ${String(health.status ?? 'unknown')}`,
      tone: String(health.status ?? '') === 'ok' ? 'info' as const : 'critical' as const,
      detail: `Gateway backend reports ${String(health.backend ?? 'unknown')} at ${String(health.observedAt ?? '')}.`,
    },
  ];
}

export function createControlResult(
  payload: Record<string, unknown>,
  message: string,
  fallbackId?: string,
): ControlActionResult {
  const operation = ensureObject(payload.operation);
  const job = ensureObject(payload.job);
  const run = ensureObject(payload.run);
  const replay = ensureObject(payload.replay);
  const operationState = String(operation.state ?? operation.operationState ?? '').replace(/^[A-Z_]*STATE_/, '').toUpperCase();
  const status: ControlActionResult['status'] =
    operationState === 'SUCCEEDED'
      ? 'succeeded'
      : operationState === 'FAILED'
        ? 'failed'
        : operationState === 'CANCELLED'
          ? 'cancelled'
          : 'accepted';
  return {
    status,
    message,
    requestId:
      typeof payload.requestId === 'string'
        ? payload.requestId
        : typeof payload.request_id === 'string'
          ? payload.request_id
          : undefined,
    operationId: typeof operation.operationId === 'string' ? operation.operationId : undefined,
    id:
      (typeof run.runId === 'string' && run.runId) ||
      (typeof replay.replayId === 'string' && replay.replayId) ||
      (typeof job.jobId === 'string' && job.jobId) ||
      fallbackId,
    job: Object.keys(job).length ? job : undefined,
    run: Object.keys(run).length ? run : undefined,
    replay: Object.keys(replay).length ? replay : undefined,
  };
}

export function filterExperimentsByDataKind(experiments: ExperimentSummary[], requestedKind?: DataKind) {
  return requestedKind
    ? experiments.filter((experiment) => experiment.runs.some((run) => run.dataKind === requestedKind))
    : experiments;
}
