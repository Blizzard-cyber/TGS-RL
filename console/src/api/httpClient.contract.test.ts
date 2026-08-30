import { afterEach, describe, expect, it, vi } from 'vitest';
import { HttpApiClient } from './client';

const originalFetch = globalThis.fetch;

const decisionProtoFixture = {
  decision_id: 'dec-1',
  sequence: 1,
  stage_id: 'actor',
  trace_id: 'trace-1',
  decided_at: '2026-08-27T00:00:00Z',
  fallback: false,
  selected_plan: {
    plan_id: 'plan-1',
    bindings: [{ pending_unit_id: 'unit-1', device_ids: ['gpu-node-1', 'gpu-node-2'] }],
    actions: [
      {
        action_id: 'act-1',
        action_type: 'ACTION_TYPE_BIND',
        sandbox_id: 'sbx-1',
        target: { target_id: 'gpu-node-1' },
      },
      {
        action_id: 'act-2',
        action_type: 'ACTION_TYPE_RECREATE',
        sandbox_id: 'sbx-2',
        target: { target_id: 'gpu-node-2' },
      },
      {
        action_id: 'act-3',
        action_type: 'ACTION_TYPE_SET_PRIORITY',
        sandbox_id: 'sbx-3',
      },
    ],
  },
  action_results: [
    {
      action_id: 'act-1',
      status: 'ACTION_RESULT_STATUS_ROLLED_BACK',
    },
    {
      action_id: 'act-2',
      status: 'ACTION_RESULT_STATUS_MYSTERY',
      error_code: 'GPU_REALLOCATE_PENDING',
    },
  ],
  candidates: [
    {
      candidate_id: 'cand-1',
      plan_id: 'plan-2',
      plan: {
        plan_id: 'plan-2',
        bindings: [{ pending_unit_id: 'unit-1', device_ids: ['gpu-node-9'] }],
      },
    },
    {
      candidate_id: 'cand-2',
      plan_id: 'plan-1',
      plan: {
        plan_id: 'plan-1',
        bindings: [{ pending_unit_id: 'unit-1', device_ids: ['gpu-node-1', 'gpu-node-2'] }],
      },
    },
  ],
};

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('HttpApiClient contract', () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('uses gateway nested decision routes with page_token and run_id', async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({
        decisions: [
          {
            decisionId: 'dec-1',
            sequence: 1,
            stageId: 'actor',
            traceId: 'trace-1',
            decidedAt: '2026-08-27T00:00:00Z',
            fallback: false,
            selectedPlan: { planId: 'plan-1' },
            actionResults: [],
            candidates: [],
          },
        ],
        nextPageToken: 'opaque-2',
      }),
    );
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listDecisions('job-live-017', {
      pageToken: 'opaque-1',
      limit: 20,
      filters: { run_id: 'run-live-017-a' },
    });

    const calls = fetchMock.mock.calls as unknown as Array<[RequestInfo | URL, ...unknown[]]>;
    const firstRequest = calls[0]?.[0];
    expect(firstRequest).toBeDefined();
    const requestUrl = new URL(String(firstRequest));
    expect(requestUrl.pathname).toBe('/v1/jobs/job-live-017/decisions');
    expect(requestUrl.searchParams.get('page_token')).toBe('opaque-1');
    expect(requestUrl.searchParams.get('limit')).toBe('20');
    expect(requestUrl.searchParams.get('run_id')).toBe('run-live-017-a');
    expect(result.state).toBe('ready');
    expect(result.pageInfo?.nextPageToken).toBe('opaque-2');
    expect(result.data?.[0]?.jobId).toBe('job-live-017');
  });

  it('aggregates overview from health, capabilities, jobs, runs, and experiments', async () => {
    const controller = new AbortController();
    const seenSignals: AbortSignal[] = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input), 'http://localhost');
      if (init?.signal) {
        seenSignals.push(init.signal as AbortSignal);
      }
      if (url.pathname === '/health') {
        expect(url.search).toBe('');
        return jsonResponse({
          status: 'ok',
          observedAt: '2026-08-27T00:00:00Z',
          backend: 'in_memory',
          counts: { jobs: 1, runs: 1, operations: 1, decisions: 1, replays: 0, experiments: 1 },
        });
      }
      if (url.pathname === '/v1/capabilities') {
        expect(url.search).toBe('');
        return jsonResponse({
          protocolVersion: 'v0.3',
          dataKinds: ['DATA_KIND_LIVE'],
          pagination: { kind: 'opaque_stable_token' },
        });
      }
      if (url.pathname === '/v1/jobs') {
        expect(url.searchParams.get('data_kind')).toBe('DATA_KIND_LIVE');
        expect(url.searchParams.get('limit')).toBe('10');
        expect(url.searchParams.get('page_token')).toBe('jobs-page-2');
        expect(url.searchParams.get('mode')).toBeNull();
        expect(url.searchParams.get('run_id')).toBeNull();
        return jsonResponse({
          jobs: [
            {
              jobId: 'job-live-017',
              displayName: 'PPO Actor-Critic Burst',
              algorithm: 'PPO',
              state: 'JOB_STATE_RUNNING',
              rolloutMode: 'ROLLOUT_MODE_PARTIALLY_ASYNC',
              dataKind: 'DATA_KIND_LIVE',
              queue: 'priority-train',
              priority: 95,
              desiredUnits: 16,
              createdAt: '2026-08-27T08:15:00Z',
              labels: { owner: 'ops-shift-a' },
              requiredCapabilities: { elasticParallelism: true },
            },
          ],
        });
      }
      if (url.pathname === '/v1/jobs/job-live-017/runs') {
        expect(url.searchParams.get('limit')).toBe('100');
        expect(url.searchParams.get('mode')).toBeNull();
        expect(url.searchParams.get('data_kind')).toBeNull();
        return jsonResponse({
          runs: [
            {
              runId: 'run-live-017-z',
              jobId: 'job-live-017',
              traceId: 'trace-live-017-old',
              runState: 'JOB_RUN_STATE_FAILED',
              createdAt: '2026-08-27T10:14:00Z',
              startedAt: '2026-08-27T08:15:00Z',
              completedAt: '2026-08-27T08:17:00Z',
              dataKind: 'DATA_KIND_LIVE',
              attempt: 1,
              policyVersion: 'policy-2026.08.27.4',
            },
            {
              runId: 'run-live-017-a',
              jobId: 'job-live-017',
              traceId: 'trace-live-017',
              runState: 'JOB_RUN_STATE_RUNNING',
              createdAt: '2026-08-27T08:16:00Z',
              startedAt: '2026-08-27T08:18:00Z',
              dataKind: 'DATA_KIND_LIVE',
              attempt: 2,
              policyVersion: 'policy-2026.08.27.5',
              componentStatus: [{ component: 'scheduler', health: 'COMPONENT_HEALTH_DEGRADED', detail: 'GPU contention' }],
            },
          ],
        });
      }
      if (url.pathname === '/v1/experiments') {
        expect(url.searchParams.get('limit')).toBe('10');
        expect(url.searchParams.get('page_token')).toBeNull();
        expect(url.searchParams.get('mode')).toBeNull();
        expect(url.searchParams.get('data_kind')).toBeNull();
        return jsonResponse({
          experiments: [{ experimentId: 'exp-1', displayName: 'Compare', state: 'EXPERIMENT_STATE_RUNNING', createdAt: '2026-08-27T05:00:00Z', summary: 'summary' }],
        });
      }
      throw new Error(`unexpected URL ${url.pathname}`);
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listOverview({
      pageToken: 'jobs-page-2',
      limit: 10,
      filters: { data_kind: 'live', mode: 'degraded' },
      signal: controller.signal,
    });

    expect(result.state).toBe('ready');
    expect(result.data?.jobs).toHaveLength(1);
    expect(result.data?.jobs[0]?.currentRunId).toBe('run-live-017-a');
    expect(result.data?.capabilities.protocolVersion).toBe('v0.3');
    expect(result.data?.systemHealth.status).toBe('ok');
    expect(seenSignals).toHaveLength(5);
    expect(seenSignals.every((signal) => signal === controller.signal)).toBe(true);
  });

  it.each([
    ['missing URL run', undefined, 'run-z'],
    ['unknown URL run', 'run-missing', 'run-z'],
    ['another job run', 'run-other-job', 'run-z'],
    ['valid URL run', 'run-05', 'run-05'],
  ])('returns every run and resolves %s to one job-bound selection', async (_label, requestedRunId, expectedRunId) => {
    const runsRequests: URL[] = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), 'http://localhost');
      if (url.pathname === '/v1/jobs/job-live-017') {
        return jsonResponse({
          job: {
            jobId: 'job-live-017',
            displayName: 'PPO Actor-Critic Burst',
            state: 'JOB_STATE_RUNNING',
            dataKind: 'DATA_KIND_LIVE',
            createdAt: '2026-08-27T08:15:00Z',
          },
        });
      }
      if (url.pathname === '/v1/jobs/job-live-017/runs') {
        runsRequests.push(url);
        expect(url.searchParams.get('page_token')).not.toBe('unrelated-detail-page');
        if (url.searchParams.get('page_token') === 'runs-page-2') {
          return jsonResponse({
            runs: [
              {
                runId: 'run-a',
                jobId: 'job-live-017',
                attempt: 21,
                createdAt: '2026-08-28T12:00:00Z',
                dataKind: 'DATA_KIND_LIVE',
              },
              {
                runId: 'run-z',
                jobId: 'job-live-017',
                attempt: 21,
                createdAt: '2020-01-01T00:00:00Z',
                dataKind: 'DATA_KIND_LIVE',
              },
              {
                runId: 'run-other-job',
                jobId: 'job-other',
                attempt: 999,
                createdAt: '2030-01-01T00:00:00Z',
                dataKind: 'DATA_KIND_LIVE',
              },
            ],
          });
        }
        expect(url.searchParams.get('page_token')).toBeNull();
        return jsonResponse({
          runs: Array.from({ length: 20 }, (_, index) => ({
            runId: `run-${String(index + 1).padStart(2, '0')}`,
            jobId: 'job-live-017',
            attempt: index + 1,
            createdAt: `2026-08-28T${String(index).padStart(2, '0')}:00:00Z`,
            dataKind: 'DATA_KIND_LIVE',
          })),
          nextPageToken: 'runs-page-2',
        });
      }
      if (url.pathname === '/v1/jobs/job-live-017/decisions') {
        expect(url.searchParams.get('run_id')).toBe(expectedRunId);
        expect(url.searchParams.get('page_token')).toBeNull();
        return jsonResponse({ decisions: [] });
      }
      if (url.pathname === '/v1/jobs/job-live-017/sandboxes') {
        expect(url.searchParams.get('run_id')).toBe(expectedRunId);
        expect(url.searchParams.get('page_token')).toBeNull();
        return jsonResponse({ sandboxes: [] });
      }
      throw new Error(`unexpected URL ${url.pathname}`);
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.getJobDetail('job-live-017', {
      pageToken: 'unrelated-detail-page',
      filters: requestedRunId ? { run_id: requestedRunId } : undefined,
    });

    expect(result.state).toBe('ready');
    expect(result.data?.runs).toHaveLength(22);
    expect(result.data?.runs[0]?.id).toBe('run-z');
    expect(result.data?.selectedRunId).toBe(expectedRunId);
    expect(result.data?.job.currentRunId).toBe(expectedRunId);
    expect(result.data?.job.runCount).toBe(22);
    expect(result.pageInfo).toBeUndefined();
    expect(runsRequests).toHaveLength(2);
  });

  it('selects the latest run across paginated runs in overview and jobs surfaces', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), 'http://localhost');
      if (url.pathname === '/health') {
        return jsonResponse({
          status: 'ok',
          observedAt: '2026-08-27T00:00:00Z',
          backend: 'in_memory',
          counts: { jobs: 1, runs: 21, operations: 1, decisions: 0, replays: 0, experiments: 0 },
        });
      }
      if (url.pathname === '/v1/capabilities') {
        return jsonResponse({
          protocolVersion: 'v0.3',
          dataKinds: ['DATA_KIND_LIVE'],
          pagination: { kind: 'opaque_stable_token' },
        });
      }
      if (url.pathname === '/v1/jobs') {
        return jsonResponse({
          jobs: [
            {
              jobId: 'job-live-017',
              displayName: 'PPO Actor-Critic Burst',
              algorithm: 'PPO',
              state: 'JOB_STATE_RUNNING',
              rolloutMode: 'ROLLOUT_MODE_PARTIALLY_ASYNC',
              dataKind: 'DATA_KIND_LIVE',
              queue: 'priority-train',
              priority: 95,
              desiredUnits: 16,
              createdAt: '2026-08-27T08:15:00Z',
              labels: { owner: 'ops-shift-a' },
              requiredCapabilities: { elasticParallelism: true },
            },
          ],
        });
      }
      if (url.pathname === '/v1/experiments') {
        return jsonResponse({ experiments: [] });
      }
      if (url.pathname === '/v1/jobs/job-live-017/runs') {
        expect(url.searchParams.get('limit')).toBe('100');
        if (url.searchParams.get('page_token') === 'runs-page-2') {
          return jsonResponse({
            runs: [
              {
                runId: 'run-live-017-latest',
                jobId: 'job-live-017',
                traceId: 'trace-live-017-latest',
                runState: 'JOB_RUN_STATE_RUNNING',
                createdAt: '2026-08-27T09:00:00Z',
                startedAt: '2026-08-27T09:01:00Z',
                dataKind: 'DATA_KIND_LIVE',
                attempt: 1,
                policyVersion: 'policy-2026.08.27.6',
              },
            ],
          });
        }
        return jsonResponse({
          runs: Array.from({ length: 20 }, (_, index) => ({
            runId: `run-live-017-${index}`,
            jobId: 'job-live-017',
            traceId: `trace-live-017-${index}`,
            runState: 'JOB_RUN_STATE_FAILED',
            createdAt: `2026-08-27T08:${String(index).padStart(2, '0')}:00Z`,
            startedAt: `2026-08-27T08:${String(index).padStart(2, '0')}:30Z`,
            completedAt: `2026-08-27T08:${String(index).padStart(2, '0')}:45Z`,
            dataKind: 'DATA_KIND_LIVE',
            attempt: 1,
            policyVersion: 'policy-2026.08.27.5',
          })),
          nextPageToken: 'runs-page-2',
        });
      }
      throw new Error(`unexpected URL ${url.pathname}`);
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const overview = await client.listOverview();
    const jobs = await client.listJobs();

    expect(overview.state).toBe('ready');
    expect(overview.data?.jobs[0]?.currentRunId).toBe('run-live-017-latest');
    expect(overview.data?.jobs[0]?.latestRunState).toBe('JOB_RUN_STATE_RUNNING');
    expect(jobs.state).toBe('ready');
    expect(jobs.data?.[0]?.currentRunId).toBe('run-live-017-latest');
  });

  it('uses gateway snake_case query keys and strips mock-only filters on timeline requests', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), 'http://localhost');
      expect(url.pathname).toBe('/v1/jobs/job-live-017/timeline');
      expect(url.searchParams.get('run_id')).toBe('run-live-017-a');
      expect(url.searchParams.get('after_event_id')).toBe('evt-42');
      expect(url.searchParams.get('limit')).toBe('25');
      expect(url.searchParams.get('mode')).toBeNull();
      expect(url.searchParams.get('data_kind')).toBeNull();
      expect(url.searchParams.get('require_gpu')).toBeNull();
      return jsonResponse({ events: [] });
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listTimeline('job-live-017', {
      limit: 25,
      filters: {
        run_id: 'run-live-017-a',
        after_event_id: 'evt-42',
        data_kind: 'live',
        mode: 'degraded',
        require_gpu: 'true',
      },
    });

    expect(result.state).toBe('ready');
    expect(result.data?.events).toHaveLength(0);
  });

  it('uses nested topology and sandbox routes', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), 'http://localhost');
      if (url.pathname === '/v1/jobs/job-live-017/topology') {
        expect(url.searchParams.get('run_id')).toBe('run-live-017-a');
        return jsonResponse({
          run: { runId: 'run-live-017-a', displayName: 'PPO Actor-Critic Burst', createdAt: '2026-08-27T08:16:00Z' },
          manifest: { manifestId: 'manifest-1' },
          runtimeUnits: [{ runtimeUnitId: 'unit-1', phaseId: 'actor-rollout', state: 'RUNTIME_STATE_RUNNING', requestedResources: { acceleratorUnits: 1 } }],
          sandboxes: [
            { sandboxId: 'sbx-1', runId: 'run-live-017-a', state: 'RUNTIME_STATE_RUNNING', generation: 1, binding: { runtimeUnitId: 'unit-1', deviceIds: ['gpu-0'], resources: { cpuMillis: 2000, acceleratorUnits: 1 } }, share: 0.5, priority: 10, safePoint: true, observedAt: '2026-08-27T08:20:00Z' },
            { sandboxId: 'sbx-2', runId: 'run-live-017-a', state: 'RUNTIME_STATE_FAILED', generation: 2, binding: { runtimeUnitId: 'unit-1', deviceIds: ['gpu-1'], resources: { cpuMillis: 1800, acceleratorUnits: 1 } }, share: 0.5, priority: 11, safePoint: false, observedAt: '2026-08-27T08:21:00Z' },
            { sandboxId: 'sbx-3', runId: 'run-live-017-a', state: 'RUNTIME_STATE_MYSTERY', generation: 3, binding: { runtimeUnitId: 'unit-1', deviceIds: ['gpu-2'], resources: { cpuMillis: 1600, acceleratorUnits: 1 } }, share: 0.5, priority: 12, safePoint: false, observedAt: '2026-08-27T08:22:00Z' },
          ],
        });
      }
      if (url.pathname === '/v1/jobs/job-live-017/sandboxes') {
        expect(url.searchParams.get('run_id')).toBe('run-live-017-a');
        expect(url.searchParams.get('page_token')).toBe('sandbox-page-2');
        expect(url.searchParams.get('limit')).toBe('25');
        return jsonResponse({
          sandboxes: [
            { sandboxId: 'sbx-1', runId: 'run-live-017-a', state: 'RUNTIME_STATE_RUNNING', generation: 1, binding: { deviceIds: ['MIG-device-0'], resources: { cpuMillis: 2000, acceleratorUnits: 1 } }, share: 0.5, priority: 10, safePoint: true, observedAt: '2026-08-27T08:20:00Z' },
            { sandboxId: 'sbx-2', runId: 'run-live-017-a', state: 'RUNTIME_STATE_FAILED', generation: 2, binding: { deviceIds: ['gpu-1'], resources: { cpuMillis: 1800 } }, share: 0.5, priority: 11, safePoint: false, observedAt: '2026-08-27T08:21:00Z' },
            { sandboxId: 'sbx-3', runId: 'run-live-017-a', state: 'RUNTIME_STATE_MYSTERY', generation: 3, binding: { deviceIds: ['gpu-2'], resources: { cpuMillis: 1600 } }, share: 0.5, priority: 12, safePoint: false, observedAt: '2026-08-27T08:22:00Z' },
          ],
          nextPageToken: 'sandbox-page-3',
        });
      }
      throw new Error(`unexpected URL ${url.pathname}`);
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const topology = await client.getTopology('job-live-017', { filters: { run_id: 'run-live-017-a' } });
    const sandboxes = await client.listSandboxes('job-live-017', {
      pageToken: 'sandbox-page-2',
      limit: 25,
      filters: { run_id: 'run-live-017-a' },
    });

    expect(topology.state).toBe('ready');
    expect(topology.data?.runId).toBe('run-live-017-a');
    expect(topology.data?.nodes.find((node) => node.id === 'sbx-2')?.status).toBe('down');
    expect(topology.data?.nodes.find((node) => node.id === 'gpu-0')?.kind).toBe('device');
    expect(topology.data?.edges).toContainEqual({ from: 'unit-1', to: 'sbx-1', relation: 'runs-in' });
    expect(topology.data?.edges).toContainEqual({ from: 'sbx-1', to: 'gpu-0', relation: 'scheduled-on' });
    expect(topology.data?.nodes.find((node) => node.id === 'sbx-3')?.status).toBe('degraded');
    expect(sandboxes.state).toBe('ready');
    expect(sandboxes.data?.sandboxes[0]?.id).toBe('sbx-1');
    expect(sandboxes.data?.sandboxes[0]?.gpuAttached).toBe(true);
    expect(sandboxes.data?.sandboxes[1]?.state).toBe('failed');
    expect(sandboxes.data?.sandboxes[2]?.state).toBe('unknown');
    expect(sandboxes.pageInfo?.nextPageToken).toBe('sandbox-page-3');
  });

  it('maps rolled back and unknown decision action statuses without false success', async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({
        decisions: [decisionProtoFixture],
      }),
    );
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listDecisions('job-live-017');

    expect(result.state).toBe('ready');
    expect(result.data?.[0]?.actions[0]?.status).toBe('rolled_back');
    expect(result.data?.[0]?.actions[1]?.status).toBe('unknown');
    expect(result.data?.[0]?.actions[0]).toMatchObject({
      actionId: 'act-1',
      type: 'bind',
      sandboxId: 'sbx-1',
      targetId: 'gpu-node-1',
    });
    expect(result.data?.[0]?.actions[1]).toMatchObject({
      actionId: 'act-2',
      type: 'recreate',
      sandboxId: 'sbx-2',
      targetId: 'gpu-node-2',
      detail: 'GPU_REALLOCATE_PENDING',
    });
    expect(result.data?.[0]?.actions[2]).toMatchObject({
      actionId: 'act-3',
      type: 'set_priority',
      sandboxId: 'sbx-3',
      status: 'unknown',
    });
    expect(result.data?.[0]?.selectedPlanId).toBe('plan-1');
    expect(result.data?.[0]?.selectedCandidate).toBe('cand-2');
  });

  it('uses experiments route with real filters only and strips mock-only query keys', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), 'http://localhost');
      expect(url.pathname).toBe('/v1/experiments');
      expect(url.searchParams.get('limit')).toBe('10');
      expect(url.searchParams.get('data_kind')).toBeNull();
      expect(url.searchParams.get('mode')).toBeNull();
      expect(url.searchParams.get('run_id')).toBeNull();
      return jsonResponse({
        experiments: [
          {
            experimentId: 'exp-1',
            displayName: 'Replay compare',
            state: 'EXPERIMENT_STATE_RUNNING',
            createdAt: '2026-08-27T05:00:00Z',
            summary: 'summary',
            runs: [
              {
                experimentRunId: 'exp-run-1',
                experimentId: 'exp-1',
                runId: 'run-replay-1',
                kind: 'EXPERIMENT_RUN_KIND_REPLAY',
                dataKind: 'DATA_KIND_REPLAY',
                policyVersion: 'replay-2026.08.27.2',
                configHash: 'cfg-a9f4',
                codeRevision: 'rev-a13bc1',
                metrics: [{ name: 'Reward', value: '0.76' }],
              },
            ],
          },
        ],
      });
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listExperiments({
      limit: 10,
      filters: {
        data_kind: 'replay',
        mode: 'degraded',
        run_id: 'run-replay-1',
      },
    });

    expect(result.state).toBe('ready');
    expect(result.data?.[0]?.runs[0]?.dataKind).toBe('replay');
    expect(result.data).toHaveLength(1);
  });

  it('parses structured gateway errors for not_found responses', async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse(
        {
          error: {
            code: 'not_found',
            message: 'job not found',
            details: { resource: 'job', resource_id: 'job-missing' },
            request_id: 'req-not-found-1',
          },
        },
        404,
      ),
    );
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.getJobDetail('job-missing');

    expect(result.state).toBe('error');
    expect(result.message).toBe('job not found');
    expect(result.apiError).toEqual({
      status: 404,
      code: 'not_found',
      message: 'job not found',
      details: { resource: 'job', resourceId: 'job-missing' },
      requestId: 'req-not-found-1',
    });
  });

  it('parses structured gateway errors for conflict responses on mutations', async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse(
        {
          error: {
            code: 'conflict',
            message: 'run already active',
            details: { job_id: 'job-console-1', run_id: 'run-console-1' },
            request_id: 'req-conflict-1',
          },
        },
        409,
      ),
    );
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.createRun('job-console-1');

    expect(result.state).toBe('error');
    expect(result.message).toBe('run already active');
    expect(result.apiError).toEqual({
      status: 409,
      code: 'conflict',
      message: 'run already active',
      details: { jobId: 'job-console-1', runId: 'run-console-1' },
      requestId: 'req-conflict-1',
    });
  });

  it('marks backend resource exhaustion as retryable', async () => {
    globalThis.fetch = vi.fn(async () =>
      jsonResponse(
        {
          error: {
            code: 'backend_resource_exhausted',
            message: 'backend is throttling requests',
            details: { grpc_code: 'RESOURCE_EXHAUSTED' },
          },
        },
        429,
      ),
    ) as typeof fetch;

    const result = await new HttpApiClient('').listJobs();

    expect(result.state).toBe('error');
    expect(result.retryable).toBe(true);
    expect(result.apiError?.status).toBe(429);
  });

  it('falls back safely when gateway error response is empty or invalid json', async () => {
    const fetchMock = vi.fn(async () =>
      new Response('not-json', {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      }),
    );
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listJobs();

    expect(result.state).toBe('error');
    expect(result.message).toBe('Request failed with status 500.');
    expect(result.apiError).toEqual({
      status: 500,
      code: undefined,
      message: 'Request failed with status 500.',
      details: undefined,
      requestId: undefined,
    });
  });

  it('returns retryable structured errors when fetch rejects', async () => {
    globalThis.fetch = vi.fn(async () => {
      throw new TypeError('Failed to fetch');
    }) as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listJobs();

    expect(result.state).toBe('error');
    expect(result.message).toBe('Failed to fetch');
    expect(result.retryable).toBe(true);
    expect(result.apiError).toEqual({
      status: 0,
      code: 'transport_error',
      message: 'Failed to fetch',
      details: undefined,
      requestId: undefined,
    });
  });

  it('returns retryable structured errors when fetch is aborted', async () => {
    globalThis.fetch = vi.fn(async () => {
      throw new DOMException('The operation was aborted.', 'AbortError');
    }) as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listJobs();

    expect(result.state).toBe('error');
    expect(result.message).toBe('Request aborted.');
    expect(result.retryable).toBe(true);
    expect(result.apiError).toEqual({
      status: 0,
      code: 'transport_error',
      message: 'Request aborted.',
      details: undefined,
      requestId: undefined,
    });
  });

  it('propagates AbortSignal through job detail fan-out requests', async () => {
    const signal = new AbortController().signal;
    const seenSignals: AbortSignal[] = [];

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input), 'http://localhost');
      if (init?.signal) {
        seenSignals.push(init.signal as AbortSignal);
      }
      if (url.pathname === '/v1/jobs/job-live-017') {
        return jsonResponse({
          job: {
            jobId: 'job-live-017',
            displayName: 'PPO Actor-Critic Burst',
            algorithm: 'PPO',
            state: 'JOB_STATE_RUNNING',
            rolloutMode: 'ROLLOUT_MODE_PARTIALLY_ASYNC',
            dataKind: 'DATA_KIND_LIVE',
            queue: 'priority-train',
            priority: 95,
            desiredUnits: 16,
            createdAt: '2026-08-27T08:15:00Z',
            labels: { owner: 'ops-shift-a' },
            requiredCapabilities: { elasticParallelism: true },
          },
        });
      }
      if (url.pathname === '/v1/jobs/job-live-017/runs') {
        return jsonResponse({
          runs: [
            {
              runId: 'run-live-017-a',
              jobId: 'job-live-017',
              traceId: 'trace-live-017',
              runState: 'JOB_RUN_STATE_RUNNING',
              createdAt: '2026-08-27T08:16:00Z',
              startedAt: '2026-08-27T08:18:00Z',
              dataKind: 'DATA_KIND_LIVE',
              attempt: 1,
              policyVersion: 'policy-2026.08.27.5',
            },
          ],
        });
      }
      if (url.pathname === '/v1/jobs/job-live-017/decisions') {
        expect(url.searchParams.get('run_id')).toBe('run-live-017-a');
        return jsonResponse({ decisions: [] });
      }
      if (url.pathname === '/v1/jobs/job-live-017/sandboxes') {
        expect(url.searchParams.get('run_id')).toBe('run-live-017-a');
        return jsonResponse({ sandboxes: [] });
      }
      throw new Error(`unexpected URL ${url.pathname}`);
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.getJobDetail('job-live-017', { signal });

    expect(result.state).toBe('ready');
    expect(seenSignals.length).toBeGreaterThanOrEqual(4);
    expect(seenSignals.every((seenSignal) => seenSignal === signal)).toBe(true);
  });

  it('returns a non-retryable structured error when a successful response body is invalid json', async () => {
    globalThis.fetch = vi.fn(async () =>
      new Response('not-json', {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    ) as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.listJobs();

    expect(result.state).toBe('error');
    expect(result.message).toBe('Gateway returned an invalid JSON response body.');
    expect(result.retryable).toBe(false);
    expect(result.apiError).toEqual({
      status: 0,
      code: 'transport_error',
      message: 'Gateway returned an invalid JSON response body.',
      details: undefined,
      requestId: undefined,
    });
  });

  it('matches selected candidates by binding identity and maps rejections separately', async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({
        decision: {
          ...decisionProtoFixture,
          selected_plan: {
            plan_id: 'final-plan',
            bindings: [
              {
                pending_unit_id: 'unit-1',
                device_ids: ['gpu-node-2', 'gpu-node-1'],
                generation: 3,
                resources: { cpu_millis: 500, accelerator_units: 0.5 },
              },
            ],
            actions: decisionProtoFixture.selected_plan.actions,
          },
          candidates: [
            {
              candidate_id: 'cand-1',
              plan_id: 'plan-2',
              plan: {
                plan_id: 'plan-2',
                bindings: [{ pending_unit_id: 'unit-1', device_ids: ['gpu-node-9'] }],
              },
            },
            {
              candidate_id: 'cand-2',
              plan_id: 'plan-1',
              plan: {
                plan_id: 'plan-1',
                bindings: [
                  {
                    pending_unit_id: 'unit-1',
                    device_ids: ['gpu-node-1', 'gpu-node-2'],
                    generation: 3,
                    resources: { accelerator_units: 0.5, cpu_millis: 500 },
                  },
                ],
              },
            },
          ],
          rejected_candidates: [
            {
              candidate_id: 'rejected-1',
              reason: 'CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH',
              detail: 'missing capability names: gpu',
            },
          ],
        },
      }),
    );
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.getDecisionExplorer('job-live-017', 'dec-1');

    expect(result.state).toBe('ready');
    expect(result.data?.candidates).toEqual([
      expect.objectContaining({ id: 'cand-1', deviceLabel: 'gpu-node-9', selected: false }),
      expect.objectContaining({ id: 'cand-2', deviceLabel: 'gpu-node-1, gpu-node-2', selected: true }),
    ]);
    expect(result.data?.selectedDecision?.selectedCandidate).toBe('cand-2');
    expect(result.data?.rejectedCandidates).toEqual([
      {
        id: 'rejected-1',
        reason: 'CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH',
        detail: 'missing capability names: gpu',
      },
    ]);
    expect(result.data?.relatedActions[0]).toMatchObject({
      actionId: 'act-1',
      sandboxId: 'sbx-1',
      targetId: 'gpu-node-1',
    });
  });

  it('uses real HTTP control-plane routes for create job and job commands', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input), 'http://localhost');
      if (url.pathname === '/v1/jobs') {
        expect(init?.method).toBe('POST');
        expect(init?.headers).toMatchObject({
          'Content-Type': 'application/json',
        });
        expect(String(init?.body)).toContain('"jobId":"job-console-1"');
        return jsonResponse({
          job: { jobId: 'job-console-1' },
          operation: { operationId: 'op-job-create-1', state: 'OPERATION_STATE_PENDING' },
        }, 201);
      }
      if (url.pathname === '/v1/jobs/job-console-1/runs') {
        expect(init?.method).toBe('POST');
        expect(init?.body).toBeUndefined();
        return jsonResponse({
          run: { runId: 'run-console-2' },
          operation: { operationId: 'op-job-create-run-1', state: 'OPERATION_STATE_SUCCEEDED' },
        }, 201);
      }
      if (url.pathname === '/v1/jobs/job-console-1/admit') {
        expect(init?.method).toBe('POST');
        expect(String(init?.body)).toContain('"actor":"console"');
        expect(String(init?.body)).toContain('"reason":"console:admit"');
        return jsonResponse({
          job: { jobId: 'job-console-1' },
          operation: { operationId: 'op-job-admit-1', state: 'OPERATION_STATE_SUCCEEDED' },
        });
      }
      if (url.pathname === '/v1/jobs/job-console-1/runs/run-console-1/commands/pause') {
        expect(init?.method).toBe('POST');
        expect(String(init?.body)).toContain('"actor":"console"');
        expect(String(init?.body)).toContain('"reason":"console:pause"');
        return jsonResponse({
          run: { runId: 'run-console-1' },
          operation: { operationId: 'op-job-cmd-1', state: 'OPERATION_STATE_CANCELLED' },
        });
      }
      throw new Error(`unexpected URL ${url.pathname}`);
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const created = await client.createJob({ jobId: 'job-console-1', displayName: 'Console Job' });
    const createdRun = await client.createRun('job-console-1');
    const admitted = await client.admitJob('job-console-1');
    const paused = await client.applyJobCommand('job-console-1', 'run-console-1', 'pause');

    expect(created.state).toBe('ready');
    expect(created.data?.id).toBe('job-console-1');
    expect(created.data?.operationId).toBe('op-job-create-1');
    expect(created.data?.status).toBe('accepted');
    expect(createdRun.state).toBe('ready');
    expect(createdRun.data?.id).toBe('run-console-2');
    expect(createdRun.data?.operationId).toBe('op-job-create-run-1');
    expect(createdRun.data?.status).toBe('succeeded');
    expect(admitted.state).toBe('ready');
    expect(admitted.data?.id).toBe('job-console-1');
    expect(admitted.data?.operationId).toBe('op-job-admit-1');
    expect(admitted.data?.status).toBe('succeeded');
    expect(paused.state).toBe('ready');
    expect(paused.data?.id).toBe('run-console-1');
    expect(paused.data?.operationId).toBe('op-job-cmd-1');
    expect(paused.data?.status).toBe('cancelled');
  });

  it('derives failed control action status from operation state instead of always succeeding', async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({
        replay: { replayId: 'replay-console-1' },
        operation: { operationId: 'op-replay-command-1', state: 'OPERATION_STATE_FAILED' },
      }),
    );
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const result = await client.applyReplayCommand('replay-console-1', 'stop');

    expect(result.state).toBe('ready');
    expect(result.data?.status).toBe('failed');
    expect(result.data?.operationId).toBe('op-replay-command-1');
  });

  it('uses all real HTTP job command routes', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input), 'http://localhost');
      expect(init?.method).toBe('POST');
      expect(url.pathname).toMatch(/^\/v1\/jobs\/job-console-1\/runs\/run-console-1\/commands\/(start|pause|resume|stop|retry|terminate)$/);
      expect(String(init?.body)).toContain('"actor":"console"');
      return jsonResponse({
        run: { runId: 'run-console-1' },
        operation: { operationId: `op-${url.pathname.split('/').at(-1)}` },
      });
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');

    for (const command of ['start', 'pause', 'resume', 'stop', 'retry', 'terminate'] as const) {
      const result = await client.applyJobCommand('job-console-1', 'run-console-1', command);
      expect(result.state).toBe('ready');
      expect(result.data?.id).toBe('run-console-1');
    }

    const calledPaths = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit | undefined]>).map(
      ([input]) => new URL(String(input), 'http://localhost').pathname,
    );
    expect(calledPaths).toEqual([
      '/v1/jobs/job-console-1/runs/run-console-1/commands/start',
      '/v1/jobs/job-console-1/runs/run-console-1/commands/pause',
      '/v1/jobs/job-console-1/runs/run-console-1/commands/resume',
      '/v1/jobs/job-console-1/runs/run-console-1/commands/stop',
      '/v1/jobs/job-console-1/runs/run-console-1/commands/retry',
      '/v1/jobs/job-console-1/runs/run-console-1/commands/terminate',
    ]);
  });

  it('uses real HTTP control-plane routes for replay create and commands', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input), 'http://localhost');
      if (url.pathname === '/v1/replays') {
        expect(init?.method).toBe('POST');
        expect(String(init?.body)).toContain('"replayId":"replay-console-1"');
        return jsonResponse({
          replay: { replayId: 'replay-console-1' },
          request_id: 'req-replay-create-1',
        }, 201);
      }
      if (url.pathname === '/v1/replays/replay-console-1/commands/start') {
        expect(init?.method).toBe('POST');
        expect(init?.body).toBeUndefined();
        return jsonResponse({
          replay: { replayId: 'replay-console-1' },
          request_id: 'req-replay-command-1',
        });
      }
      throw new Error(`unexpected URL ${url.pathname}`);
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');
    const created = await client.createReplay({ replayId: 'replay-console-1', jobId: 'job-console-1' });
    const started = await client.applyReplayCommand('replay-console-1', 'start');

    expect(created.state).toBe('ready');
    expect(created.data?.id).toBe('replay-console-1');
    expect(created.data?.requestId).toBe('req-replay-create-1');
    expect(started.state).toBe('ready');
    expect(started.data?.id).toBe('replay-console-1');
    expect(started.data?.requestId).toBe('req-replay-command-1');
  });

  it('uses all real HTTP replay command routes', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input), 'http://localhost');
      expect(init?.method).toBe('POST');
      expect(url.pathname).toMatch(/^\/v1\/replays\/replay-console-1\/commands\/(start|pause|resume|stop|terminate)$/);
      return jsonResponse({
        replay: { replayId: 'replay-console-1' },
        request_id: `req-${url.pathname.split('/').at(-1)}`,
      });
    });
    globalThis.fetch = fetchMock as typeof fetch;

    const client = new HttpApiClient('');

    for (const command of ['start', 'pause', 'resume', 'stop', 'terminate'] as const) {
      const result = await client.applyReplayCommand('replay-console-1', command);
      expect(result.state).toBe('ready');
      expect(result.data?.id).toBe('replay-console-1');
    }

    const calledPaths = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit | undefined]>).map(
      ([input]) => new URL(String(input), 'http://localhost').pathname,
    );
    expect(calledPaths).toEqual([
      '/v1/replays/replay-console-1/commands/start',
      '/v1/replays/replay-console-1/commands/pause',
      '/v1/replays/replay-console-1/commands/resume',
      '/v1/replays/replay-console-1/commands/stop',
      '/v1/replays/replay-console-1/commands/terminate',
    ]);
  });
});
