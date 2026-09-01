import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { MockApiClient } from '../api/client';
import { AppLayoutWithClient } from '../app/layout';
import type { ApiClient, OverviewResponse, QueryResult } from '../api/types';
import { createTestApiClient, ready } from '../test/testApiClient';
import { OverviewPage } from './OverviewPage';

function renderPage(client: ApiClient = new MockApiClient()) {
  const router = createMemoryRouter(
    [
      {
        path: '/',
        element: <AppLayoutWithClient client={client} />,
        children: [{ index: true, element: <OverviewPage /> }],
      },
    ],
    { initialEntries: ['/'] },
  );
  return render(<RouterProvider router={router} />);
}

function overviewWithJob(name: string): OverviewResponse {
  return {
    metrics: [],
    jobs: [
      {
        id: `job-${name.toLowerCase().replaceAll(' ', '-')}`,
        name,
        algorithm: 'PPO',
        state: 'running',
        rolloutMode: 'sync',
        dataKind: 'live',
        queue: 'default',
        priority: 1,
        desiredUnits: 1,
        gpuRequired: false,
        createdAt: '2026-08-27T00:00:00Z',
        updatedAt: '2026-08-27T00:00:00Z',
        owner: 'test',
        policyVersion: 'policy-test',
        traceId: 'trace-test',
        executionId: 'execution-test',
        health: 'healthy',
      },
    ],
    experiments: [],
    decisions: [],
    capabilities: { protocolVersion: 'v0.3', dataKinds: [], pagination: 'opaque' },
    systemHealth: { status: 'ok', observedAt: '', counts: {} },
    alerts: [],
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

describe('OverviewPage', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it('renders loading and ready overview content from the client', async () => {
    const pending = new Promise<QueryResult<OverviewResponse>>((resolve) => {
      window.setTimeout(
        () =>
          resolve(
            ready({
              metrics: [{ label: '保留任务', value: '2' }],
              jobs: [
                {
                  id: 'job-live-017',
                  name: 'PPO Actor-Critic Burst',
                  algorithm: 'PPO',
                  state: 'running',
                  rolloutMode: 'partially_async',
                  dataKind: 'live',
                  queue: 'priority-train',
                  priority: 95,
                  desiredUnits: 16,
                  gpuRequired: true,
                  createdAt: '2026-08-27T08:15:00Z',
                  updatedAt: '2026-08-27T08:18:00Z',
                  owner: 'ops-shift-a',
                  policyVersion: 'policy-2026.08.27.5',
                  traceId: 'trace-live-017',
                  executionId: 'exec-live-017',
                  currentRunId: 'run-live-017-a',
                  latestRunState: 'JOB_RUN_STATE_RUNNING',
                  runCount: 1,
                  health: 'degraded',
                },
              ],
              experiments: [],
              decisions: [],
              capabilities: { protocolVersion: 'v0.3', dataKinds: ['DATA_KIND_LIVE'], pagination: 'opaque' },
              systemHealth: { status: 'ok', observedAt: '2026-08-27T00:00:00Z', counts: { jobs: 2 } },
              alerts: [{ id: 'alert-1', title: '加速卡压力', tone: 'warn', detail: '加速卡余量偏低。' }],
            }),
          ),
        0,
      );
    });
    const client = createTestApiClient({
      listOverview: vi.fn(async () => pending),
    });

    renderPage(client);

    expect(screen.getByText('正在加载控制面数据')).toBeInTheDocument();
    expect(await screen.findByText('PPO Actor-Critic Burst')).toBeInTheDocument();
    expect(screen.getByText('加速卡压力')).toBeInTheDocument();
    expect(screen.getByText('任务总数')).toBeInTheDocument();
  });

  it('hides mock-only surface controls outside mock mode', async () => {
    renderPage();

    expect(await screen.findByRole('heading', { name: '运行总览' })).toBeInTheDocument();
    expect(screen.queryByLabelText('模拟状态')).not.toBeInTheDocument();
  });

  it('switches into forbidden state rendering in mock mode', async () => {
    const user = userEvent.setup();
    vi.stubEnv('VITE_TGSRL_API_ADAPTER', 'mock');
    renderPage();

    expect(await screen.findByRole('heading', { name: '运行总览' })).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText('模拟状态'), 'forbidden');

    expect(await screen.findByText('没有访问权限')).toBeInTheDocument();
  });

  it('passes selected source kind through overview query filters', async () => {
    const user = userEvent.setup();
    const listOverview = vi.fn(async () =>
      ready({
        metrics: [],
        jobs: [],
        experiments: [],
        decisions: [],
        capabilities: { protocolVersion: 'v0.3', dataKinds: [], pagination: 'opaque' },
        systemHealth: { status: 'ok', observedAt: '', counts: {} },
        alerts: [],
      }),
    );

    renderPage(createTestApiClient({ listOverview }));
    await screen.findByRole('heading', { name: '运行总览' });

    await user.selectOptions(screen.getByLabelText('数据来源'), 'replay');

    expect(listOverview).toHaveBeenLastCalledWith({
      filters: {
        data_kind: 'replay',
        mode: 'ready',
      },
      signal: expect.any(AbortSignal),
    });
  });

  it('renders an error state when the overview query rejects', async () => {
    renderPage(
      createTestApiClient({
        listOverview: vi.fn(async () => {
          throw new Error('overview exploded');
        }),
      }),
    );

    expect(await screen.findByText('请求失败')).toBeInTheDocument();
    expect(screen.getByText('overview exploded')).toBeInTheDocument();
  });

  it('aborts the active overview request when the page unmounts', async () => {
    let requestSignal: AbortSignal | undefined;
    const listOverview = vi.fn((options) => {
      requestSignal = options?.signal;
      return new Promise<QueryResult<OverviewResponse>>(() => undefined);
    });

    const view = renderPage(createTestApiClient({ listOverview }));
    await waitFor(() => expect(requestSignal).toBeInstanceOf(AbortSignal));
    expect(requestSignal?.aborted).toBe(false);

    view.unmount();

    expect(requestSignal?.aborted).toBe(true);
  });

  it('does not let an older overview response overwrite a newer filter result', async () => {
    const user = userEvent.setup();
    const first = deferred<QueryResult<OverviewResponse>>();
    const second = deferred<QueryResult<OverviewResponse>>();
    const requestSignals: AbortSignal[] = [];
    const listOverview = vi.fn((options) => {
      if (options?.signal) {
        requestSignals.push(options.signal);
      }
      return listOverview.mock.calls.length === 1 ? first.promise : second.promise;
    });

    renderPage(createTestApiClient({ listOverview }));
    await waitFor(() => expect(listOverview).toHaveBeenCalledTimes(1));
    await user.selectOptions(screen.getByLabelText('数据来源'), 'replay');
    await waitFor(() => expect(listOverview).toHaveBeenCalledTimes(2));
    expect(requestSignals[0]?.aborted).toBe(true);

    await act(async () => {
      second.resolve(ready(overviewWithJob('Fresh replay result')));
    });
    expect(await screen.findByText('Fresh replay result')).toBeInTheDocument();

    await act(async () => {
      first.resolve(ready(overviewWithJob('Stale live result')));
    });
    expect(screen.queryByText('Stale live result')).not.toBeInTheDocument();
    expect(screen.getByText('Fresh replay result')).toBeInTheDocument();
  });
});
