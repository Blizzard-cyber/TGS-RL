import { render, screen, waitFor } from '@testing-library/react';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { AppLayoutWithClient } from '../app/layout';
import { createTestApiClient, ready } from '../test/testApiClient';
import type { TraceEventRecord } from '../api/types';
import { TraceExplorerPage } from './TraceExplorerPage';

function renderPage(initialEntry = '/traces?jobId=job-live-017&runId=run-live-017-a') {
  const listTraces = vi.fn(async () => ready({
    events: [{
      id: 'evt-trace-1', jobId: 'job-live-017', runId: 'run-live-017-a', traceId: 'trace-live-017',
      executionId: 'exec-live-017', sequence: 17, occurredAt: '2026-08-27T14:28:31Z',
      type: 'decision-applied', phaseId: 'actor-rollout', stageId: 'actor-rollout', algorithm: 'PPO',
      rolloutMode: 'partially_async', policyVersion: 'policy-5', bufferLevel: 8, safePoint: true,
      decisionId: 'dec-7104', sandboxId: 'sbx-live-a14', generation: 7, dataKind: 'live' as const,
      attributes: { source: 'runtime', duration_ms: '42.5', component: 'executor' },
    }],
  }));
  const client = createTestApiClient({
    listJobs: async () => ready([{
      id: 'job-live-017', name: '训练任务', algorithm: 'PPO', state: 'running', rolloutMode: 'partially_async',
      dataKind: 'live', queue: 'default', priority: 1, desiredUnits: 1, gpuRequired: true,
      createdAt: '', updatedAt: '', owner: 'owner', policyVersion: 'policy-5', traceId: 'trace-live-017',
      executionId: 'exec-live-017', currentRunId: 'run-live-017-a', health: 'healthy',
    }]),
    listTraces,
  });
  const router = createMemoryRouter([{ path: '/', element: <AppLayoutWithClient client={client} />, children: [{ path: 'traces', element: <TraceExplorerPage /> }] }], { initialEntries: [initialEntry] });
  render(<RouterProvider router={router} />);
  return { listTraces };
}

describe('TraceExplorerPage', () => {
  it('展示真实链路身份和关联入口', async () => {
    const { listTraces } = renderPage();
    expect(await screen.findByRole('heading', { name: '链路追踪' })).toBeInTheDocument();
    expect((await screen.findAllByText('trace-live-017')).length).toBeGreaterThan(0);
    expect(screen.getByRole('link', { name: '查看关联决策' })).toHaveAttribute('href', '/jobs/job-live-017/decisions?runId=run-live-017-a&decisionId=dec-7104');
    expect(screen.getByText('source')).toBeInTheDocument();
    expect(screen.getAllByText('42.50 毫秒').length).toBeGreaterThan(0);
    expect(screen.getAllByText('推理调度').length).toBeGreaterThan(0);
    await waitFor(() => expect(listTraces).toHaveBeenCalledWith('job-live-017', expect.objectContaining({ filters: expect.objectContaining({ run_id: 'run-live-017-a' }) })));
  });

  it('展示请求到执行器和工作进程的跨轨关联', async () => {
    const events: TraceEventRecord[] = [
        {
          id: 'request-span', jobId: 'job-live-017', runId: 'run-live-017-a', traceId: 'trace-live-017',
          executionId: 'exec-live-017', sequence: 1, occurredAt: '2026-08-27T14:28:31Z',
          type: 'sample_produced', phaseId: 'actor-rollout', stageId: 'agent-loop', algorithm: 'PPO',
          rolloutMode: 'partially_async', policyVersion: 'policy-5', bufferLevel: 8, safePoint: false,
          generation: 7, dataKind: 'live' as const, attributes: { component: 'request', request_id: 'req-42', duration_ms: '18' },
        },
        {
          id: 'executor-span', jobId: 'job-live-017', runId: 'run-live-017-a', traceId: 'trace-live-017',
          executionId: 'exec-live-017', sequence: 2, occurredAt: '2026-08-27T14:28:31.004Z',
          type: 'decision_applied', phaseId: 'decode', stageId: 'executor', algorithm: 'PPO',
          rolloutMode: 'partially_async', policyVersion: 'policy-5', bufferLevel: 8, safePoint: false,
          generation: 7, dataKind: 'live' as const, attributes: { component: 'executor', request_id: 'req-42', executor_id: 'executor-0', duration_ms: '10', batch_size: '16' },
        },
        {
          id: 'worker-span', jobId: 'job-live-017', runId: 'run-live-017-a', traceId: 'trace-live-017',
          executionId: 'exec-live-017', sequence: 3, occurredAt: '2026-08-27T14:28:31.006Z',
          type: 'sample_consumed', phaseId: 'decode', stageId: 'model-forward', algorithm: 'PPO',
          rolloutMode: 'partially_async', policyVersion: 'policy-5', bufferLevel: 8, safePoint: false,
          generation: 7, dataKind: 'live' as const, attributes: { component: 'worker', request_id: 'req-42', executor_id: 'executor-0', worker_id: 'worker-0', duration_ms: '7', batch_size: '8' },
        },
      ];
    const listTraces = vi.fn(async () => ready({ events }));
    const client = createTestApiClient({
      listJobs: async () => ready([{
        id: 'job-live-017', name: '训练任务', algorithm: 'PPO', state: 'running', rolloutMode: 'partially_async',
        dataKind: 'live', queue: 'default', priority: 1, desiredUnits: 1, gpuRequired: true, createdAt: '',
        updatedAt: '', owner: 'owner', policyVersion: 'policy-5', traceId: 'trace-live-017',
        executionId: 'exec-live-017', currentRunId: 'run-live-017-a', health: 'healthy',
      }]),
      listTraces,
    });
    const router = createMemoryRouter([{ path: '/', element: <AppLayoutWithClient client={client} />, children: [{ path: 'traces', element: <TraceExplorerPage /> }] }], { initialEntries: ['/traces?jobId=job-live-017&runId=run-live-017-a'] });
    render(<RouterProvider router={router} />);

    expect(await screen.findByRole('region', { name: '跨轨调用关联' })).toBeInTheDocument();
    expect(screen.getAllByText('req-42').length).toBeGreaterThan(0);
    expect(screen.getAllByText('executor-0').length).toBeGreaterThan(0);
    expect(screen.getAllByText('worker-0').length).toBeGreaterThan(0);
    expect(screen.getByText('18.00 毫秒 · 批量 16/8')).toBeInTheDocument();
  });
});
