import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { AppLayoutWithClient } from '../app/layout';
import type { TopologySnapshot } from '../api/types';
import { createTestApiClient, ready } from '../test/testApiClient';
import { TopologyPage } from './TopologyPage';

function renderPage(initialEntry = '/topology?jobId=job-live-017', client = createTestApiClient()) {
  const router = createMemoryRouter(
    [
      {
        path: '/',
        element: <AppLayoutWithClient client={client} />,
        children: [
          { path: 'topology', element: <TopologyPage /> },
          { path: 'jobs/:jobId/topology', element: <TopologyPage /> },
        ],
      },
    ],
    { initialEntries: [initialEntry] },
  );
  return render(<RouterProvider router={router} />);
}

describe('TopologyPage', () => {
  it('requests topology with selected run and filters GPU-only nodes in the UI', async () => {
    const user = userEvent.setup();
    const topology: TopologySnapshot = {
        runId: 'run-live-017-a',
        manifestId: 'manifest-1',
        lastUpdated: '2026-08-27T14:30:00Z',
        nodes: [
          { id: 'job-live-017', label: 'PPO Actor-Critic Burst', kind: 'job' as const, status: 'degraded' as const, gpu: true, utilization: 0.88 },
          { id: 'gpu-cell-4', label: 'gpu-cell-4', kind: 'device' as const, status: 'busy' as const, gpu: true, share: 0.94, utilization: 0.97 },
          { id: 'cpu-bank-2', label: 'cpu-bank-2', kind: 'device' as const, status: 'ready' as const, gpu: false, share: 0.61, utilization: 0.59 },
        ],
        edges: [
          { from: 'job-live-017', to: 'gpu-cell-4', relation: 'scheduled-on' as const },
          { from: 'job-live-017', to: 'cpu-bank-2', relation: 'feeds' as const },
        ],
      };
    const getTopology = vi.fn(async () => ready(topology));

    renderPage('/jobs/job-live-017/topology?runId=run-live-017-a', createTestApiClient({ getTopology }));

    expect(await screen.findByText('PPO Actor-Critic Burst')).toBeInTheDocument();
    expect(getTopology).toHaveBeenLastCalledWith('job-live-017', {
      filters: {
        mode: 'ready',
        run_id: 'run-live-017-a',
      },
      signal: expect.any(AbortSignal),
    });

    await user.click(screen.getByLabelText('GPU path only'));

    expect(screen.queryByText('cpu-bank-2')).not.toBeInTheDocument();
    expect(screen.getAllByText('gpu-cell-4').length).toBeGreaterThan(0);
    expect(screen.queryByText('job-live-017 → cpu-bank-2')).not.toBeInTheDocument();

    await waitFor(() =>
      expect(getTopology).toHaveBeenLastCalledWith('job-live-017', {
        filters: {
          mode: 'ready',
          require_gpu: 'true',
          run_id: 'run-live-017-a',
        },
        signal: expect.any(AbortSignal),
      }),
    );
  });

  it('hides edges whose endpoints are missing from the topology node set', async () => {
    const topology: TopologySnapshot = {
      runId: 'run-live-017-a',
      manifestId: 'manifest-1',
      lastUpdated: '2026-08-27T14:30:00Z',
      nodes: [
        { id: 'job-live-017', label: 'PPO Actor-Critic Burst', kind: 'job', status: 'degraded', gpu: true, utilization: 0.88 },
        { id: 'gpu-cell-4', label: 'gpu-cell-4', kind: 'device', status: 'busy', gpu: true, share: 0.94, utilization: 0.97 },
      ],
      edges: [
        { from: 'job-live-017', to: 'gpu-cell-4', relation: 'scheduled-on' },
        { from: 'job-live-017', to: 'cpu-bank-2', relation: 'feeds' },
        { from: 'missing-node', to: 'gpu-cell-4', relation: 'blocked-by' },
      ],
    };

    renderPage(
      '/topology?jobId=job-live-017',
      createTestApiClient({
        getTopology: async () => ready(topology),
      }),
    );

    expect(await screen.findByText('job-live-017 → gpu-cell-4')).toBeInTheDocument();
    expect(screen.queryByText('job-live-017 → cpu-bank-2')).not.toBeInTheDocument();
    expect(screen.queryByText('missing-node → gpu-cell-4')).not.toBeInTheDocument();
  });

  it('renders gpu unavailable state', async () => {
    renderPage(
      '/topology?jobId=job-live-017',
      createTestApiClient({
        getTopology: async () => ({ state: 'gpu-unavailable', message: 'GPU capacity is currently unavailable.' }),
      }),
    );

    expect(await screen.findByText('GPU capacity unavailable')).toBeInTheDocument();
    expect(screen.getByText('GPU capacity is currently unavailable.')).toBeInTheDocument();
  });
});
