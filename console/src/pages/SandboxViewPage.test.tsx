import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { AppLayoutWithClient } from '../app/layout';
import type { SandboxResponse } from '../api/types';
import { createTestApiClient, ready } from '../test/testApiClient';
import { SandboxViewPage } from './SandboxViewPage';

function renderPage(initialEntry = '/sandboxes?jobId=job-live-017', client = createTestApiClient()) {
  const router = createMemoryRouter(
    [
      {
        path: '/',
        element: <AppLayoutWithClient client={client} />,
        children: [
          { path: 'sandboxes', element: <SandboxViewPage /> },
          { path: 'jobs/:jobId/sandboxes', element: <SandboxViewPage /> },
        ],
      },
    ],
    { initialEntries: [initialEntry] },
  );
  return render(<RouterProvider router={router} />);
}

describe('SandboxViewPage', () => {
  it('does not query an empty job scope', async () => {
    const listSandboxes = vi.fn();
    renderPage('/sandboxes', createTestApiClient({ listSandboxes }));

    expect(await screen.findByText('Select a job to view its sandboxes.')).toBeInTheDocument();
    expect(listSandboxes).not.toHaveBeenCalled();
  });

  it('passes source and run filters to sandbox queries and renders retained rows', async () => {
    const user = userEvent.setup();
    const sandboxResponse: SandboxResponse = {
        sandboxes: [
          {
            id: 'sbx-live-a14',
            jobId: 'job-live-017',
            runId: 'run-live-017-a',
            state: 'running' as const,
            generation: 7,
            nodeLabel: 'gpu-cell-4',
            gpuAttached: true,
            share: 0.5,
            priority: 95,
            safePoint: true,
            offloaded: false,
            updatedAt: '2026-08-27T14:29:03Z',
            bindingSummary: 'A100:0, cpu=2400m, memory=8Gi',
          },
        ],
      };
    const listSandboxes = vi.fn(async () => ready(sandboxResponse));

    renderPage('/jobs/job-live-017/sandboxes?runId=run-live-017-a', createTestApiClient({ listSandboxes }));

    expect(await screen.findByText('sbx-live-a14')).toBeInTheDocument();
    expect(listSandboxes).toHaveBeenLastCalledWith('job-live-017', {
      filters: {
        mode: 'ready',
        run_id: 'run-live-017-a',
      },
      signal: expect.any(AbortSignal),
    });

    await user.selectOptions(screen.getByLabelText('Source'), 'live');

    await waitFor(() =>
      expect(listSandboxes).toHaveBeenLastCalledWith('job-live-017', {
        filters: {
          data_kind: 'live',
          mode: 'ready',
          run_id: 'run-live-017-a',
        },
        signal: expect.any(AbortSignal),
      }),
    );
    expect(screen.getByText('Safe point')).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Sandbox' })).toHaveAttribute('scope', 'col');
  });

  it('renders empty table label for empty retained sandboxes', async () => {
    renderPage(
      '/sandboxes?jobId=job-live-017',
      createTestApiClient({
        listSandboxes: async () => ready({ sandboxes: [] }),
      }),
    );

    expect(await screen.findByText('No sandboxes matched the current job and source filters.')).toBeInTheDocument();
  });

  it('renders an error state when the sandbox query rejects', async () => {
    renderPage(
      '/sandboxes?jobId=job-live-017',
      createTestApiClient({
        listSandboxes: async () => {
          throw new Error('sandbox fetch failed');
        },
      }),
    );

    expect(await screen.findByText('Request failed')).toBeInTheDocument();
    expect(screen.getByText('sandbox fetch failed')).toBeInTheDocument();
  });
});
