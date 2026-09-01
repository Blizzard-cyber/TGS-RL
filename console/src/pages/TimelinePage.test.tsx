import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { AppLayoutWithClient } from '../app/layout';
import type { TimelineResponse } from '../api/types';
import { createTestApiClient, ready } from '../test/testApiClient';
import { TimelinePage } from './TimelinePage';

function renderPage(initialEntry = '/timeline?jobId=job-live-017', client = createTestApiClient()) {
  const router = createMemoryRouter(
    [
      {
        path: '/',
        element: <AppLayoutWithClient client={client} />,
        children: [
          { path: 'timeline', element: <TimelinePage /> },
          { path: 'jobs/:jobId/timeline', element: <TimelinePage /> },
        ],
      },
    ],
    { initialEntries: [initialEntry] },
  );
  return render(<RouterProvider router={router} />);
}

describe('TimelinePage', () => {
  it('does not query an empty job scope', async () => {
    const listTimeline = vi.fn();
    renderPage('/timeline', createTestApiClient({ listTimeline }));

    expect(await screen.findByText('Select a job to view its timeline.')).toBeInTheDocument();
    expect(listTimeline).not.toHaveBeenCalled();
  });

  it('passes job and run filters into the timeline query', async () => {
    const user = userEvent.setup();
    const timelineResponse: TimelineResponse = {
        events: [
          {
            id: 'evt-909',
            jobId: 'job-live-017',
            runId: 'run-live-017-a',
            title: 'Decision applied',
            type: 'decision-applied' as const,
            occurredAt: '2026-08-27T14:28:31Z',
            sequence: 909,
            phase: 'actor-rollout',
            sandboxId: 'sbx-live-a14',
            decisionId: 'dec-7104',
            dataKind: 'live' as const,
            severity: 'info' as const,
            summary: 'Applied decision dec-7104 to rebalance actor capacity.',
          },
        ],
      };
    const listTimeline = vi.fn(async () => ready(timelineResponse));

    renderPage('/jobs/job-live-017/timeline?runId=run-live-017-a', createTestApiClient({ listTimeline }));

    expect(await screen.findByText('Decision applied')).toBeInTheDocument();
    expect(listTimeline).toHaveBeenLastCalledWith('job-live-017', {
      filters: {
        mode: 'ready',
        run_id: 'run-live-017-a',
      },
      signal: expect.any(AbortSignal),
    });

    await user.clear(screen.getByLabelText('Run filter'));
    await waitFor(() =>
      expect(listTimeline).toHaveBeenLastCalledWith('job-live-017', {
        filters: {
          mode: 'ready',
        },
        signal: expect.any(AbortSignal),
      }),
    );
  });

  it('renders error state returned by the client', async () => {
    renderPage(
      '/timeline?jobId=job-live-017',
      createTestApiClient({
        listTimeline: async () => ({ state: 'error', message: 'timeline failed', retryable: true }),
      }),
    );

    expect(await screen.findByText('Request failed')).toBeInTheDocument();
    expect(screen.getByText('timeline failed')).toBeInTheDocument();
  });

  it('renders an error state when the timeline query rejects', async () => {
    renderPage(
      '/timeline?jobId=job-live-017',
      createTestApiClient({
        listTimeline: async () => {
          throw new Error('timeline blew up');
        },
      }),
    );

    expect(await screen.findByText('Request failed')).toBeInTheDocument();
    expect(screen.getByText('timeline blew up')).toBeInTheDocument();
  });
});
