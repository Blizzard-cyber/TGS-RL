import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import type { QueryResult } from '../api/types';
import { MockApiClient } from '../api/client';
import { AppLayoutWithClient } from '../app/layout';
import { createTestApiClient, ready } from '../test/testApiClient';
import { DecisionExplorerPage } from './DecisionExplorerPage';

function renderPage(initialEntry = '/decisions', client = new MockApiClient()) {
  const router = createMemoryRouter(
    [
      {
        path: '/',
        element: <AppLayoutWithClient client={client} />,
        children: [{ path: 'decisions', element: <DecisionExplorerPage /> }],
      },
    ],
    { initialEntries: [initialEntry] },
  );
  return render(<RouterProvider router={router} />);
}

describe('DecisionExplorerPage', () => {
  it('renders retained decisions and lets the user paginate', async () => {
    renderPage('/decisions?jobId=job-live-017');

    expect(await screen.findByText('dec-7104')).toBeInTheDocument();
    expect(screen.queryByText('dec-7098')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled();
  });

  it('uses page token and selected decision to drive explorer fetches', async () => {
    const user = userEvent.setup();
    const listDecisions = vi
      .fn()
      .mockResolvedValueOnce({
        state: 'ready',
        data: [
          {
            id: 'dec-7104',
            sequence: 7104,
            jobId: 'job-live-017',
            runId: 'run-live-017-a',
            traceId: 'trace-live-017',
            stageId: 'actor-rollout',
            decidedAt: '2026-08-27T14:28:31Z',
            fallback: false,
            selectedPlanId: 'plan-7104',
            selectedCandidate: 'gpu-cell-4',
            policyVersion: 'policy-2026.08.27.5',
            summary: 'first page',
            actions: [],
          },
          {
            id: 'dec-7103',
            sequence: 7103,
            jobId: 'job-live-017',
            runId: 'run-live-017-a',
            traceId: 'trace-live-017',
            stageId: 'actor-rollout',
            decidedAt: '2026-08-27T14:27:31Z',
            fallback: false,
            selectedPlanId: 'plan-7103',
            selectedCandidate: 'gpu-cell-2',
            policyVersion: 'policy-2026.08.27.5',
            summary: 'first page second row',
            actions: [],
          },
        ],
        pageInfo: { nextPageToken: 'opaque-2' },
      } satisfies QueryResult<unknown>)
      .mockResolvedValueOnce({
        state: 'ready',
        data: [
          {
            id: 'dec-7102',
            sequence: 7102,
            jobId: 'job-live-017',
            runId: 'run-live-017-a',
            traceId: 'trace-live-017',
            stageId: 'learner',
            decidedAt: '2026-08-27T14:26:31Z',
            fallback: true,
            policyVersion: 'policy-2026.08.27.4',
            summary: 'second page',
            actions: [],
          },
        ],
        pageInfo: {},
      });
    const getDecisionExplorer = vi.fn(async (_jobId: string, decisionId: string) =>
      ready({
        selectedDecision: {
          id: decisionId,
          sequence: 1,
          jobId: 'job-live-017',
          traceId: 'trace-live-017',
          stageId: 'actor-rollout',
          decidedAt: '2026-08-27T14:28:31Z',
          fallback: false,
          policyVersion: 'policy-2026.08.27.5',
          summary: `explorer ${decisionId}`,
          actions: [],
        },
        candidates: [
          { id: 'gpu-cell-4', deviceLabel: 'gpu-cell-4', score: 0.91, reason: 'best', selected: true },
          { id: 'gpu-cell-2', deviceLabel: 'gpu-cell-2', score: 0.72, reason: 'eligible', selected: false },
        ],
        rejectedCandidates: [
          { id: 'cpu-bank-2', reason: 'CAPABILITY_MISMATCH', detail: 'GPU capability is required.' },
        ],
        relatedActions: [],
      }),
    );

    renderPage(
      '/decisions?jobId=job-live-017&runId=run-live-017-a',
      createTestApiClient({
        listDecisions,
        getDecisionExplorer,
      }),
    );

    expect(await screen.findByText('dec-7104')).toBeInTheDocument();
    expect(await screen.findByText('Feasible')).toBeInTheDocument();
    expect(screen.getByText('CAPABILITY MISMATCH')).toBeInTheDocument();
    expect(screen.getByText('GPU capability is required.')).toBeInTheDocument();
    await waitFor(() =>
      expect(getDecisionExplorer).toHaveBeenCalledWith('job-live-017', 'dec-7104', {
        filters: { mode: 'ready' },
        signal: expect.any(AbortSignal),
      }),
    );
    expect(listDecisions).toHaveBeenNthCalledWith(1, 'job-live-017', {
      limit: 2,
      pageToken: undefined,
      filters: {
        mode: 'ready',
        run_id: 'run-live-017-a',
      },
      signal: expect.any(AbortSignal),
    });
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /dec-7104/i })).toHaveAttribute('aria-pressed', 'true'),
    );

    await user.click(screen.getByText('dec-7103').closest('button') as HTMLElement);
    await waitFor(() =>
      expect(getDecisionExplorer).toHaveBeenLastCalledWith('job-live-017', 'dec-7103', {
        filters: { mode: 'ready' },
        signal: expect.any(AbortSignal),
      }),
    );
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /dec-7103/i })).toHaveAttribute('aria-pressed', 'true'),
    );

    await user.click(screen.getByRole('button', { name: 'Next' }));
    await waitFor(() =>
      expect(listDecisions).toHaveBeenLastCalledWith('job-live-017', {
        limit: 2,
        pageToken: 'opaque-2',
        filters: {
          mode: 'ready',
          run_id: 'run-live-017-a',
        },
        signal: expect.any(AbortSignal),
      }),
    );
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /dec-7102/i })).toHaveAttribute('aria-pressed', 'true'),
    );
    expect(screen.getByRole('button', { name: 'Prev' })).toBeEnabled();
  });

  it('falls back to the first decision on the new page when the prior selection is no longer present', async () => {
    const user = userEvent.setup();
    const listDecisions = vi
      .fn()
      .mockResolvedValueOnce({
        state: 'ready',
        data: [
          {
            id: 'dec-7104',
            sequence: 7104,
            jobId: 'job-live-017',
            runId: 'run-live-017-a',
            traceId: 'trace-live-017',
            stageId: 'actor-rollout',
            decidedAt: '2026-08-27T14:28:31Z',
            fallback: false,
            selectedPlanId: 'plan-7104',
            selectedCandidate: 'gpu-cell-4',
            policyVersion: 'policy-2026.08.27.5',
            summary: 'first page',
            actions: [],
          },
          {
            id: 'dec-7103',
            sequence: 7103,
            jobId: 'job-live-017',
            runId: 'run-live-017-a',
            traceId: 'trace-live-017',
            stageId: 'actor-rollout',
            decidedAt: '2026-08-27T14:27:31Z',
            fallback: false,
            selectedPlanId: 'plan-7103',
            selectedCandidate: 'gpu-cell-2',
            policyVersion: 'policy-2026.08.27.5',
            summary: 'first page second row',
            actions: [],
          },
        ],
        pageInfo: { nextPageToken: 'opaque-2' },
      } satisfies QueryResult<unknown>)
      .mockResolvedValueOnce({
        state: 'ready',
        data: [
          {
            id: 'dec-7102',
            sequence: 7102,
            jobId: 'job-live-017',
            runId: 'run-live-017-a',
            traceId: 'trace-live-017',
            stageId: 'learner',
            decidedAt: '2026-08-27T14:26:31Z',
            fallback: true,
            selectedPlanId: 'plan-7102',
            selectedCandidate: 'gpu-cell-3',
            policyVersion: 'policy-2026.08.27.4',
            summary: 'second page',
            actions: [],
          },
        ],
        pageInfo: {},
      } satisfies QueryResult<unknown>);
    const getDecisionExplorer = vi.fn(async (_jobId: string, decisionId: string) =>
      ready({
        selectedDecision: {
          id: decisionId,
          sequence: 1,
          jobId: 'job-live-017',
          traceId: 'trace-live-017',
          stageId: 'actor-rollout',
          decidedAt: '2026-08-27T14:28:31Z',
          fallback: false,
          policyVersion: 'policy-2026.08.27.5',
          summary: `explorer ${decisionId}`,
          actions: [],
        },
        candidates: [],
        rejectedCandidates: [],
        relatedActions: [],
      }),
    );

    renderPage(
      '/decisions?jobId=job-live-017&runId=run-live-017-a',
      createTestApiClient({
        listDecisions,
        getDecisionExplorer,
      }),
    );

    expect(await screen.findByText('dec-7104')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /dec-7103/i }));
    await waitFor(() =>
      expect(getDecisionExplorer).toHaveBeenLastCalledWith('job-live-017', 'dec-7103', {
        filters: { mode: 'ready' },
        signal: expect.any(AbortSignal),
      }),
    );

    await user.click(screen.getByRole('button', { name: 'Next' }));

    await waitFor(() =>
      expect(screen.getByRole('button', { name: /dec-7102/i })).toHaveAttribute('aria-pressed', 'true'),
    );
    await waitFor(() =>
      expect(getDecisionExplorer).toHaveBeenLastCalledWith('job-live-017', 'dec-7102', {
        filters: { mode: 'ready' },
        signal: expect.any(AbortSignal),
      }),
    );
  });

  it('renders empty candidate analysis when no decision is selected', async () => {
    const listDecisions = vi.fn();
    renderPage(
      '/decisions',
      createTestApiClient({
        listDecisions,
      }),
    );

    expect(await screen.findByText('Select a job to inspect its decisions.')).toBeInTheDocument();
    expect(await screen.findByText('Select a decision to inspect candidates.')).toBeInTheDocument();
    expect(listDecisions).not.toHaveBeenCalled();
  });
});
