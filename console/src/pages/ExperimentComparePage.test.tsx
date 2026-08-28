import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { AppLayoutWithClient } from '../app/layout';
import type { ExperimentSummary } from '../api/types';
import { createTestApiClient, ready } from '../test/testApiClient';
import { ExperimentComparePage } from './ExperimentComparePage';

function renderPage(client = createTestApiClient()) {
  const router = createMemoryRouter(
    [
      {
        path: '/',
        element: <AppLayoutWithClient client={client} />,
        children: [{ path: 'experiments', element: <ExperimentComparePage /> }],
      },
    ],
    { initialEntries: ['/experiments'] },
  );
  return render(<RouterProvider router={router} />);
}

describe('ExperimentComparePage', () => {
  it('requests experiments with source filters and lets the user switch selected experiment', async () => {
    const user = userEvent.setup();
    const experiments: ExperimentSummary[] = [
        {
          id: 'exp-ppo-compare',
          name: 'PPO rollout policy compare',
          state: 'running' as const,
          createdAt: '2026-08-27T05:00:00Z',
          summary: 'Comparing live PPO against replayed baseline.',
          runs: [
            {
              id: 'run-live-ppo',
              experimentId: 'exp-ppo-compare',
              runId: 'run-live-ppo',
              label: 'Live LIVE',
              kind: 'live' as const,
              dataKind: 'live' as const,
              policyVersion: 'policy-2026.08.27.5',
              configHash: 'cfg-a9f4',
              codeRevision: 'rev-a13bc1',
              summary: 'Highest throughput.',
              metrics: [{ label: 'Reward', value: '0.81' }],
            },
          ],
        },
        {
          id: 'exp-capacity-sim',
          name: 'Synthetic GPU scarcity drill',
          state: 'failed' as const,
          createdAt: '2026-08-26T21:10:00Z',
          completedAt: '2026-08-27T13:53:00Z',
          summary: 'Synthetic drill exhausted READY GPU capacity.',
          runs: [
            {
              id: 'run-sim-a3c',
              experimentId: 'exp-capacity-sim',
              runId: 'run-sim-a3c',
              label: 'Synthetic SYNTHETIC',
              kind: 'simulation' as const,
              dataKind: 'synthetic' as const,
              policyVersion: 'sim-2026.08.26.9',
              configHash: 'cfg-b29d',
              codeRevision: 'rev-ff02a1',
              summary: 'Stopped on GPU unavailable fallback path.',
              metrics: [{ label: 'Fallbacks', value: '17' }],
            },
          ],
        },
      ];
    const listExperiments = vi.fn(async () => ready(experiments));

    renderPage(createTestApiClient({ listExperiments }));

    expect((await screen.findAllByText('PPO rollout policy compare')).length).toBeGreaterThan(0);
    expect(screen.getByText('Reward: 0.81')).toBeInTheDocument();

    const experimentButtons = screen.getAllByRole('button');
    expect(experimentButtons[1]).toBeDefined();
    await user.click(experimentButtons[1] as HTMLElement);
    expect(await screen.findByText('Fallbacks: 17')).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText('Source'), 'synthetic');
    await waitFor(() =>
      expect(listExperiments).toHaveBeenLastCalledWith({
        filters: {
          data_kind: 'synthetic',
          mode: 'ready',
        },
        signal: expect.any(AbortSignal),
      }),
    );
    expect(screen.getByRole('button', { name: /synthetic gpu scarcity drill/i })).toHaveAttribute('aria-pressed', 'true');
  });

  it('renders empty state when no experiments are retained', async () => {
    renderPage(
      createTestApiClient({
        listExperiments: async () => ready([]),
      }),
    );

    expect(await screen.findByText('Select an experiment to compare retained runs.')).toBeInTheDocument();
  });
});
