import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { within } from '@testing-library/react';
import { AppLayoutWithClient } from '../app/layout';
import type {
  ApiClient,
  ControlActionResult,
  JobCommand,
  JobDetailResponse,
  JobSummary,
  QueryResult,
  ReplayCommand,
  RunSummary,
} from '../api/types';
import { createTestApiClient, ready } from '../test/testApiClient';
import { JobDetailPage } from './JobDetailPage';

const job: JobSummary = {
  id: 'job-live-017',
  name: 'PPO Actor-Critic Burst',
  algorithm: 'PPO',
  state: 'running',
  rolloutMode: 'partially_async',
  dataKind: 'live',
  queue: 'priority-train',
  priority: 95,
  desiredUnits: 16,
  activeUnits: 16,
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
  health: 'healthy',
};

const jobs: JobSummary[] = [job];

const runs: RunSummary[] = [
  {
    id: 'run-live-017-a',
    jobId: 'job-live-017',
    traceId: 'trace-live-017',
    state: 'JOB_RUN_STATE_RUNNING',
    attempt: 1,
    policyVersion: 'policy-2026.08.27.5',
    createdAt: '2026-08-27T08:16:00Z',
    startedAt: '2026-08-27T08:18:00Z',
    dataKind: 'live',
    componentHealth: [],
  },
];

const detail: JobDetailResponse = {
  job,
  runs,
  selectedRunId: 'run-live-017-a',
  decisions: [],
  sandboxes: [],
  metrics: [{ label: 'Runs', value: '1' }],
};

function renderPage(client: ApiClient, initialEntry = '/jobs/job-live-017?runId=run-live-017-a') {
  const router = createMemoryRouter(
    [
      {
        path: '/',
        element: <AppLayoutWithClient client={client} />,
        children: [
          { path: 'jobs', element: <JobDetailPage /> },
          { path: 'jobs/:jobId', element: <JobDetailPage /> },
        ],
      },
    ],
    { initialEntries: [initialEntry] },
  );
  return render(<RouterProvider router={router} />);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

describe('JobDetailPage controls', () => {
  it('invokes job and replay control-plane actions through ApiClient', async () => {
    const user = userEvent.setup();
    const createJob = vi.fn(async (): Promise<QueryResult<ControlActionResult>> => ready({ status: 'accepted', message: 'job created', id: 'job-console-new' }));
    const createRun = vi.fn(async (): Promise<QueryResult<ControlActionResult>> => ready({ status: 'accepted', message: 'run created', id: 'run-live-017-b' }));
    const admitJob = vi.fn(async (): Promise<QueryResult<ControlActionResult>> => ready({ status: 'accepted', message: 'job admitted', id: 'job-live-017' }));
    const applyJobCommand = vi.fn(async (): Promise<QueryResult<ControlActionResult>> => ready({ status: 'accepted', message: 'job command applied', id: 'run-live-017-a' }));
    const createReplay = vi.fn(async (): Promise<QueryResult<ControlActionResult>> => ready({ status: 'accepted', message: 'replay created', id: 'replay-console-new' }));
    const applyReplayCommand = vi.fn(async (): Promise<QueryResult<ControlActionResult>> => ready({ status: 'accepted', message: 'replay command applied', id: 'replay-console-new' }));

    const client: ApiClient = {
      listOverview: async () => ready({ metrics: [], jobs: [], experiments: [], decisions: [], capabilities: { protocolVersion: 'v0.3', dataKinds: [], pagination: 'opaque' }, systemHealth: { status: 'ok', observedAt: '', counts: {} }, alerts: [] }),
      listJobs: async () => ready(jobs),
      getJobDetail: async () => ready(detail),
      listRuns: async () => ready(runs),
      listTimeline: async () => ready({ events: [] }),
      getTopology: async () => ready({ nodes: [], edges: [], lastUpdated: '' }),
      listSandboxes: async () => ready({ sandboxes: [] }),
      getDecisionExplorer: async () => ready({ candidates: [], rejectedCandidates: [], relatedActions: [] }),
      listDecisions: async () => ready([]),
      listExperiments: async () => ready([]),
      createJob,
      createRun,
      admitJob,
      applyJobCommand,
      createReplay,
      applyReplayCommand,
    };

    renderPage(client);

    expect(await screen.findByRole('button', { name: 'Create Job' })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Create Job' }));
    await waitFor(() => expect(createJob).toHaveBeenCalledTimes(1));

    await user.click(screen.getByRole('button', { name: 'Create Run' }));
    await waitFor(() => expect(createRun).toHaveBeenCalledWith('job-live-017'));
    expect(screen.getByRole('button', { name: /ppo actor-critic burst/i })).toHaveAttribute('aria-pressed', 'true');

    const jobRunCommands = screen.getByText('Job Run Commands').closest('.list-card');
    expect(jobRunCommands).not.toBeNull();
    await user.click(within(jobRunCommands as HTMLElement).getByRole('button', { name: 'Admit' }));
    await waitFor(() => expect(admitJob).toHaveBeenCalledWith('job-live-017'));
    await user.click(within(jobRunCommands as HTMLElement).getByRole('button', { name: 'Start' }));
    await waitFor(() => expect(applyJobCommand).toHaveBeenCalledWith('job-live-017', 'run-live-017-a', 'start'));
    await user.click(within(jobRunCommands as HTMLElement).getByRole('button', { name: 'Pause' }));
    await waitFor(() => expect(applyJobCommand).toHaveBeenCalledWith('job-live-017', 'run-live-017-a', 'pause'));

    await user.click(screen.getByRole('button', { name: 'Create Replay' }));
    await waitFor(() => expect(createReplay).toHaveBeenCalledTimes(1));
    expect(await screen.findByRole('status')).toHaveTextContent('Control action succeeded');
    expect(screen.getByRole('status')).toHaveAttribute('aria-live', 'polite');

    const replayCommands = screen.getByText('Replay Commands').closest('.list-card');
    expect(replayCommands).not.toBeNull();
    await user.click(within(replayCommands as HTMLElement).getByRole('button', { name: 'Start' }));
    await waitFor(() => expect(applyReplayCommand).toHaveBeenCalledWith('replay-console-new', 'start'));
  });

  it('uses the selected job boundary instead of a hardcoded default in replay draft payloads', async () => {
    const user = userEvent.setup();
    const createReplay = vi.fn(async (): Promise<QueryResult<ControlActionResult>> => ready({ status: 'accepted', message: 'replay created', id: 'replay-console-new' }));
    const jobB = { ...job, id: 'job-replay-204', name: 'Replay Latency Audit', dataKind: 'replay' as const, currentRunId: 'run-replay-204-a' };
    const client = createTestApiClient({
      listJobs: async () => ready([job, jobB]),
      getJobDetail: async (jobId) => {
        const replayRun: RunSummary = {
          id: 'run-replay-204-a',
          jobId: 'job-replay-204',
          traceId: 'trace-replay-204',
          state: 'JOB_RUN_STATE_RUNNING',
          attempt: 1,
          policyVersion: 'replay-2026.08.27.2',
          createdAt: '2026-08-27T07:02:00Z',
          startedAt: '2026-08-27T07:03:00Z',
          dataKind: 'replay',
          componentHealth: [],
        };
        return ready<JobDetailResponse>({
          ...detail,
          job: jobId === 'job-replay-204' ? jobB : job,
          runs: jobId === 'job-replay-204' ? [replayRun] : runs,
          selectedRunId: jobId === 'job-replay-204' ? 'run-replay-204-a' : 'run-live-017-a',
        });
      },
      createReplay,
    });

    renderPage(client, '/jobs/job-replay-204');

    expect((await screen.findAllByText('Replay Latency Audit')).length).toBeGreaterThan(0);
    await user.click(screen.getByRole('button', { name: 'Create Replay' }));

    await waitFor(() =>
      expect(createReplay).toHaveBeenCalledWith({
        replayId: 'replay-console-new',
        jobId: 'job-replay-204',
        displayName: 'Console Replay',
      }),
    );
  });

  it('exposes all job and replay commands and surfaces control errors', async () => {
    const user = userEvent.setup();
    const listJobs = vi.fn(async () => ready(jobs));
    const getJobDetail = vi.fn(async () => ready(detail));
    const admitJob = vi.fn(async (): Promise<QueryResult<ControlActionResult>> =>
      ready<ControlActionResult>({ status: 'accepted', message: 'job admitted' }),
    );
    const applyJobCommand = vi.fn(async (_jobId: string, _runId: string, command: JobCommand): Promise<QueryResult<ControlActionResult>> =>
      command === 'terminate'
        ? { state: 'error', message: 'terminate rejected' }
        : ready<ControlActionResult>({ status: 'accepted', message: `job ${command}` }),
    );
    const applyReplayCommand = vi.fn(async (_replayId: string, command: ReplayCommand): Promise<QueryResult<ControlActionResult>> =>
      ready<ControlActionResult>({ status: 'accepted', message: `replay ${command}` }),
    );
    const client = createTestApiClient({
      listJobs,
      getJobDetail,
      admitJob,
      applyJobCommand,
      applyReplayCommand,
    });

    renderPage(client);

    expect(await screen.findByText('Job Run Commands')).toBeInTheDocument();
    await waitFor(() =>
      expect(listJobs).toHaveBeenCalledWith({
        limit: 50,
        filters: {
          mode: 'ready',
        },
        signal: expect.any(AbortSignal),
      }),
    );
    await waitFor(() =>
      expect(getJobDetail).toHaveBeenCalledWith('job-live-017', {
        filters: {
          mode: 'ready',
          require_gpu: 'true',
          run_id: 'run-live-017-a',
        },
        signal: expect.any(AbortSignal),
      }),
    );

    const jobRunCommands = screen.getByText('Job Run Commands').closest('.list-card');
    const replayCommands = screen.getByText('Replay Commands').closest('.list-card');
    expect(jobRunCommands).not.toBeNull();
    expect(replayCommands).not.toBeNull();

    for (const command of ['Admit', 'Start', 'Pause', 'Resume', 'Stop', 'Retry', 'Terminate']) {
      await user.click(within(jobRunCommands as HTMLElement).getByRole('button', { name: command }));
      await waitFor(() => expect(within(jobRunCommands as HTMLElement).getByRole('button', { name: command })).toBeEnabled());
    }
    expect(await screen.findByText('terminate rejected')).toBeInTheDocument();

    for (const command of ['Start', 'Pause', 'Resume', 'Stop', 'Terminate']) {
      await user.click(within(replayCommands as HTMLElement).getByRole('button', { name: command }));
      await waitFor(() => expect(within(replayCommands as HTMLElement).getByRole('button', { name: command })).toBeEnabled());
    }

    await waitFor(() => expect(admitJob).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(applyJobCommand).toHaveBeenCalledTimes(6));
    await waitFor(() => expect(applyReplayCommand).toHaveBeenCalledTimes(5));
    expect(applyJobCommand.mock.calls.map((call) => call[2])).toEqual(['start', 'pause', 'resume', 'stop', 'retry', 'terminate']);
    expect(applyReplayCommand.mock.calls.map((call) => call[1])).toEqual(['start', 'pause', 'resume', 'stop', 'terminate']);
  });

  it.each([
    ['/jobs/job-live-017', undefined],
    ['/jobs/job-live-017?runId=run-does-not-exist', 'run-does-not-exist'],
  ])('syncs %s to the resolved job run before enabling commands', async (initialEntry, initialRunId) => {
    const user = userEvent.setup();
    const getJobDetail = vi.fn(async () => ready(detail));
    const applyJobCommand = vi.fn(async (): Promise<QueryResult<ControlActionResult>> =>
      ready({ status: 'accepted', message: 'started' }),
    );
    const client = createTestApiClient({
      listJobs: async () => ready(jobs),
      getJobDetail,
      applyJobCommand,
    });

    renderPage(client, initialEntry);

    const commands = await screen.findByText('Job Run Commands');
    const commandCard = commands.closest('.list-card') as HTMLElement;
    await waitFor(() => expect(within(commandCard).getByText('run-live-017-a')).toBeInTheDocument());
    await waitFor(() =>
      expect(getJobDetail).toHaveBeenLastCalledWith('job-live-017', {
        filters: { mode: 'ready', require_gpu: 'true', run_id: 'run-live-017-a' },
        signal: expect.any(AbortSignal),
      }),
    );

    await user.click(within(commandCard).getByRole('button', { name: 'Start' }));
    await waitFor(() => expect(applyJobCommand).toHaveBeenCalledWith('job-live-017', 'run-live-017-a', 'start'));
    if (initialRunId) {
      expect(applyJobCommand).not.toHaveBeenCalledWith('job-live-017', initialRunId, 'start');
    }
  });

  it('renders and selects runs beyond the former twenty-row cutoff', async () => {
    const user = userEvent.setup();
    const baseRun = runs[0] as RunSummary;
    const manyRuns = Array.from({ length: 21 }, (_, index): RunSummary => ({
      ...baseRun,
      id: `run-live-017-${String(index + 1).padStart(2, '0')}`,
      attempt: 21 - index,
    }));
    const latestRunId = manyRuns[0]?.id ?? '';
    const client = createTestApiClient({
      listJobs: async () => ready(jobs),
      getJobDetail: async (_jobId, options) => {
        const requestedRunId = options?.filters?.run_id;
        const selectedRunId = manyRuns.some((run) => run.id === requestedRunId)
          ? requestedRunId
          : latestRunId;
        return ready({ ...detail, runs: manyRuns, selectedRunId });
      },
    });

    renderPage(client, '/jobs/job-live-017');

    const lastRun = await screen.findByRole('button', { name: 'run-live-017-21' });
    await user.click(lastRun);
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'run-live-017-21 (selected)' })).toHaveAttribute('aria-pressed', 'true'),
    );
  });

  it('keeps the validated selected run in detail surface links after choosing a historical run', async () => {
    const user = userEvent.setup();
    const baseRun = runs[0];
    expect(baseRun).toBeDefined();
    const historicalRuns: RunSummary[] = [
      {
        ...baseRun!,
        id: 'run-live-017-b',
        attempt: 2,
        createdAt: '2026-08-27T08:10:00Z',
        startedAt: '2026-08-27T08:11:00Z',
      },
      baseRun!,
    ];
    const linkedDetail: JobDetailResponse = {
      ...detail,
      runs: historicalRuns,
      selectedRunId: 'run-live-017-a',
      decisions: [
        {
          id: 'dec-7104',
          sequence: 7104,
          jobId: 'job-live-017',
          runId: 'run-live-017-b',
          traceId: 'trace-live-017',
          stageId: 'actor-rollout',
          decidedAt: '2026-08-27T14:28:31Z',
          fallback: false,
          selectedPlanId: 'plan-7104',
          selectedCandidate: 'gpu-cell-4',
          policyVersion: 'policy-2026.08.27.5',
          summary: 'linked decision',
          actions: [],
        },
      ],
    };
    const getJobDetail = vi.fn(async (_jobId: string, options?: { filters?: Record<string, string> }) =>
      ready({
        ...linkedDetail,
        selectedRunId: options?.filters?.run_id === 'run-live-017-b' ? 'run-live-017-b' : 'run-live-017-a',
      }),
    );
    const client = createTestApiClient({
      listJobs: async () => ready(jobs),
      getJobDetail,
    });

    renderPage(client, '/jobs/job-live-017?runId=run-live-017-a');

    expect(await screen.findByText('Runs (2)')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'run-live-017-b' }));

    await waitFor(() =>
      expect(getJobDetail).toHaveBeenLastCalledWith('job-live-017', {
        filters: {
          mode: 'ready',
          require_gpu: 'true',
          run_id: 'run-live-017-b',
        },
        signal: expect.any(AbortSignal),
      }),
    );

    expect(screen.getByRole('link', { name: 'Timeline' })).toHaveAttribute('href', '/jobs/job-live-017/timeline?runId=run-live-017-b');
    expect(screen.getByRole('link', { name: 'Topology' })).toHaveAttribute('href', '/jobs/job-live-017/topology?runId=run-live-017-b');
    expect(screen.getByRole('link', { name: 'Sandboxes' })).toHaveAttribute('href', '/jobs/job-live-017/sandboxes?runId=run-live-017-b');
    expect(screen.getAllByRole('link', { name: 'Decisions' })[0]).toHaveAttribute('href', '/jobs/job-live-017/decisions?runId=run-live-017-b');
    expect(screen.getByRole('link', { name: 'dec-7104' })).toHaveAttribute(
      'href',
      '/jobs/job-live-017/decisions?runId=run-live-017-b&decisionId=dec-7104',
    );
  });

  it('serializes every control action while a request is pending', async () => {
    const user = userEvent.setup();
    const pendingStart = deferred<QueryResult<ControlActionResult>>();
    const applyJobCommand = vi.fn(async (_jobId: string, _runId: string, command: JobCommand) =>
      command === 'start' ? pendingStart.promise : ready<ControlActionResult>({ status: 'accepted', message: command }),
    );
    const admitJob = vi.fn(async () => ready<ControlActionResult>({ status: 'accepted', message: 'admitted' }));
    const client = createTestApiClient({
      listJobs: async () => ready(jobs),
      getJobDetail: async () => ready(detail),
      admitJob,
      applyJobCommand,
    });

    renderPage(client);
    const commandCard = (await screen.findByText('Job Run Commands')).closest('.list-card') as HTMLElement;
    const start = within(commandCard).getByRole('button', { name: 'Start' });
    const pause = within(commandCard).getByRole('button', { name: 'Pause' });
    const admit = within(commandCard).getByRole('button', { name: 'Admit' });

    await user.click(start);
    expect(pause).toBeDisabled();
    expect(admit).toBeDisabled();
    await user.click(pause);
    await user.click(admit);
    expect(applyJobCommand).toHaveBeenCalledTimes(1);
    expect(admitJob).not.toHaveBeenCalled();

    pendingStart.resolve(ready({ status: 'accepted', message: 'start completed' }));
    expect(await screen.findByText('start completed')).toBeInTheDocument();
    await waitFor(() => expect(pause).toBeEnabled());
  });

  it('renders empty detail state when no job is selected', async () => {
    const client = createTestApiClient({
      listJobs: async () => ({ state: 'empty', message: 'No jobs matched the active filters.' }),
    });

    renderPage(client, '/jobs');

    expect(await screen.findByText('No jobs matched the active filters.')).toBeInTheDocument();
    expect(screen.getByText('Select a job to inspect retained detail.')).toBeInTheDocument();
  });
});
