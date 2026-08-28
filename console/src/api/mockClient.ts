import {
  decisions,
  experiments,
  getDecisionExplorer,
  getJobDetail,
  getRunList,
  jobs,
  overview,
  sandboxes,
  timeline,
  topology,
} from './mockFixture';
import {
  applySimulation,
  delay,
  filterJobs,
  getSelectedRunId,
  parseSimulation,
  selectLatestRun,
  sortRunsByServerOrder,
} from './helpers';
import type {
  ApiClient,
  ControlActionResult,
  DataKind,
  DecisionExplorerResult,
  DecisionRecord,
  ExperimentSummary,
  JobDetailResponse,
  JobCommand,
  JobSummary,
  OverviewResponse,
  QueryOptions,
  QueryResult,
  ReplayCommand,
  RunSummary,
  SandboxResponse,
  TimelineResponse,
  TopologySnapshot,
} from './types';

type MockSurface = 'jobs' | 'runs' | 'timeline' | 'sandboxes' | 'decisions' | 'experiments';

function filterDecisions(entries: DecisionRecord[], options?: QueryOptions): DecisionRecord[] {
  return entries.filter((entry) => {
    const jobId = options?.filters?.job_id ?? options?.filters?.jobId;
    const runId = getSelectedRunId(options?.filters);
    return (jobId ? entry.jobId === jobId : true) && (runId ? entry.runId === runId : true);
  });
}

function createMockPageToken(surface: MockSurface, scope: string, offset: number): string {
  return `mock:${surface}:${encodeURIComponent(scope)}:${offset}`;
}

function parseMockPageToken(surface: MockSurface, scope: string, pageToken?: string): QueryResult<number> {
  if (!pageToken) {
    return { state: 'ready', data: 0 };
  }
  const [prefix, tokenSurface, tokenScope, offsetValue] = pageToken.split(':');
  if (prefix !== 'mock' || tokenSurface !== surface || decodeURIComponent(tokenScope ?? '') !== scope) {
    return {
      state: 'error',
      message: `Invalid pagination token for ${surface}.`,
      retryable: false,
    };
  }
  const offset = Number(offsetValue);
  if (!Number.isFinite(offset) || offset < 0) {
    return {
      state: 'error',
      message: `Invalid pagination token for ${surface}.`,
      retryable: false,
    };
  }
  return { state: 'ready', data: offset };
}

function paginateMockEntries<T>(
  surface: MockSurface,
  scope: string,
  entries: T[],
  options?: QueryOptions,
): QueryResult<{ entries: T[]; nextPageToken?: string; totalApprox: number }> {
  const parsedToken = parseMockPageToken(surface, scope, options?.pageToken);
  if (parsedToken.state !== 'ready') {
    return {
      state: parsedToken.state,
      message: parsedToken.message,
      retryable: parsedToken.retryable,
      apiError: parsedToken.apiError,
    };
  }
  const limit = options?.limit ?? entries.length;
  const start = parsedToken.data ?? 0;
  const data = entries.slice(start, start + limit);
  const nextPageToken = start + limit < entries.length ? createMockPageToken(surface, scope, start + limit) : undefined;
  return {
    state: 'ready',
    data: {
      entries: data,
      nextPageToken,
      totalApprox: entries.length,
    },
  };
}

export class MockApiClient implements ApiClient {
  async listOverview(options?: QueryOptions): Promise<QueryResult<OverviewResponse>> {
    await delay(180, options?.signal);
    const mode = parseSimulation(options?.filters);
    return applySimulation(mode, overview, 'No overview metrics matched the selected source.');
  }

  async listJobs(options?: QueryOptions): Promise<QueryResult<JobSummary[]>> {
    await delay(180, options?.signal);
    const filtered = filterJobs(jobs, options);
    const paged = paginateMockEntries('jobs', 'jobs', filtered, options);
    if (paged.state !== 'ready') {
      return {
        state: paged.state,
        message: paged.message,
        retryable: paged.retryable,
        apiError: paged.apiError,
      };
    }
    const mode = parseSimulation(options?.filters);
    const result = applySimulation(mode, paged.data?.entries ?? [], 'No jobs matched the active filters.');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async getJobDetail(jobId: string, options?: QueryOptions): Promise<QueryResult<JobDetailResponse>> {
    await delay(180, options?.signal);
    const detail = getJobDetail(jobId);
    if (!detail) {
      return { state: 'empty', message: 'The requested job was not retained by the console dataset.' };
    }
    const jobRuns = sortRunsByServerOrder(getRunList(jobId));
    const requestedRunId = getSelectedRunId(options?.filters);
    const selectedRun = jobRuns.find((run) => run.id === requestedRunId) ?? selectLatestRun(jobRuns);
    const mode = parseSimulation(options?.filters);
    return applySimulation(
      mode,
      {
        ...detail,
        job: { ...detail.job, currentRunId: selectedRun?.id, runCount: jobRuns.length },
        runs: jobRuns,
        selectedRunId: selectedRun?.id,
      },
      'The selected job has no detail surface to render.',
    );
  }

  async listRuns(jobId: string, options?: QueryOptions): Promise<QueryResult<RunSummary[]>> {
    await delay(180, options?.signal);
    const data = sortRunsByServerOrder(getRunList(jobId));
    const paged = paginateMockEntries('runs', `job=${jobId}`, data, options);
    if (paged.state !== 'ready') {
      return {
        state: paged.state,
        message: paged.message,
        retryable: paged.retryable,
        apiError: paged.apiError,
      };
    }
    const mode = parseSimulation(options?.filters);
    const result = applySimulation(mode, paged.data?.entries ?? [], 'No runs matched the selected job.');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async listTimeline(jobId: string, options?: QueryOptions): Promise<QueryResult<TimelineResponse>> {
    await delay(180, options?.signal);
    const runId = getSelectedRunId(options?.filters);
    const filteredEvents = timeline.events.filter((event) =>
      (jobId ? event.jobId === jobId : true) && (runId ? event.runId === runId : true),
    );
    const paged = paginateMockEntries('timeline', `job=${jobId}|run=${runId ?? ''}`, filteredEvents, options);
    if (paged.state !== 'ready') {
      return {
        state: paged.state,
        message: paged.message,
        retryable: paged.retryable,
        apiError: paged.apiError,
      };
    }
    const mode = parseSimulation(options?.filters);
    const result = applySimulation(mode, { events: paged.data?.entries ?? [] }, 'No timeline events matched the filters.');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async getTopology(_jobId: string, options?: QueryOptions): Promise<QueryResult<TopologySnapshot>> {
    await delay(180, options?.signal);
    const runId = getSelectedRunId(options?.filters);
    if (runId && topology.runId !== runId) {
      return { state: 'empty', message: 'No topology matched the selected job and run filters.' };
    }
    const mode = parseSimulation(options?.filters);
    return applySimulation(mode, topology, 'Topology has no active nodes to display.');
  }

  async listSandboxes(jobId: string, options?: QueryOptions): Promise<QueryResult<SandboxResponse>> {
    await delay(180, options?.signal);
    const runId = getSelectedRunId(options?.filters);
    const filteredSandboxes = sandboxes.filter((entry) =>
      (jobId ? entry.jobId === jobId : true) && (runId ? entry.runId === runId : true),
    );
    const paged = paginateMockEntries('sandboxes', `job=${jobId}|run=${runId ?? ''}`, filteredSandboxes, options);
    if (paged.state !== 'ready') {
      return {
        state: paged.state,
        message: paged.message,
        retryable: paged.retryable,
        apiError: paged.apiError,
      };
    }
    const mode = parseSimulation(options?.filters);
    const result = applySimulation(mode, { sandboxes: paged.data?.entries ?? [] }, 'No sandboxes matched the active filters.');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async getDecisionExplorer(_jobId: string, decisionId: string, options?: QueryOptions): Promise<QueryResult<DecisionExplorerResult>> {
    await delay(180, options?.signal);
    const detail = getDecisionExplorer(decisionId);
    if (!detail) {
      return { state: 'empty', message: 'The requested decision was not retained.' };
    }
    const mode = parseSimulation(options?.filters);
    return applySimulation(mode, detail, 'No candidate set exists for the selected decision.');
  }

  async listDecisions(jobId: string, options?: QueryOptions): Promise<QueryResult<DecisionRecord[]>> {
    await delay(180, options?.signal);
    const filtered = filterDecisions(decisions.filter((entry) => entry.jobId === jobId), options);
    const paged = paginateMockEntries('decisions', `job=${jobId}|run=${getSelectedRunId(options?.filters) ?? ''}`, filtered, options);
    if (paged.state !== 'ready') {
      return {
        state: paged.state,
        message: paged.message,
        retryable: paged.retryable,
        apiError: paged.apiError,
      };
    }
    const mode = parseSimulation(options?.filters);
    const result = applySimulation(mode, paged.data?.entries ?? [], 'No decisions matched the active filters.');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async listExperiments(options?: QueryOptions): Promise<QueryResult<ExperimentSummary[]>> {
    await delay(180, options?.signal);
    const requestedKind = (options?.filters?.data_kind ?? options?.filters?.dataKind) as DataKind | undefined;
    const filtered = requestedKind
      ? experiments.filter((experiment) => experiment.runs.some((run) => run.dataKind === requestedKind))
      : experiments;
    const paged = paginateMockEntries('experiments', `kind=${requestedKind ?? ''}`, filtered, options);
    if (paged.state !== 'ready') {
      return {
        state: paged.state,
        message: paged.message,
        retryable: paged.retryable,
        apiError: paged.apiError,
      };
    }
    const mode = parseSimulation(options?.filters);
    const result = applySimulation(mode, paged.data?.entries ?? [], 'No experiments matched the active filters.');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async createJob(job: Record<string, unknown>): Promise<QueryResult<ControlActionResult>> {
    await delay();
    return {
      state: 'ready',
      data: {
        status: 'accepted',
        id: String(job.jobId ?? 'mock-job'),
        message: 'Mock mode: job creation is a console-only demonstration.',
        job,
      },
    };
  }

  async createRun(jobId: string): Promise<QueryResult<ControlActionResult>> {
    await delay();
    return {
      state: 'ready',
      data: {
        status: 'accepted',
        id: `${jobId}-run-mock`,
        message: 'Mock mode: create run is a console-only demonstration.',
      },
    };
  }

  async admitJob(jobId: string): Promise<QueryResult<ControlActionResult>> {
    await delay();
    return {
      state: 'ready',
      data: {
        status: 'accepted',
        id: jobId,
        message: `Mock mode: admit job for ${jobId} is a console-only demonstration.`,
      },
    };
  }

  async applyJobCommand(jobId: string, runId: string, command: JobCommand): Promise<QueryResult<ControlActionResult>> {
    await delay();
    return {
      state: 'ready',
      data: {
        status: 'accepted',
        id: runId,
        message: `Mock mode: job command ${command} for ${jobId} is a console-only demonstration.`,
      },
    };
  }

  async createReplay(replay: Record<string, unknown>): Promise<QueryResult<ControlActionResult>> {
    await delay();
    return {
      state: 'ready',
      data: {
        status: 'accepted',
        id: String(replay.replayId ?? 'mock-replay'),
        message: 'Mock mode: replay creation is a console-only demonstration.',
        replay,
      },
    };
  }

  async applyReplayCommand(replayId: string, command: ReplayCommand): Promise<QueryResult<ControlActionResult>> {
    await delay();
    return {
      state: 'ready',
      data: {
        status: 'accepted',
        id: replayId,
        message: `Mock mode: replay command ${command} is a console-only demonstration.`,
      },
    };
  }
}
