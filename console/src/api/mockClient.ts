import {
  decisions,
  experiments,
  getDecisionExplorer,
  getJobDetail,
  getRunList,
  jobs,
  overview,
  resources,
  sandboxes,
  timeline,
  traceEvents,
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
  ResourceSnapshot,
  QueryOptions,
  QueryResult,
  ReplayCommand,
  RunSummary,
  SandboxResponse,
  TimelineResponse,
  TraceResponse,
  TopologySnapshot,
} from './types';

type MockSurface = 'jobs' | 'runs' | 'timeline' | 'traces' | 'sandboxes' | 'decisions' | 'experiments';

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
    return applySimulation(mode, overview, '当前数据来源下没有总览指标。');
  }

  async getResources(options?: QueryOptions): Promise<QueryResult<ResourceSnapshot>> {
    await delay(180, options?.signal);
    return applySimulation(parseSimulation(options?.filters), resources, '当前没有发现可调度的算力设备。');
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
    const result = applySimulation(mode, paged.data?.entries ?? [], '当前筛选条件下没有任务。');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async getJobDetail(jobId: string, options?: QueryOptions): Promise<QueryResult<JobDetailResponse>> {
    await delay(180, options?.signal);
    const detail = getJobDetail(jobId);
    if (!detail) {
      return { state: 'empty', message: '控制台数据中没有保留该任务。' };
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
      '当前任务没有可展示的详情。',
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
    const result = applySimulation(mode, paged.data?.entries ?? [], '当前任务没有匹配的运行记录。');
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
    const result = applySimulation(mode, { events: paged.data?.entries ?? [] }, '当前条件下没有控制事件。');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async listTraces(jobId: string, options?: QueryOptions): Promise<QueryResult<TraceResponse>> {
    await delay(180, options?.signal);
    const runId = getSelectedRunId(options?.filters);
    const traceId = options?.filters?.trace_id;
    const requestedKind = options?.filters?.data_kind as DataKind | undefined;
    const filteredTraceEvents: TraceResponse['events'] = traceEvents.filter((event) =>
      event.jobId === jobId &&
      (!runId || event.runId === runId) &&
      (!traceId || event.traceId === traceId) &&
      (!requestedKind || event.dataKind === requestedKind),
    );
    const paged = paginateMockEntries('traces', `job=${jobId}|run=${runId ?? ''}|trace=${traceId ?? ''}`, filteredTraceEvents, options);
    if (paged.state !== 'ready') {
      return { state: paged.state, message: paged.message, retryable: paged.retryable, apiError: paged.apiError };
    }
    const mode = parseSimulation(options?.filters);
    const result = applySimulation(mode, { events: paged.data?.entries ?? [] }, '没有找到符合条件的链路事件。');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async getTopology(_jobId: string, options?: QueryOptions): Promise<QueryResult<TopologySnapshot>> {
    await delay(180, options?.signal);
    const runId = getSelectedRunId(options?.filters);
    if (runId && topology.runId !== runId) {
      return { state: 'empty', message: '当前任务与运行没有匹配的拓扑。' };
    }
    const mode = parseSimulation(options?.filters);
    return applySimulation(mode, topology, '当前拓扑没有可展示的活动节点。');
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
    const result = applySimulation(mode, { sandboxes: paged.data?.entries ?? [] }, '当前筛选条件下没有沙箱。');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async getDecisionExplorer(_jobId: string, decisionId: string, options?: QueryOptions): Promise<QueryResult<DecisionExplorerResult>> {
    await delay(180, options?.signal);
    const detail = getDecisionExplorer(decisionId);
    if (!detail) {
      return { state: 'empty', message: '系统没有保留该决策。' };
    }
    const mode = parseSimulation(options?.filters);
    return applySimulation(mode, detail, '当前决策没有候选集合。');
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
    const result = applySimulation(mode, paged.data?.entries ?? [], '当前筛选条件下没有调度决策。');
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
    const result = applySimulation(mode, paged.data?.entries ?? [], '当前筛选条件下没有实验。');
    return { ...result, pageInfo: { nextPageToken: paged.data?.nextPageToken, totalApprox: paged.data?.totalApprox } };
  }

  async createJob(job: Record<string, unknown>): Promise<QueryResult<ControlActionResult>> {
    await delay();
    return {
      state: 'ready',
      data: {
        status: 'accepted',
        id: String(job.jobId ?? 'mock-job'),
        message: '模拟模式：任务创建仅用于控制台演示。',
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
        message: '模拟模式：运行创建仅用于控制台演示。',
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
        message: `模拟模式：任务 ${jobId} 的准入仅用于控制台演示。`,
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
        message: `模拟模式：任务 ${jobId} 的 ${command} 命令仅用于控制台演示。`,
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
        message: '模拟模式：回放创建仅用于控制台演示。',
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
        message: `模拟模式：回放命令 ${command} 仅用于控制台演示。`,
      },
    };
  }
}
