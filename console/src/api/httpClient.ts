import { asResult, createControlResult, ensureArray, ensureObject, filterExperimentsByDataKind, getSelectedRunId, selectLatestRun, sortRunsByServerOrder, toMetricToneByHealth } from './helpers';
import {
  buildOverview,
  mapDecision,
  mapDecisionExplorer,
  mapExperiment,
  mapJob,
  mapRun,
  mapSandbox,
  mapTimelineEvent,
  mapTopology,
} from './mappers';
import { GatewayTransport } from './transport';
import type {
  ApiClient,
  ControlActionResult,
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

const RUNS_PAGE_SIZE = 100;

export class HttpApiClient implements ApiClient {
  private readonly transport: GatewayTransport;

  constructor(baseUrl = '') {
    this.transport = new GatewayTransport(baseUrl);
  }

  private async listRunsPage(jobId: string, options?: QueryOptions): Promise<QueryResult<RunSummary[]>> {
    const result = await this.transport.get<{ runs?: unknown[]; nextPageToken?: string }>(`/v1/jobs/${jobId}/runs`, options);
    if (result.state !== 'ready') {
      return asResult<RunSummary[]>(result);
    }
    const runs = ensureArray<Record<string, unknown>>(result.data?.runs).map(mapRun);
    return {
      state: 'ready',
      data: sortRunsByServerOrder(runs),
      pageInfo: result.pageInfo,
    };
  }

  private async listAllRuns(jobId: string, signal?: AbortSignal): Promise<QueryResult<RunSummary[]>> {
    const allRuns: RunSummary[] = [];
    let nextPageToken: string | undefined;

    do {
      const page = await this.listRunsPage(jobId, {
        pageToken: nextPageToken,
        limit: RUNS_PAGE_SIZE,
        signal,
      });
      if (page.state !== 'ready') {
        return asResult<RunSummary[]>(page);
      }
      allRuns.push(...(page.data ?? []));
      nextPageToken = page.pageInfo?.nextPageToken;
    } while (nextPageToken);

    return {
      state: 'ready',
      data: sortRunsByServerOrder(allRuns),
    };
  }

  async listOverview(options?: QueryOptions): Promise<QueryResult<OverviewResponse>> {
    const [healthResult, capabilitiesResult, jobsResult, experimentsResult] = await Promise.all([
      this.transport.get<Record<string, unknown>>('/health', options, { includePagination: false }),
      this.transport.get<Record<string, unknown>>('/v1/capabilities', options, { includePagination: false }),
      this.transport.get<{ jobs?: unknown[]; nextPageToken?: string }>('/v1/jobs', options, {
        allowedFilters: ['data_kind'],
      }),
      this.transport.get<{ experiments?: unknown[]; nextPageToken?: string }>('/v1/experiments', {
        limit: options?.limit,
        signal: options?.signal,
      }),
    ]);

    if (healthResult.state !== 'ready') {
      return asResult<OverviewResponse>(healthResult);
    }
    if (capabilitiesResult.state !== 'ready') {
      return asResult<OverviewResponse>(capabilitiesResult);
    }
    if (jobsResult.state !== 'ready') {
      return asResult<OverviewResponse>(jobsResult);
    }
    if (experimentsResult.state !== 'ready') {
      return asResult<OverviewResponse>(experimentsResult);
    }

    const runsByJob = new Map<string, RunSummary>();
    await Promise.all(
      ensureArray<Record<string, unknown>>(jobsResult.data?.jobs).map(async (job) => {
        const jobId = String(ensureObject(job).jobId ?? '');
        if (!jobId) {
          return;
        }
        const runsResult = await this.listAllRuns(jobId, options?.signal);
        const latestRun = runsResult.state === 'ready' ? selectLatestRun(runsResult.data ?? []) : undefined;
        if (latestRun) {
          runsByJob.set(jobId, latestRun);
        }
      }),
    );

    const mappedJobs = ensureArray<Record<string, unknown>>(jobsResult.data?.jobs).map((job) =>
      mapJob(job, runsByJob.get(String(ensureObject(job).jobId ?? ''))),
    );
    const mappedExperiments = filterExperimentsByDataKind(
      ensureArray<Record<string, unknown>>(experimentsResult.data?.experiments).map(mapExperiment),
      options?.filters?.data_kind as JobSummary['dataKind'] | undefined,
    );

    return {
      state: 'ready',
      data: buildOverview(
        mappedJobs,
        mappedExperiments,
        ensureObject(healthResult.data),
        ensureObject(capabilitiesResult.data),
      ),
      pageInfo: { nextPageToken: jobsResult.pageInfo?.nextPageToken },
    };
  }

  async listJobs(options?: QueryOptions): Promise<QueryResult<JobSummary[]>> {
    const result = await this.transport.get<{ jobs?: unknown[]; nextPageToken?: string }>('/v1/jobs', options, {
      allowedFilters: ['data_kind'],
    });
    if (result.state !== 'ready') {
      return asResult<JobSummary[]>(result);
    }
    const runsByJob = new Map<string, RunSummary>();
    await Promise.all(
      ensureArray<Record<string, unknown>>(result.data?.jobs).map(async (job) => {
        const jobId = String(ensureObject(job).jobId ?? '');
        if (!jobId) {
          return;
        }
        const runsResult = await this.listAllRuns(jobId, options?.signal);
        const latestRun = runsResult.state === 'ready' ? selectLatestRun(runsResult.data ?? []) : undefined;
        if (latestRun) {
          runsByJob.set(jobId, latestRun);
        }
      }),
    );
    return {
      state: 'ready',
      data: ensureArray<Record<string, unknown>>(result.data?.jobs).map((job) =>
        mapJob(job, runsByJob.get(String(ensureObject(job).jobId ?? ''))),
      ),
      pageInfo: result.pageInfo,
    };
  }

  async getJobDetail(jobId: string, options?: QueryOptions): Promise<QueryResult<JobDetailResponse>> {
    const [jobResult, allRunsResult] = await Promise.all([
      this.transport.get<{ job?: unknown }>(`/v1/jobs/${jobId}`, options, { includePagination: false }),
      this.listAllRuns(jobId, options?.signal),
    ]);
    if (jobResult.state !== 'ready') {
      return asResult<JobDetailResponse>(jobResult);
    }
    if (allRunsResult.state !== 'ready') {
      return asResult<JobDetailResponse>(allRunsResult);
    }

    const allRuns = sortRunsByServerOrder(
      (allRunsResult.data ?? []).filter((run) => run.jobId === jobId),
    );
    const requestedRunId = getSelectedRunId(options?.filters);
    const selectedRun = allRuns.find((run) => run.id === requestedRunId) ?? selectLatestRun(allRuns);
    const selectedRunId = selectedRun?.id ?? '';
    const detailFilters = Object.fromEntries(
      Object.entries(options?.filters ?? {}).filter(([key]) => key !== 'run_id' && key !== 'runId'),
    );
    const runScopedFilters = selectedRunId ? { ...detailFilters, run_id: selectedRunId } : detailFilters;
    const [decisionsResult, sandboxesResult] = await Promise.all([
      this.listDecisions(jobId, {
        limit: 20,
        filters: runScopedFilters,
        signal: options?.signal,
      }),
      this.listSandboxes(jobId, {
        filters: runScopedFilters,
        signal: options?.signal,
      }),
    ]);
    if (decisionsResult.state !== 'ready') {
      return asResult<JobDetailResponse>(decisionsResult);
    }
    if (sandboxesResult.state !== 'ready') {
      return asResult<JobDetailResponse>(sandboxesResult);
    }

    const mappedJob = mapJob(jobResult.data?.job, selectedRun);
    mappedJob.runCount = allRuns.length;
    return {
      state: 'ready',
      data: {
        job: mappedJob,
        runs: allRuns,
        selectedRunId,
        decisions: decisionsResult.data ?? [],
        sandboxes: sandboxesResult.data?.sandboxes ?? [],
        metrics: [
          { label: 'Runs', value: String(allRuns.length) },
          { label: 'Current run', value: selectedRunId || 'none', tone: selectedRun ? toMetricToneByHealth(mappedJob.health) : 'warn' },
          { label: 'Health', value: mappedJob.health.toUpperCase(), tone: toMetricToneByHealth(mappedJob.health) },
        ],
      },
    };
  }

  async listRuns(jobId: string, options?: QueryOptions): Promise<QueryResult<RunSummary[]>> {
    return this.listRunsPage(jobId, options);
  }

  async listTimeline(jobId: string, options?: QueryOptions): Promise<QueryResult<TimelineResponse>> {
    const result = await this.transport.get<{ events?: unknown[]; nextPageToken?: string }>(`/v1/jobs/${jobId}/timeline`, options, {
      allowedFilters: ['run_id', 'after_event_id'],
    });
    if (result.state !== 'ready') {
      return asResult<TimelineResponse>(result);
    }
    return {
      state: 'ready',
      data: {
        events: ensureArray<Record<string, unknown>>(result.data?.events).map((event) => mapTimelineEvent(event, jobId)),
      },
      pageInfo: result.pageInfo,
    };
  }

  async getTopology(jobId: string, options?: QueryOptions): Promise<QueryResult<TopologySnapshot>> {
    const result = await this.transport.get<Record<string, unknown>>(`/v1/jobs/${jobId}/topology`, options, {
      includePagination: false,
      allowedFilters: ['run_id'],
    });
    if (result.state !== 'ready') {
      return asResult<TopologySnapshot>(result);
    }
    return { state: 'ready', data: mapTopology(result.data, jobId) };
  }

  async listSandboxes(jobId: string, options?: QueryOptions): Promise<QueryResult<SandboxResponse>> {
    const result = await this.transport.get<{ sandboxes?: unknown[]; nextPageToken?: string }>(`/v1/jobs/${jobId}/sandboxes`, options, {
      allowedFilters: ['run_id'],
    });
    if (result.state !== 'ready') {
      return asResult<SandboxResponse>(result);
    }
    return {
      state: 'ready',
      data: {
        sandboxes: ensureArray<Record<string, unknown>>(result.data?.sandboxes).map((sandbox) => mapSandbox(sandbox, jobId)),
      },
      pageInfo: result.pageInfo,
    };
  }

  async getDecisionExplorer(jobId: string, decisionId: string, options?: QueryOptions): Promise<QueryResult<DecisionExplorerResult>> {
    const result = await this.transport.get<{ decision?: unknown }>(`/v1/jobs/${jobId}/decisions/${decisionId}`, options, {
      includePagination: false,
    });
    if (result.state !== 'ready') {
      return asResult<DecisionExplorerResult>(result);
    }
    return {
      state: 'ready',
      data: mapDecisionExplorer(result.data?.decision, jobId),
    };
  }

  async listDecisions(jobId: string, options?: QueryOptions): Promise<QueryResult<DecisionRecord[]>> {
    const result = await this.transport.get<{ decisions?: unknown[]; nextPageToken?: string }>(`/v1/jobs/${jobId}/decisions`, options, {
      allowedFilters: ['run_id'],
    });
    if (result.state !== 'ready') {
      return asResult<DecisionRecord[]>(result);
    }
    return {
      state: 'ready',
      data: ensureArray<Record<string, unknown>>(result.data?.decisions).map((decision) => {
        const mapped = mapDecision(decision);
        mapped.jobId = jobId;
        return mapped;
      }),
      pageInfo: result.pageInfo,
    };
  }

  async listExperiments(options?: QueryOptions): Promise<QueryResult<ExperimentSummary[]>> {
    const result = await this.transport.get<{ experiments?: unknown[]; nextPageToken?: string }>('/v1/experiments', {
      pageToken: options?.pageToken,
      limit: options?.limit,
      signal: options?.signal,
    });
    if (result.state !== 'ready') {
      return asResult<ExperimentSummary[]>(result);
    }
    const mappedExperiments = ensureArray<Record<string, unknown>>(result.data?.experiments).map(mapExperiment);
    return {
      state: 'ready',
      data: filterExperimentsByDataKind(
        mappedExperiments,
        options?.filters?.data_kind as JobSummary['dataKind'] | undefined,
      ),
      pageInfo: result.pageInfo,
    };
  }

  async createJob(job: Record<string, unknown>): Promise<QueryResult<ControlActionResult>> {
    const result = await this.transport.post<Record<string, unknown>>('/v1/jobs', job);
    if (result.state !== 'ready') {
      return asResult<ControlActionResult>(result);
    }
    return { state: 'ready', data: createControlResult(result.data ?? {}, 'Job created.') };
  }

  async createRun(jobId: string): Promise<QueryResult<ControlActionResult>> {
    const result = await this.transport.post<Record<string, unknown>>(`/v1/jobs/${jobId}/runs`);
    if (result.state !== 'ready') {
      return asResult<ControlActionResult>(result);
    }
    return { state: 'ready', data: createControlResult(result.data ?? {}, 'Run created.', jobId) };
  }

  async admitJob(
    jobId: string,
    payload: Record<string, unknown> = {},
  ): Promise<QueryResult<ControlActionResult>> {
    const result = await this.transport.post<Record<string, unknown>>(
      `/v1/jobs/${jobId}/admit`,
      {
        actor: 'console',
        reason: 'console:admit',
        ...payload,
      },
    );
    if (result.state !== 'ready') {
      return asResult<ControlActionResult>(result);
    }
    return { state: 'ready', data: createControlResult(result.data ?? {}, 'Job admitted.', jobId) };
  }

  async applyJobCommand(
    jobId: string,
    runId: string,
    command: JobCommand,
    payload: Record<string, unknown> = {},
  ): Promise<QueryResult<ControlActionResult>> {
    const result = await this.transport.post<Record<string, unknown>>(
      `/v1/jobs/${jobId}/runs/${runId}/commands/${command}`,
      {
        actor: 'console',
        reason: `console:${command}`,
        ...payload,
      },
    );
    if (result.state !== 'ready') {
      return asResult<ControlActionResult>(result);
    }
    return { state: 'ready', data: createControlResult(result.data ?? {}, `Job command ${command} applied.`, runId) };
  }

  async createReplay(replay: Record<string, unknown>): Promise<QueryResult<ControlActionResult>> {
    const result = await this.transport.post<Record<string, unknown>>('/v1/replays', replay);
    if (result.state !== 'ready') {
      return asResult<ControlActionResult>(result);
    }
    return { state: 'ready', data: createControlResult(result.data ?? {}, 'Replay created.') };
  }

  async applyReplayCommand(replayId: string, command: ReplayCommand): Promise<QueryResult<ControlActionResult>> {
    const result = await this.transport.post<Record<string, unknown>>(`/v1/replays/${replayId}/commands/${command}`);
    if (result.state !== 'ready') {
      return asResult<ControlActionResult>(result);
    }
    return { state: 'ready', data: createControlResult(result.data ?? {}, `Replay command ${command} applied.`, replayId) };
  }
}
