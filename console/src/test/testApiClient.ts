import type {
  ApiClient,
  ControlActionResult,
  DecisionExplorerResult,
  DecisionRecord,
  ExperimentSummary,
  JobDetailResponse,
  JobSummary,
  OverviewResponse,
  ResourceSnapshot,
  QueryResult,
  RunSummary,
  SandboxResponse,
  TimelineResponse,
  TraceResponse,
  TopologySnapshot,
} from '../api/types';

export function ready<T>(data: T): QueryResult<T> {
  return { state: 'ready', data };
}

export function createTestApiClient(overrides: Partial<ApiClient> = {}): ApiClient {
  return {
    listOverview: async () =>
      ready<OverviewResponse>({
        metrics: [],
        jobs: [],
        experiments: [],
        decisions: [],
        capabilities: { protocolVersion: 'v0.3', dataKinds: [], pagination: 'opaque' },
        systemHealth: { status: 'ok', observedAt: '', counts: {} },
        alerts: [],
      }),
    getResources: async () => ready<ResourceSnapshot>({ id: '', revision: 0, observedAt: '', devices: [], allocations: [], pendingUnits: 0 }),
    listJobs: async () => ready<JobSummary[]>([]),
    getJobDetail: async () =>
      ready<JobDetailResponse>({
        job: {
          id: 'job-default',
          name: 'Default Job',
          algorithm: 'PPO',
          state: 'running',
          rolloutMode: 'sync',
          dataKind: 'live',
          queue: 'default',
          priority: 1,
          desiredUnits: 1,
          gpuRequired: false,
          createdAt: '2026-08-27T00:00:00Z',
          updatedAt: '2026-08-27T00:00:00Z',
          owner: 'console',
          policyVersion: 'policy-default',
          traceId: 'trace-default',
          executionId: 'exec-default',
          currentRunId: 'run-default',
          latestRunState: 'JOB_RUN_STATE_RUNNING',
          runCount: 1,
          health: 'healthy',
        },
        runs: [],
        selectedRunId: undefined,
        decisions: [],
        sandboxes: [],
        metrics: [],
      }),
    listRuns: async () => ready<RunSummary[]>([]),
    listTimeline: async () => ready<TimelineResponse>({ events: [] }),
    listTraces: async () => ready<TraceResponse>({ events: [] }),
    getTopology: async () => ready<TopologySnapshot>({ nodes: [], edges: [], lastUpdated: '' }),
    listSandboxes: async () => ready<SandboxResponse>({ sandboxes: [] }),
    getDecisionExplorer: async () => ready<DecisionExplorerResult>({ candidates: [], rejectedCandidates: [], relatedActions: [] }),
    listDecisions: async () => ready<DecisionRecord[]>([]),
    listExperiments: async () => ready<ExperimentSummary[]>([]),
    createJob: async () => ready<ControlActionResult>({ status: 'accepted', message: 'created' }),
    createRun: async () => ready<ControlActionResult>({ status: 'accepted', message: 'created run' }),
    admitJob: async () => ready<ControlActionResult>({ status: 'accepted', message: 'job admitted' }),
    applyJobCommand: async () => ready<ControlActionResult>({ status: 'accepted', message: 'job command' }),
    createReplay: async () => ready<ControlActionResult>({ status: 'accepted', message: 'replay created' }),
    applyReplayCommand: async () => ready<ControlActionResult>({ status: 'accepted', message: 'replay command' }),
    ...overrides,
  };
}
