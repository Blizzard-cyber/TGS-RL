import type { ApiError } from './contracts';

export type DataKind = 'synthetic' | 'replay' | 'live';
export type LoadStateKind =
  | 'loading'
  | 'ready'
  | 'empty'
  | 'error'
  | 'forbidden'
  | 'gpu-unavailable'
  | 'degraded';

export interface QueryOptions {
  pageToken?: string;
  limit?: number;
  filters?: Record<string, string>;
  signal?: AbortSignal;
}

export interface PageInfo {
  nextPageToken?: string;
  totalApprox?: number;
}

export interface MetricCard {
  label: string;
  value: string;
  delta?: string;
  tone?: 'neutral' | 'good' | 'warn' | 'critical';
}

export interface JobSummary {
  id: string;
  name: string;
  algorithm: string;
  state: 'pending' | 'running' | 'paused' | 'succeeded' | 'failed' | 'cancelled';
  rolloutMode: 'sync' | 'partially_async' | 'fully_async';
  dataKind: DataKind;
  queue: string;
  priority: number;
  desiredUnits: number;
  activeUnits: number;
  gpuRequired: boolean;
  createdAt: string;
  updatedAt: string;
  owner: string;
  policyVersion: string;
  traceId: string;
  executionId: string;
  currentRunId?: string;
  latestRunState?: string;
  runCount?: number;
  health: 'healthy' | 'degraded' | 'stalled';
}

export interface RunSummary {
  id: string;
  jobId: string;
  traceId: string;
  state: string;
  attempt: number;
  policyVersion: string;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  dataKind: DataKind;
  componentHealth: Array<{
    component: string;
    health: 'healthy' | 'degraded' | 'failed' | 'progressing';
    detail: string;
  }>;
}

export interface DecisionAction {
  actionId: string;
  type: 'bind' | 'pause' | 'resume' | 'resize' | 'offload' | 'checkpoint';
  sandboxId: string;
  targetId: string;
  status: 'succeeded' | 'failed' | 'skipped' | 'rolled_back' | 'unknown';
  detail: string;
}

export interface DecisionRecord {
  id: string;
  sequence: number;
  jobId: string;
  runId?: string;
  traceId: string;
  stageId: string;
  decidedAt: string;
  fallback: boolean;
  fallbackReason?: string;
  selectedPlanId?: string;
  selectedCandidate?: string;
  policyVersion: string;
  summary: string;
  actions: DecisionAction[];
}

export interface TopologyNode {
  id: string;
  label: string;
  kind: 'job' | 'queue' | 'sandbox' | 'device' | 'runtime';
  status: 'ready' | 'busy' | 'degraded' | 'down';
  gpu: boolean;
  share?: number;
  utilization?: number;
}

export interface TopologyEdge {
  from: string;
  to: string;
  relation: 'scheduled-on' | 'feeds' | 'blocked-by' | 'runs-in';
}

export interface TopologySnapshot {
  runId?: string;
  manifestId?: string;
  nodes: TopologyNode[];
  edges: TopologyEdge[];
  lastUpdated: string;
}

export interface TimelineEvent {
  id: string;
  jobId: string;
  runId?: string;
  title: string;
  type:
    | 'phase-started'
    | 'phase-completed'
    | 'sample-produced'
    | 'sample-consumed'
    | 'policy-published'
    | 'backpressure'
    | 'safe-point'
    | 'decision-applied';
  occurredAt: string;
  sequence: number;
  phase: string;
  sandboxId?: string;
  decisionId?: string;
  dataKind: DataKind;
  severity: 'info' | 'warn' | 'critical';
  summary: string;
}

export interface SandboxRecord {
  id: string;
  jobId: string;
  runId?: string;
  state: 'requested' | 'bound' | 'running' | 'paused' | 'sleeping' | 'terminated' | 'failed' | 'unknown';
  generation: number;
  nodeLabel: string;
  gpuAttached: boolean;
  share: number;
  priority: number;
  safePoint: boolean;
  offloaded: boolean;
  updatedAt: string;
  bindingSummary: string;
}

export interface ExperimentRun {
  id: string;
  experimentId: string;
  runId: string;
  label: string;
  kind: 'simulation' | 'replay' | 'live';
  dataKind: DataKind;
  policyVersion: string;
  configHash: string;
  codeRevision: string;
  summary: string;
  metrics: MetricCard[];
}

export interface ExperimentSummary {
  id: string;
  name: string;
  state: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  createdAt: string;
  completedAt?: string;
  summary: string;
  runs: ExperimentRun[];
}

export interface DecisionExplorerResult {
  selectedDecision?: DecisionRecord;
  candidates: Array<{
    id: string;
    deviceLabel: string;
    score: number;
    reason: string;
    selected: boolean;
  }>;
  rejectedCandidates: Array<{
    id: string;
    reason: string;
    detail: string;
  }>;
  relatedActions: DecisionAction[];
}

export interface StatusEnvelope<T> {
  state: Exclude<LoadStateKind, 'loading'>;
  data?: T;
  message?: string;
  retryable?: boolean;
  apiError?: ApiError;
}

export interface QueryResult<T> {
  state: LoadStateKind;
  data?: T;
  pageInfo?: PageInfo;
  message?: string;
  retryable?: boolean;
  apiError?: ApiError;
}

export type JobCommand = 'start' | 'pause' | 'resume' | 'stop' | 'retry' | 'terminate';
export type ReplayCommand = 'start' | 'pause' | 'resume' | 'stop' | 'terminate';

export interface ControlActionResult {
  requestId?: string;
  id?: string;
  status: 'accepted' | 'succeeded' | 'failed' | 'cancelled';
  message: string;
  operationId?: string;
  job?: Record<string, unknown>;
  run?: Record<string, unknown>;
  replay?: Record<string, unknown>;
}

export interface OverviewResponse {
  metrics: MetricCard[];
  jobs: JobSummary[];
  experiments: ExperimentSummary[];
  decisions: DecisionRecord[];
  capabilities: {
    protocolVersion: string;
    dataKinds: string[];
    pagination: string;
  };
  systemHealth: {
    status: string;
    observedAt: string;
    counts: Record<string, number>;
  };
  alerts: Array<{
    id: string;
    title: string;
    tone: 'warn' | 'critical' | 'info';
    detail: string;
  }>;
}

export interface JobDetailResponse {
  job: JobSummary;
  runs: RunSummary[];
  selectedRunId?: string;
  decisions: DecisionRecord[];
  sandboxes: SandboxRecord[];
  metrics: MetricCard[];
}

export interface TimelineResponse {
  events: TimelineEvent[];
}

export interface SandboxResponse {
  sandboxes: SandboxRecord[];
}

export interface ApiClient {
  listOverview(options?: QueryOptions): Promise<QueryResult<OverviewResponse>>;
  listJobs(options?: QueryOptions): Promise<QueryResult<JobSummary[]>>;
  getJobDetail(jobId: string, options?: QueryOptions): Promise<QueryResult<JobDetailResponse>>;
  listRuns(jobId: string, options?: QueryOptions): Promise<QueryResult<RunSummary[]>>;
  listTimeline(jobId: string, options?: QueryOptions): Promise<QueryResult<TimelineResponse>>;
  getTopology(jobId: string, options?: QueryOptions): Promise<QueryResult<TopologySnapshot>>;
  listSandboxes(jobId: string, options?: QueryOptions): Promise<QueryResult<SandboxResponse>>;
  getDecisionExplorer(jobId: string, decisionId: string, options?: QueryOptions): Promise<QueryResult<DecisionExplorerResult>>;
  listDecisions(jobId: string, options?: QueryOptions): Promise<QueryResult<DecisionRecord[]>>;
  listExperiments(options?: QueryOptions): Promise<QueryResult<ExperimentSummary[]>>;
  createJob(job: Record<string, unknown>): Promise<QueryResult<ControlActionResult>>;
  createRun(jobId: string): Promise<QueryResult<ControlActionResult>>;
  admitJob(jobId: string, payload?: Record<string, unknown>): Promise<QueryResult<ControlActionResult>>;
  applyJobCommand(jobId: string, runId: string, command: JobCommand, payload?: Record<string, unknown>): Promise<QueryResult<ControlActionResult>>;
  createReplay(replay: Record<string, unknown>): Promise<QueryResult<ControlActionResult>>;
  applyReplayCommand(replayId: string, command: ReplayCommand): Promise<QueryResult<ControlActionResult>>;
}
