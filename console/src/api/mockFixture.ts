import type {
  DataKind,
  DecisionExplorerResult,
  DecisionRecord,
  ExperimentSummary,
  JobDetailResponse,
  JobSummary,
  MetricCard,
  OverviewResponse,
  RunSummary,
  SandboxRecord,
  TimelineEvent,
  TimelineResponse,
  TopologySnapshot,
} from './types';

const now = '2026-08-27T14:32:00Z';

function metric(label: string, value: string, delta?: string, tone?: MetricCard['tone']): MetricCard {
  return { label, value, delta, tone };
}

function kindSuffix(dataKind: DataKind): string {
  return dataKind.toUpperCase();
}

export const jobs: JobSummary[] = [
  {
    id: 'job-live-017',
    name: 'PPO Actor-Critic Burst',
    algorithm: 'PPO',
    state: 'running',
    rolloutMode: 'partially_async',
    dataKind: 'live',
    queue: 'priority-train',
    priority: 95,
    desiredUnits: 16,
    activeUnits: 14,
    gpuRequired: true,
    createdAt: '2026-08-27T08:15:00Z',
    updatedAt: now,
    owner: 'ops-shift-a',
    policyVersion: 'policy-2026.08.27.5',
    traceId: 'trace-live-017',
    executionId: 'exec-live-017',
    currentRunId: 'run-live-017-a',
    latestRunState: 'JOB_RUN_STATE_RUNNING',
    runCount: 1,
    health: 'degraded',
  },
  {
    id: 'job-replay-204',
    name: 'Replay Latency Audit',
    algorithm: 'GRPO',
    state: 'running',
    rolloutMode: 'sync',
    dataKind: 'replay',
    queue: 'audit',
    priority: 62,
    desiredUnits: 6,
    activeUnits: 6,
    gpuRequired: false,
    createdAt: '2026-08-27T07:00:00Z',
    updatedAt: now,
    owner: 'analysis-bot',
    policyVersion: 'replay-2026.08.27.2',
    traceId: 'trace-replay-204',
    executionId: 'exec-replay-204',
    currentRunId: 'run-replay-204-a',
    latestRunState: 'JOB_RUN_STATE_RUNNING',
    runCount: 1,
    health: 'healthy',
  },
  {
    id: 'job-sim-031',
    name: 'Synthetic Capacity Soak',
    algorithm: 'A3C',
    state: 'paused',
    rolloutMode: 'fully_async',
    dataKind: 'synthetic',
    queue: 'simulation',
    priority: 40,
    desiredUnits: 10,
    activeUnits: 4,
    gpuRequired: true,
    createdAt: '2026-08-26T23:45:00Z',
    updatedAt: now,
    owner: 'capacity-lab',
    policyVersion: 'sim-2026.08.26.9',
    traceId: 'trace-sim-031',
    executionId: 'exec-sim-031',
    currentRunId: 'run-sim-031-a',
    latestRunState: 'JOB_RUN_STATE_PAUSED',
    runCount: 1,
    health: 'stalled',
  },
];

export const runs: RunSummary[] = [
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
    componentHealth: [
      { component: 'scheduler', health: 'degraded', detail: 'GPU contention on actor pool' },
      { component: 'runtime', health: 'healthy', detail: 'Runtimes converged' },
    ],
  },
  {
    id: 'run-replay-204-a',
    jobId: 'job-replay-204',
    traceId: 'trace-replay-204',
    state: 'JOB_RUN_STATE_RUNNING',
    attempt: 2,
    policyVersion: 'replay-2026.08.27.2',
    createdAt: '2026-08-27T07:02:00Z',
    startedAt: '2026-08-27T07:03:00Z',
    dataKind: 'replay',
    componentHealth: [
      { component: 'scheduler', health: 'healthy', detail: 'Stable CPU placement' },
      { component: 'runtime', health: 'healthy', detail: 'Replay stream active' },
    ],
  },
  {
    id: 'run-sim-031-a',
    jobId: 'job-sim-031',
    traceId: 'trace-sim-031',
    state: 'JOB_RUN_STATE_PAUSED',
    attempt: 1,
    policyVersion: 'sim-2026.08.26.9',
    createdAt: '2026-08-26T23:47:00Z',
    startedAt: '2026-08-26T23:49:00Z',
    dataKind: 'synthetic',
    componentHealth: [
      { component: 'scheduler', health: 'failed', detail: 'No READY GPU capacity' },
      { component: 'runtime', health: 'progressing', detail: 'Paused at safe point' },
    ],
  },
];

export const decisions: DecisionRecord[] = [
  {
    id: 'dec-7104',
    sequence: 7104,
    jobId: 'job-live-017',
    traceId: 'trace-live-017',
    stageId: 'actor-rollout',
    decidedAt: '2026-08-27T14:28:31Z',
    fallback: false,
    selectedPlanId: 'plan-7104',
    selectedCandidate: 'gpu-cell-4',
    policyVersion: 'policy-2026.08.27.5',
    summary: 'Bound two actor units onto gpu-cell-4 with reduced share headroom.',
    actions: [
      {
        actionId: 'act-7104-a',
        type: 'bind',
        sandboxId: 'sbx-live-a14',
        targetId: 'gpu-cell-4',
        status: 'succeeded',
        detail: 'Generation 7 binding applied with one A100 slice.',
      },
      {
        actionId: 'act-7104-b',
        type: 'resize',
        sandboxId: 'sbx-live-a09',
        targetId: 'gpu-cell-4',
        status: 'succeeded',
        detail: 'Raised CPU reservation to 2400 millicores.',
      },
    ],
  },
  {
    id: 'dec-7098',
    sequence: 7098,
    jobId: 'job-sim-031',
    traceId: 'trace-sim-031',
    stageId: 'learner',
    decidedAt: '2026-08-27T13:51:11Z',
    fallback: true,
    fallbackReason: 'NO_READY_GPU_CAPACITY',
    policyVersion: 'sim-2026.08.26.9',
    summary: 'Returned fallback because no compatible GPU device stayed READY.',
    actions: [],
  },
  {
    id: 'dec-7084',
    sequence: 7084,
    jobId: 'job-replay-204',
    traceId: 'trace-replay-204',
    stageId: 'replay',
    decidedAt: '2026-08-27T13:05:20Z',
    fallback: false,
    selectedPlanId: 'plan-7084',
    selectedCandidate: 'cpu-bank-2',
    policyVersion: 'replay-2026.08.27.2',
    summary: 'Replay workers converged on cpu-bank-2 with no additional mutations.',
    actions: [
      {
        actionId: 'act-7084-a',
        type: 'resume',
        sandboxId: 'sbx-replay-4',
        targetId: 'cpu-bank-2',
        status: 'succeeded',
        detail: 'Resumed from safe point at sequence 18232.',
      },
    ],
  },
];

export const sandboxes: SandboxRecord[] = [
  {
    id: 'sbx-live-a14',
    jobId: 'job-live-017',
    state: 'running',
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
  {
    id: 'sbx-live-a09',
    jobId: 'job-live-017',
    state: 'paused',
    generation: 3,
    nodeLabel: 'gpu-cell-4',
    gpuAttached: true,
    share: 0.25,
    priority: 82,
    safePoint: true,
    offloaded: false,
    updatedAt: '2026-08-27T14:24:10Z',
    bindingSummary: 'A100:1, cpu=2400m, memory=6Gi',
  },
  {
    id: 'sbx-replay-4',
    jobId: 'job-replay-204',
    state: 'running',
    generation: 2,
    nodeLabel: 'cpu-bank-2',
    gpuAttached: false,
    share: 0.7,
    priority: 62,
    safePoint: false,
    offloaded: false,
    updatedAt: '2026-08-27T13:06:01Z',
    bindingSummary: 'cpu=1800m, memory=3Gi',
  },
  {
    id: 'sbx-sim-2',
    jobId: 'job-sim-031',
    state: 'sleeping',
    generation: 5,
    nodeLabel: 'queue-simulation',
    gpuAttached: false,
    share: 0.1,
    priority: 40,
    safePoint: true,
    offloaded: true,
    updatedAt: '2026-08-27T13:52:07Z',
    bindingSummary: 'Offloaded while waiting for GPU capacity',
  },
];

export const timelineEvents: TimelineEvent[] = [
  {
    id: 'evt-901',
    jobId: 'job-live-017',
    title: 'Policy published',
    type: 'policy-published',
    occurredAt: '2026-08-27T14:20:00Z',
    sequence: 901,
    phase: 'learner',
    dataKind: 'live',
    severity: 'info',
    summary: 'Published policy-2026.08.27.5 to actors.',
  },
  {
    id: 'evt-904',
    jobId: 'job-live-017',
    title: 'Backpressure changed',
    type: 'backpressure',
    occurredAt: '2026-08-27T14:26:11Z',
    sequence: 904,
    phase: 'actor-rollout',
    dataKind: 'live',
    severity: 'warn',
    summary: 'Replay buffer saturation crossed 78 percent.',
  },
  {
    id: 'evt-909',
    jobId: 'job-live-017',
    title: 'Decision applied',
    type: 'decision-applied',
    occurredAt: '2026-08-27T14:28:31Z',
    sequence: 909,
    phase: 'actor-rollout',
    sandboxId: 'sbx-live-a14',
    decisionId: 'dec-7104',
    dataKind: 'live',
    severity: 'info',
    summary: 'Applied decision dec-7104 to rebalance actor capacity.',
  },
  {
    id: 'evt-915',
    jobId: 'job-sim-031',
    title: 'Safe point reached',
    type: 'safe-point',
    occurredAt: '2026-08-27T13:50:30Z',
    sequence: 915,
    phase: 'learner',
    sandboxId: 'sbx-sim-2',
    dataKind: 'synthetic',
    severity: 'warn',
    summary: 'Synthetic learner moved to safe point while waiting on GPUs.',
  },
  {
    id: 'evt-920',
    jobId: 'job-replay-204',
    title: 'Sample consumed',
    type: 'sample-consumed',
    occurredAt: '2026-08-27T13:07:01Z',
    sequence: 920,
    phase: 'replay',
    sandboxId: 'sbx-replay-4',
    dataKind: 'replay',
    severity: 'info',
    summary: 'Consumed 1200 replay samples without drift.',
  },
];

export const topology: TopologySnapshot = {
  nodes: [
    { id: 'queue-priority', label: 'priority-train', kind: 'queue', status: 'busy', gpu: false, utilization: 0.92 },
    { id: 'queue-audit', label: 'audit', kind: 'queue', status: 'ready', gpu: false, utilization: 0.48 },
    { id: 'job-live-017', label: 'PPO Actor-Critic Burst', kind: 'job', status: 'degraded', gpu: true, utilization: 0.88 },
    { id: 'job-replay-204', label: 'Replay Latency Audit', kind: 'job', status: 'ready', gpu: false, utilization: 0.51 },
    { id: 'gpu-cell-4', label: 'gpu-cell-4', kind: 'device', status: 'busy', gpu: true, share: 0.94, utilization: 0.97 },
    { id: 'cpu-bank-2', label: 'cpu-bank-2', kind: 'device', status: 'ready', gpu: false, share: 0.61, utilization: 0.59 },
    { id: 'sbx-live-a14', label: 'sbx-live-a14', kind: 'sandbox', status: 'busy', gpu: true, share: 0.5, utilization: 0.72 },
    { id: 'sbx-replay-4', label: 'sbx-replay-4', kind: 'sandbox', status: 'ready', gpu: false, share: 0.7, utilization: 0.46 },
  ],
  edges: [
    { from: 'queue-priority', to: 'job-live-017', relation: 'feeds' },
    { from: 'queue-audit', to: 'job-replay-204', relation: 'feeds' },
    { from: 'job-live-017', to: 'sbx-live-a14', relation: 'runs-in' },
    { from: 'job-replay-204', to: 'sbx-replay-4', relation: 'runs-in' },
    { from: 'sbx-live-a14', to: 'gpu-cell-4', relation: 'scheduled-on' },
    { from: 'sbx-replay-4', to: 'cpu-bank-2', relation: 'scheduled-on' },
  ],
  lastUpdated: now,
};

export const experiments: ExperimentSummary[] = [
  {
    id: 'exp-ppo-compare',
    name: 'PPO rollout policy compare',
    state: 'running',
    createdAt: '2026-08-27T05:00:00Z',
    summary: 'Comparing live PPO against replayed baseline under identical queue pressure.',
    runs: [
      {
        id: 'run-live-ppo',
        experimentId: 'exp-ppo-compare',
        runId: 'run-live-ppo',
        label: `Live ${kindSuffix('live')}`,
        kind: 'live',
        dataKind: 'live',
        policyVersion: 'policy-2026.08.27.5',
        configHash: 'cfg-a9f4',
        codeRevision: 'rev-a13bc1',
        summary: 'Highest throughput, elevated replay buffer pressure.',
        metrics: [
          metric('Reward', '0.81', '+0.04', 'good'),
          metric('Samples/s', '18.4k', '+2.3k', 'good'),
          metric('P95 decision latency', '211ms', '+44ms', 'warn'),
        ],
      },
      {
        id: 'run-replay-ppo',
        experimentId: 'exp-ppo-compare',
        runId: 'run-replay-ppo',
        label: `Replay ${kindSuffix('replay')}`,
        kind: 'replay',
        dataKind: 'replay',
        policyVersion: 'replay-2026.08.27.2',
        configHash: 'cfg-a9f4',
        codeRevision: 'rev-a13bc1',
        summary: 'Lower throughput but stable decision latency envelope.',
        metrics: [
          metric('Reward', '0.76', '-0.01', 'neutral'),
          metric('Samples/s', '11.1k', '-1.2k', 'warn'),
          metric('P95 decision latency', '128ms', '-12ms', 'good'),
        ],
      },
    ],
  },
  {
    id: 'exp-capacity-sim',
    name: 'Synthetic GPU scarcity drill',
    state: 'failed',
    createdAt: '2026-08-26T21:10:00Z',
    completedAt: '2026-08-27T13:53:00Z',
    summary: 'Synthetic drill exhausted READY GPU capacity before learner convergence.',
    runs: [
      {
        id: 'run-sim-a3c',
        experimentId: 'exp-capacity-sim',
        runId: 'run-sim-a3c',
        label: `Synthetic ${kindSuffix('synthetic')}`,
        kind: 'simulation',
        dataKind: 'synthetic',
        policyVersion: 'sim-2026.08.26.9',
        configHash: 'cfg-b29d',
        codeRevision: 'rev-ff02a1',
        summary: 'Stopped on GPU unavailable fallback path.',
        metrics: [
          metric('GPU ready ratio', '0.02', '-0.11', 'critical'),
          metric('Fallbacks', '17', '+12', 'critical'),
          metric('Checkpoint drift', '0.0', 'stable', 'good'),
        ],
      },
    ],
  },
];

export const overview: OverviewResponse = {
  metrics: [
    metric('Active jobs', '3', '+1', 'good'),
    metric('Decision stream lag', '214ms', '+37ms', 'warn'),
    metric('GPU headroom', '6%', '-4%', 'critical'),
    metric('Replay drift', '0.3%', '-0.1%', 'good'),
  ],
  jobs,
  experiments,
  decisions,
  capabilities: {
    protocolVersion: 'v0.3',
    dataKinds: ['DATA_KIND_SYNTHETIC', 'DATA_KIND_REPLAY', 'DATA_KIND_LIVE'],
    pagination: 'opaque_stable_token',
  },
  systemHealth: {
    status: 'ok',
    observedAt: now,
    counts: {
      jobs: jobs.length,
      runs: runs.length,
      operations: 5,
      decisions: decisions.length,
      replays: 1,
      experiments: experiments.length,
    },
  },
  alerts: [
    {
      id: 'al-1',
      title: 'GPU capacity degraded',
      tone: 'critical',
      detail: 'Synthetic learner traffic is being fenced off because READY GPU capacity dropped below policy floor.',
    },
    {
      id: 'al-2',
      title: 'Replay path stable',
      tone: 'info',
      detail: 'Replay workload remains within target latency envelope and can be used for safe comparison.',
    },
  ],
};

export function getJobDetail(jobId: string): JobDetailResponse | undefined {
  const job = jobs.find((entry) => entry.id === jobId);
  if (!job) {
    return undefined;
  }
  return {
    job,
    runs: runs.filter((entry) => entry.jobId === jobId),
    selectedRunId: runs.find((entry) => entry.jobId === jobId)?.id,
    decisions: decisions.filter((entry) => entry.jobId === jobId),
    sandboxes: sandboxes.filter((entry) => entry.jobId === jobId),
    metrics: [
      metric('Units active', `${job.activeUnits}/${job.desiredUnits}`, undefined, job.activeUnits === job.desiredUnits ? 'good' : 'warn'),
      metric('Priority', `${job.priority}`),
      metric('Health', job.health.toUpperCase(), undefined, job.health === 'healthy' ? 'good' : job.health === 'degraded' ? 'warn' : 'critical'),
    ],
  };
}

export const timeline: TimelineResponse = { events: timelineEvents };

export function getRunList(jobId: string): RunSummary[] {
  return runs.filter((entry) => entry.jobId === jobId);
}

export function getDecisionExplorer(decisionId: string): DecisionExplorerResult | undefined {
  const decision = decisions.find((entry) => entry.id === decisionId);
  if (!decision) {
    return undefined;
  }
  return {
    selectedDecision: decision,
    candidates: [
      { id: 'gpu-cell-4', deviceLabel: 'gpu-cell-4', score: 0.91, reason: 'Highest headroom among READY GPU cells.', selected: true },
      { id: 'gpu-cell-2', deviceLabel: 'gpu-cell-2', score: 0.72, reason: 'Rejected due to safe-point conflict on learner sandbox.', selected: false },
      { id: 'cpu-bank-2', deviceLabel: 'cpu-bank-2', score: 0.41, reason: 'Rejected because GPU capability is required.', selected: false },
    ],
    relatedActions: decision.actions,
  };
}
