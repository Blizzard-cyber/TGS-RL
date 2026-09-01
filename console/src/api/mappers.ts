import type {
  DecisionExplorerResult,
  DecisionRecord,
  ExperimentSummary,
  JobSummary,
  OverviewResponse,
  RunSummary,
  SandboxResponse,
  TimelineResponse,
  TraceResponse,
  TopologySnapshot,
} from './types';
import {
  collectBindingDeviceIds,
  createControlResult,
  deriveOverviewHealth,
  ensureArray,
  ensureObject,
  toDataKind,
  toHealthFromRunState,
  toJobState,
  toRolloutMode,
  toTimestamp,
} from './helpers';

export function mapJob(job: unknown, run?: RunSummary): JobSummary {
  const input = ensureObject(job);
  const labels = ensureObject(input.labels);
  const resources = ensureObject(input.resourcesPerUnit);
  const desiredUnits = typeof input.desiredUnits === 'number' ? input.desiredUnits : 0;
  return {
    id: String(input.jobId ?? ''),
    name: String(input.displayName ?? input.jobId ?? '未命名任务'),
    algorithm: String(input.algorithm ?? '未知'),
    state: toJobState(typeof input.state === 'string' ? input.state : undefined),
    rolloutMode: toRolloutMode(typeof input.rolloutMode === 'string' ? input.rolloutMode : undefined),
    dataKind: toDataKind(typeof input.dataKind === 'string' ? input.dataKind : undefined),
    queue: String(input.queue ?? 'default'),
    priority: typeof input.priority === 'number' ? input.priority : 0,
    desiredUnits,
    gpuRequired:
      typeof resources.acceleratorUnits === 'number' && resources.acceleratorUnits > 0,
    createdAt: toTimestamp(input.createdAt),
    updatedAt: run?.startedAt ?? toTimestamp(input.createdAt),
    owner: String(labels.owner ?? 'gateway'),
    policyVersion: run?.policyVersion ?? String(labels.policy_version ?? ''),
    traceId: run?.traceId ?? '',
    executionId: '',
    currentRunId: run?.id,
    latestRunState: run?.state,
    runCount: run ? 1 : 0,
    health: run ? toHealthFromRunState(run.state) : 'degraded',
  };
}

function toDecisionActionType(value: unknown, fallbackActionId: string): DecisionRecord['actions'][number]['type'] {
  const normalized = String(value ?? fallbackActionId)
    .toLowerCase()
    .replace(/^action_type_/, '');
  const actions: DecisionRecord['actions'][number]['type'][] = [
    'set_priority',
    'set_share',
    'recreate',
    'release',
    'offload',
    'rebind',
    'resize',
    'resume',
    'pause',
    'sleep',
    'bind',
  ];
  return actions.find((action) => normalized === action || normalized.includes(action)) ?? 'unknown';
}

function toDecisionActionStatus(value: unknown): DecisionRecord['actions'][number]['status'] {
  const normalized = String(value ?? '');
  if (normalized === 'ACTION_RESULT_STATUS_FAILED') {
    return 'failed';
  }
  if (normalized === 'ACTION_RESULT_STATUS_SKIPPED') {
    return 'skipped';
  }
  if (normalized === 'ACTION_RESULT_STATUS_ROLLED_BACK') {
    return 'rolled_back';
  }
  if (normalized === 'ACTION_RESULT_STATUS_SUCCEEDED') {
    return 'succeeded';
  }
  return 'unknown';
}

export function mapRun(run: unknown): RunSummary {
  const input = ensureObject(run);
  const statuses = ensureArray<Record<string, unknown>>(input.componentStatus);
  return {
    id: String(input.runId ?? ''),
    jobId: String(input.jobId ?? ''),
    traceId: String(input.traceId ?? ''),
    state: String(input.runState ?? input.state ?? 'JOB_RUN_STATE_UNKNOWN'),
    attempt: typeof input.attempt === 'number' ? input.attempt : 0,
    policyVersion: String(input.policyVersion ?? ''),
    createdAt: toTimestamp(input.createdAt),
    startedAt: typeof input.startedAt === 'string' ? input.startedAt : undefined,
    completedAt: typeof input.completedAt === 'string' ? input.completedAt : undefined,
    dataKind: toDataKind(typeof input.dataKind === 'string' ? input.dataKind : undefined),
    componentHealth: statuses.map((status) => ({
      component: String(status.component ?? 'unknown'),
      health:
        status.health === 'COMPONENT_HEALTH_FAILED'
          ? 'failed'
          : status.health === 'COMPONENT_HEALTH_DEGRADED'
            ? 'degraded'
            : status.health === 'COMPONENT_HEALTH_PROGRESSING'
              ? 'progressing'
              : 'healthy',
      detail: String(status.detail ?? ''),
    })),
  };
}

export function mapDecision(inputValue: unknown): DecisionRecord {
  const input = ensureObject(inputValue);
  const selectedPlan = ensureObject(input.selectedPlan);
  const planActions = ensureArray<Record<string, unknown>>(selectedPlan.actions);
  const actionResults = ensureArray<Record<string, unknown>>(input.actionResults);
  const actionResultById = new Map(
    actionResults.map((actionResult) => [
      String(actionResult.actionId ?? ''),
      actionResult,
    ]),
  );
  const actions: DecisionRecord['actions'] = (planActions.length ? planActions : actionResults).map((plannedAction) => {
    const actionId = String(plannedAction.actionId ?? '');
    const actionResult = actionResultById.get(actionId) ?? {};
    const sandbox = ensureObject(plannedAction.sandbox);
    const target = ensureObject(plannedAction.target);
    return {
      actionId,
      type: toDecisionActionType(plannedAction.actionType ?? plannedAction.type, actionId),
      sandboxId: String(plannedAction.sandboxId ?? sandbox.sandboxId ?? ''),
      targetId: String(plannedAction.targetId ?? target.targetId ?? target.deviceId ?? ''),
      status: toDecisionActionStatus(actionResult.status),
      detail: String(actionResult.errorMessage ?? actionResult.errorCode ?? plannedAction.detail ?? '动作已完成'),
    };
  });
  const selectedPlanId = typeof selectedPlan.planId === 'string' ? selectedPlan.planId : undefined;
  const selectedBindings = ensureArray<Record<string, unknown>>(selectedPlan.bindings);
  const selectedCandidate = ensureArray<Record<string, unknown>>(input.candidates).find(
    (candidate) => candidateMatchesSelectedBindings(candidate, selectedBindings),
  );
  return {
    id: String(input.decisionId ?? ''),
    sequence: typeof input.sequence === 'number' ? input.sequence : 0,
    jobId: '',
    runId: typeof input.runId === 'string' ? input.runId : undefined,
    traceId: String(input.traceId ?? ''),
    stageId: String(input.stageId ?? ''),
    decidedAt: toTimestamp(input.decidedAt),
    fallback: Boolean(input.fallback),
    fallbackReason: typeof input.fallbackReason === 'string' ? input.fallbackReason : undefined,
    selectedPlanId,
    selectedCandidate: typeof selectedCandidate?.candidateId === 'string' ? selectedCandidate.candidateId : undefined,
    policyVersion: String(input.policyVersion ?? ''),
    summary: input.fallback
      ? String(input.fallbackReason ?? '调度器执行了回退策略')
      : `阶段 ${String(input.stageId ?? '')} 已选择计划 ${String(selectedPlanId ?? '未命名计划')}。`,
    actions,
  };
}

function normalizedDeviceIds(binding: Record<string, unknown>): string[] {
  return ensureArray<unknown>(binding.deviceIds).map(String).sort();
}

function stableObject(value: unknown): string {
  const normalized = (item: unknown): unknown => {
    if (Array.isArray(item)) {
      return item.map(normalized);
    }
    if (item !== null && typeof item === 'object') {
      return Object.fromEntries(
        Object.entries(item as Record<string, unknown>)
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([key, nested]) => [key, normalized(nested)]),
      );
    }
    return item;
  };
  return JSON.stringify(normalized(value));
}

function bindingIdentityMatches(
  candidateBinding: Record<string, unknown>,
  selectedBinding: Record<string, unknown>,
): boolean {
  if (String(candidateBinding.pendingUnitId ?? '') !== String(selectedBinding.pendingUnitId ?? '')) {
    return false;
  }
  if (stableObject(normalizedDeviceIds(candidateBinding)) !== stableObject(normalizedDeviceIds(selectedBinding))) {
    return false;
  }
  const candidateGeneration = candidateBinding.generation;
  const selectedGeneration = selectedBinding.generation;
  if (typeof candidateGeneration === 'number' && typeof selectedGeneration === 'number' && candidateGeneration !== selectedGeneration) {
    return false;
  }
  const candidateResources = ensureObject(candidateBinding.resources);
  const selectedResources = ensureObject(selectedBinding.resources);
  if (Object.keys(candidateResources).length > 0 && Object.keys(selectedResources).length > 0) {
    return stableObject(candidateResources) === stableObject(selectedResources);
  }
  return true;
}

function candidateMatchesSelectedBindings(
  candidate: Record<string, unknown>,
  selectedBindings: Record<string, unknown>[],
): boolean {
  const candidateBindings = ensureArray<Record<string, unknown>>(ensureObject(candidate.plan).bindings);
  return candidateBindings.length > 0 && candidateBindings.every((candidateBinding) =>
    selectedBindings.some((selectedBinding) => bindingIdentityMatches(candidateBinding, selectedBinding)),
  );
}

export function mapTimelineEvent(inputValue: unknown, jobId: string): TimelineResponse['events'][number] {
  const input = ensureObject(inputValue);
  const eventType = String(input.eventType ?? 'JOB_EVENT_TYPE_UNKNOWN');
  const componentStatus = ensureObject(input.componentStatus);
  const operation = ensureObject(input.operation);
  const detail = typeof input.detail === 'string' ? input.detail : '';
  return {
    id: String(input.eventId ?? ''),
    jobId,
    runId: typeof input.runId === 'string' ? input.runId : undefined,
    title: eventType.replace(/^JOB_EVENT_TYPE_/, '').replace(/_/g, ' '),
    type:
      eventType === 'JOB_EVENT_TYPE_JOB_STARTED'
        ? 'phase-started'
        : eventType === 'JOB_EVENT_TYPE_JOB_STOPPED'
          ? 'phase-completed'
          : eventType === 'JOB_EVENT_TYPE_COMPONENT_CHANGED'
            ? 'backpressure'
            : eventType === 'JOB_EVENT_TYPE_OPERATION_RECORDED'
              ? 'decision-applied'
              : 'policy-published',
    occurredAt: toTimestamp(input.occurredAt),
    sequence: typeof input.sequence === 'number' ? input.sequence : 0,
    phase: String(componentStatus.component ?? operation.type ?? 'control-plane'),
    sandboxId: undefined,
    decisionId: undefined,
    dataKind: toDataKind(typeof input.dataKind === 'string' ? input.dataKind : undefined),
    severity:
      componentStatus.health === 'COMPONENT_HEALTH_FAILED'
        ? 'critical'
        : componentStatus.health === 'COMPONENT_HEALTH_DEGRADED'
          ? 'warn'
          : 'info',
    summary: detail || String(operation.reason ?? componentStatus.detail ?? eventType),
  };
}

export function mapTraceEvent(inputValue: unknown, jobId: string): TraceResponse['events'][number] {
  const input = ensureObject(inputValue);
  const sourceAttributes = ensureObject(input.attributes);
  const traceAttributeKeys: Record<string, string> = {
    durationMs: 'duration_ms',
    durationUs: 'duration_us',
    durationNs: 'duration_ns',
    requestId: 'request_id',
    executorId: 'executor_id',
    workerId: 'worker_id',
    spanId: 'span_id',
    parentSpanId: 'parent_span_id',
    batchSize: 'batch_size',
    gpuActiveMs: 'gpu_active_ms',
    deviceId: 'device_id',
    deviceIds: 'device_ids',
    runtimeUnitId: 'runtime_unit_id',
    bindingId: 'binding_id',
    displayName: 'display_name',
  };
  const attributes = Object.fromEntries(
    Object.entries(sourceAttributes).map(([key, value]) => [
      traceAttributeKeys[key] ?? key,
      String(value),
    ]),
  );
  return {
    id: String(input.eventId ?? ''),
    jobId: String(input.jobId ?? jobId),
    runId: String(input.runId ?? ''),
    traceId: String(input.traceId ?? ''),
    executionId: String(input.executionId ?? ''),
    sequence: Number(input.sequence ?? 0),
    occurredAt: toTimestamp(input.occurredAt),
    type: String(input.eventType ?? 'TRACE_EVENT_TYPE_UNKNOWN').replace(/^TRACE_EVENT_TYPE_/, '').toLowerCase(),
    phaseId: String(input.phaseId ?? ''),
    stageId: String(input.stageId ?? ''),
    algorithm: String(input.algorithm ?? ''),
    rolloutMode: String(input.rolloutMode ?? '').replace(/^ROLLOUT_MODE_/, '').toLowerCase(),
    policyVersion: String(input.policyVersion ?? ''),
    bufferLevel: Number(input.bufferLevel ?? 0),
    safePoint: Boolean(input.safePoint),
    decisionId: typeof input.decisionId === 'string' && input.decisionId ? input.decisionId : undefined,
    sandboxId: typeof input.sandboxId === 'string' && input.sandboxId ? input.sandboxId : undefined,
    generation: Number(input.generation ?? 0),
    dataKind: toDataKind(typeof input.dataKind === 'string' ? input.dataKind : undefined),
    attributes,
  };
}

export function mapSandbox(inputValue: unknown, jobId: string): SandboxResponse['sandboxes'][number] {
  const input = ensureObject(inputValue);
  const binding = ensureObject(input.binding);
  const resources = ensureObject(binding.resources);
  const deviceIds = collectBindingDeviceIds(binding);
  const acceleratorUnits =
    typeof resources.acceleratorUnits === 'number' ? resources.acceleratorUnits : 0;
  const state = String(input.state ?? 'RUNTIME_STATE_UNKNOWN').replace(/^RUNTIME_STATE_/, '').toLowerCase();
  const sandboxState: SandboxResponse['sandboxes'][number]['state'] =
    state === 'requested' ||
    state === 'bound' ||
    state === 'running' ||
    state === 'paused' ||
    state === 'sleeping' ||
    state === 'terminated' ||
    state === 'failed'
      ? state
      : 'unknown';
  return {
    id: String(input.sandboxId ?? ''),
    jobId,
    runId: typeof input.runId === 'string' ? input.runId : undefined,
    state: sandboxState,
    generation: typeof input.generation === 'number' ? input.generation : 0,
    nodeLabel: deviceIds[0] ?? '未绑定',
    gpuAttached:
      acceleratorUnits > 0 ||
      deviceIds.some((id) => /(^mig-)|gpu|nvidia|cuda|a100|h100/i.test(id)),
    share: typeof input.share === 'number' ? input.share : 0,
    priority: typeof input.priority === 'number' ? input.priority : 0,
    safePoint: Boolean(input.safePoint),
    offloaded: Boolean(input.offloaded),
    updatedAt: toTimestamp(input.observedAt),
    bindingSummary: `设备=${deviceIds.join(', ') || '无'}，处理器=${String(resources.cpuMillis ?? 0)}m`,
  };
}

export function mapTopology(input: unknown, jobId: string): TopologySnapshot {
  const payload = ensureObject(input);
  const manifest = ensureObject(payload.manifest);
  const runtimeUnits = ensureArray<Record<string, unknown>>(payload.runtimeUnits);
  const sandboxRecords = ensureArray<Record<string, unknown>>(payload.sandboxes);
  const run = ensureObject(payload.run);
  const deviceIds = [...new Set(sandboxRecords.flatMap(collectBindingDeviceIds))].sort();
  const nodes = [
    {
      id: jobId,
      label: String(run.displayName ?? jobId),
      kind: 'job' as const,
      status: 'ready' as const,
      gpu: false,
    },
    ...runtimeUnits.map((unit) => ({
      id: String(unit.runtimeUnitId ?? ''),
      label: String(unit.phaseId ?? unit.runtimeUnitId ?? ''),
      kind: 'runtime' as const,
      status:
        String(unit.state ?? '').includes('FAILED')
          ? 'down' as const
          : String(unit.state ?? '').includes('PAUS')
            ? 'degraded' as const
            : String(unit.state ?? '').includes('RUNNING')
              ? 'busy' as const
              : 'ready' as const,
      gpu:
        Number(ensureObject(unit.requestedResources).acceleratorUnits ?? 0) > 0,
    })),
    ...sandboxRecords.map((sandbox) => {
      const mapped = mapSandbox(sandbox, jobId);
      return {
        id: mapped.id,
        label: mapped.id,
        kind: 'sandbox' as const,
        status:
          mapped.state === 'terminated' || mapped.state === 'failed'
            ? 'down' as const
            : mapped.state === 'paused' || mapped.state === 'sleeping' || mapped.state === 'unknown'
              ? 'degraded' as const
              : mapped.state === 'running'
                ? 'busy' as const
                : 'ready' as const,
        gpu: mapped.gpuAttached,
        share: mapped.share,
      };
    }),
    ...deviceIds.map((deviceId) => ({
      id: deviceId,
      label: deviceId,
      kind: 'device' as const,
      status: 'ready' as const,
      gpu: true,
    })),
  ];
  const edges = [
    ...runtimeUnits.map((unit) => ({
      from: jobId,
      to: String(unit.runtimeUnitId ?? ''),
      relation: 'feeds' as const,
    })),
    ...sandboxRecords.flatMap((sandbox) => {
      const binding = ensureObject(sandbox.binding);
      const runtimeUnitId = String(binding.runtimeUnitId ?? binding.pendingUnitId ?? '');
      const sandboxId = String(sandbox.sandboxId ?? '');
      const runtimeEdge = runtimeUnitId
        ? [{ from: runtimeUnitId, to: sandboxId, relation: 'runs-in' as const }]
        : [];
      const deviceEdges = collectBindingDeviceIds(binding).map((deviceId) => ({
        from: sandboxId,
        to: deviceId,
        relation: 'scheduled-on' as const,
      }));
      return [...runtimeEdge, ...deviceEdges];
    }),
  ];
  return {
    runId: typeof run.runId === 'string' ? run.runId : undefined,
    manifestId: typeof manifest.manifestId === 'string' ? manifest.manifestId : undefined,
    nodes,
    edges,
    lastUpdated: toTimestamp(run.createdAt ?? manifest.createdAt),
  };
}

export function mapExperiment(inputValue: unknown): ExperimentSummary {
  const input = ensureObject(inputValue);
  const runs = ensureArray<Record<string, unknown>>(input.runs).map((run) => ({
    id: String(run.experimentRunId ?? ''),
    experimentId: String(run.experimentId ?? input.experimentId ?? ''),
    runId: String(run.runId ?? ''),
    label: String(run.runId ?? 'run'),
    kind: (
      String(run.kind ?? '').includes('REPLAY')
        ? 'replay'
        : String(run.kind ?? '').includes('LIVE')
          ? 'live'
          : 'simulation'
    ) as 'replay' | 'live' | 'simulation',
    dataKind: toDataKind(typeof run.dataKind === 'string' ? run.dataKind : undefined),
    policyVersion: String(run.policyVersion ?? ''),
    configHash: String(run.configHash ?? ''),
    codeRevision: String(run.codeRevision ?? ''),
    summary: String(ensureObject(run.annotations).summary ?? ''),
    metrics: ensureArray<Record<string, unknown>>(run.metrics).map((metric) => ({
      label: String(metric.name ?? 'metric'),
      value: String(metric.value ?? ''),
      delta: typeof metric.unit === 'string' ? metric.unit : undefined,
    })),
  }));
  return {
    id: String(input.experimentId ?? ''),
    name: String(input.displayName ?? input.experimentId ?? '未命名实验'),
    state:
      String(input.state ?? 'EXPERIMENT_STATE_PENDING').replace(/^EXPERIMENT_STATE_/, '').toLowerCase() as ExperimentSummary['state'],
    createdAt: toTimestamp(input.createdAt),
    completedAt: typeof input.completedAt === 'string' ? input.completedAt : undefined,
    summary: String(input.summary ?? '暂无实验摘要。'),
    runs,
  };
}

export function mapDecisionExplorer(decision: unknown, jobId: string): DecisionExplorerResult {
  const rawDecision = ensureObject(decision);
  const mappedDecision = mapDecision(rawDecision);
  mappedDecision.jobId = jobId;
  return {
    selectedDecision: mappedDecision,
    candidates: ensureArray<Record<string, unknown>>(rawDecision.candidates).map((candidate) => {
      const plan = ensureObject(candidate.plan);
      const deviceIds = collectBindingDeviceIds(plan);
      return {
        id: String(candidate.candidateId ?? ''),
        deviceLabel: deviceIds.join(', ') || String(candidate.candidateId ?? 'candidate'),
        score: typeof candidate.score === 'number' ? candidate.score : 0,
        reason: String(candidate.detail ?? '候选资源满足约束'),
        selected: candidateMatchesSelectedBindings(
          candidate,
          ensureArray<Record<string, unknown>>(ensureObject(rawDecision.selectedPlan).bindings),
        ),
      };
    }),
    rejectedCandidates: ensureArray<Record<string, unknown>>(rawDecision.rejectedCandidates).map(
      (candidate) => ({
        id: String(candidate.candidateId ?? ''),
        reason: String(candidate.reason ?? 'CANDIDATE_REJECTION_REASON_UNKNOWN'),
        detail: String(candidate.detail ?? '候选资源被拒绝'),
      }),
    ),
    relatedActions: mappedDecision.actions,
  };
}

export function buildOverview(
  mappedJobs: JobSummary[],
  mappedExperiments: ExperimentSummary[],
  health: Record<string, unknown>,
  capabilities: Record<string, unknown>,
): OverviewResponse {
  return {
    metrics: [
      { label: '保留任务', value: String(mappedJobs.length), tone: 'good' },
      {
        label: '网关状态',
        value: String(health.status ?? '') === 'ok' ? '正常' : '异常',
        tone: String(health.status ?? '') === 'ok' ? 'good' : 'critical',
      },
      {
        label: '协议版本',
        value: String(capabilities.protocolVersion ?? 'v0.0'),
      },
      {
        label: '实验数量',
        value: String(mappedExperiments.length),
      },
    ],
    jobs: mappedJobs,
    experiments: mappedExperiments,
    decisions: [],
    capabilities: {
      protocolVersion: String(capabilities.protocolVersion ?? 'v0.0'),
      dataKinds: ensureArray<string>(capabilities.dataKinds),
      pagination: String(ensureObject(capabilities.pagination).kind ?? 'opaque'),
    },
    systemHealth: {
      status: String(health.status ?? 'unknown'),
      observedAt: String(health.observedAt ?? ''),
      counts: ensureObject(health.counts) as Record<string, number>,
    },
    alerts: deriveOverviewHealth(mappedJobs, health),
  };
}

export { createControlResult };
