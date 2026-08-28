function withOptionalRunId(path: string, runId?: string) {
  if (!runId) {
    return path;
  }
  const searchParams = new URLSearchParams({ runId });
  return `${path}?${searchParams.toString()}`;
}

export function jobDetailPath(jobId: string, runId?: string) {
  return withOptionalRunId(`/jobs/${jobId}`, runId);
}

export function jobTimelinePath(jobId: string, runId?: string) {
  return withOptionalRunId(`/jobs/${jobId}/timeline`, runId);
}

export function jobTopologyPath(jobId: string, runId?: string) {
  return withOptionalRunId(`/jobs/${jobId}/topology`, runId);
}

export function jobSandboxesPath(jobId: string, runId?: string) {
  return withOptionalRunId(`/jobs/${jobId}/sandboxes`, runId);
}

export function jobDecisionsPath(jobId: string, runId?: string) {
  return withOptionalRunId(`/jobs/${jobId}/decisions`, runId);
}
