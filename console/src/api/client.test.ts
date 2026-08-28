import { describe, expect, it } from 'vitest';
import { MockApiClient } from './client';

describe('MockApiClient', () => {
  it('omits a next page token when the filtered decision set fits on one page', async () => {
    const client = new MockApiClient();

    const first = await client.listDecisions('job-live-017', { limit: 1, filters: { mode: 'ready' } });

    expect(first.state).toBe('ready');
    expect(first.data).toHaveLength(1);
    expect(first.pageInfo?.nextPageToken).toBeUndefined();
  });

  it('returns gpu-unavailable state when requested', async () => {
    const client = new MockApiClient();

    const result = await client.getJobDetail('job-live-017', {
      filters: { mode: 'gpu-unavailable', requireGpu: 'true' },
    });

    expect(result.state).toBe('gpu-unavailable');
    expect(result.retryable).toBe(true);
  });

  it('filters and paginates runs, timeline, sandboxes, and decisions by run scope', async () => {
    const client = new MockApiClient();

    const runs = await client.listRuns('job-live-017', { limit: 1 });
    expect(runs.state).toBe('ready');
    expect(runs.data).toHaveLength(1);
    expect(runs.pageInfo?.nextPageToken).toBeUndefined();

    const timeline = await client.listTimeline('job-live-017', {
      limit: 1,
      filters: { run_id: 'run-live-017-missing' },
    });
    expect(timeline.state).toBe('ready');
    expect(timeline.data?.events).toHaveLength(0);
    expect(timeline.pageInfo?.nextPageToken).toBeUndefined();

    const sandboxes = await client.listSandboxes('job-live-017', {
      limit: 1,
      filters: { run_id: 'run-live-017-missing' },
    });
    expect(sandboxes.state).toBe('ready');
    expect(sandboxes.data?.sandboxes).toHaveLength(0);
    expect(sandboxes.pageInfo?.nextPageToken).toBeUndefined();

    const decisions = await client.listDecisions('job-live-017', {
      limit: 1,
      filters: { run_id: 'run-live-017-missing' },
    });
    expect(decisions.state).toBe('ready');
    expect(decisions.data).toHaveLength(0);
    expect(decisions.pageInfo?.nextPageToken).toBeUndefined();
  });

  it('returns empty topology when the selected run does not match the retained topology run', async () => {
    const client = new MockApiClient();

    const result = await client.getTopology('job-live-017', {
      filters: { run_id: 'run-does-not-exist' },
    });

    expect(result.state).toBe('empty');
    expect(result.message).toBe('No topology matched the selected job and run filters.');
  });

  it('surfaces invalid or cross-surface mock page tokens as errors', async () => {
    const client = new MockApiClient();

    const invalid = await client.listRuns('job-live-017', {
      pageToken: 'offset:1',
    });
    expect(invalid.state).toBe('error');
    expect(invalid.message).toBe('Invalid pagination token for runs.');

    const crossSurface = await client.listTimeline('job-live-017', {
      pageToken: 'mock:runs:job%3Djob-live-017:1',
    });
    expect(crossSurface.state).toBe('error');
    expect(crossSurface.message).toBe('Invalid pagination token for timeline.');
  });

  it('aborts mock queries via AbortSignal instead of silently completing', async () => {
    const client = new MockApiClient();
    const controller = new AbortController();
    const promise = client.listTimeline('job-live-017', { signal: controller.signal });

    controller.abort();

    await expect(promise).rejects.toMatchObject({ name: 'AbortError' });
  });
});
