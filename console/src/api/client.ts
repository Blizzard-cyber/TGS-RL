import type { ApiClient } from './types';
import { HttpApiClient } from './httpClient';
import { MockApiClient } from './mockClient';

export function createApiClient(): ApiClient {
  const adapter = import.meta.env.VITE_TGSRL_API_ADAPTER ?? 'http';
  return adapter === 'mock' ? new MockApiClient() : new HttpApiClient(import.meta.env.VITE_TGSRL_API_BASE ?? '');
}

export { HttpApiClient, MockApiClient };
