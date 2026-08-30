import type { ApiError, FetchResponse, GatewayFilterKey } from './contracts';
import { ensureObject, normalizeGatewayJson, pickGatewayFilters } from './helpers';
import type { QueryOptions, QueryResult } from './types';

export interface GetRequestConfig {
  includePagination?: boolean;
  allowedFilters?: readonly GatewayFilterKey[];
}

async function readJsonBody(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text.trim()) {
    return undefined;
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new Error('Gateway returned an invalid JSON response body.');
  }
}

function parseApiError(status: number, payload: unknown): ApiError {
  const normalized = ensureObject(normalizeGatewayJson(payload));
  const error = ensureObject(normalized.error);
  const details = ensureObject(error.details);
  const fallbackMessage = `Request failed with status ${status}.`;
  return {
    status,
    code: typeof error.code === 'string' ? error.code : undefined,
    message: typeof error.message === 'string' && error.message ? error.message : fallbackMessage,
    details: Object.keys(details).length ? details : undefined,
    requestId: typeof error.requestId === 'string' ? error.requestId : undefined,
  };
}

function toErrorResult<T>(
  response: Response,
  apiError: ApiError,
  options?: QueryOptions,
): QueryResult<T> {
  if (response.status === 403) {
    return {
      state: 'forbidden',
      message: apiError.message,
      retryable: false,
      apiError,
    };
  }
  if (response.status === 503 && (options?.filters?.require_gpu === 'true' || options?.filters?.requireGpu === 'true')) {
    return {
      state: 'gpu-unavailable',
      message: apiError.message,
      retryable: true,
      apiError,
    };
  }
  return {
    state: 'error',
    message: apiError.message,
    retryable: response.status === 429 || response.status >= 500,
    apiError,
  };
}

function createTransportError(message: string, status = 0): ApiError {
  return {
    status,
    code: status === 0 ? 'transport_error' : undefined,
    message,
    details: undefined,
    requestId: undefined,
  };
}

function toTransportErrorResult<T>(message: string, retryable = true): QueryResult<T> {
  const apiError = createTransportError(message, 0);
  return {
    state: 'error',
    message: apiError.message,
    retryable,
    apiError,
  };
}

async function fetchWithContract(input: URL, init: RequestInit): Promise<Response | QueryResult<never>> {
  try {
    return await fetch(input, init);
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      return toTransportErrorResult('Request aborted.', true);
    }
    if (error instanceof Error) {
      return toTransportErrorResult(error.message || 'Network request failed.', true);
    }
    return toTransportErrorResult('Network request failed.', true);
  }
}

function createRequestId(): string {
  if (typeof globalThis.crypto?.randomUUID === 'function') {
    return globalThis.crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export class GatewayTransport {
  constructor(private readonly baseUrl = '') {}

  async get<T>(
    path: string,
    options?: QueryOptions,
    config?: GetRequestConfig,
  ): Promise<QueryResult<T>> {
    const url = new URL(`${this.baseUrl}${path}`, window.location.origin);
    if (config?.includePagination !== false && options?.pageToken) {
      url.searchParams.set('page_token', options.pageToken);
    }
    if (config?.includePagination !== false && options?.limit) {
      url.searchParams.set('limit', String(options.limit));
    }
    for (const [key, value] of Object.entries(pickGatewayFilters(options?.filters, config?.allowedFilters ?? []) ?? {})) {
      url.searchParams.set(key, value);
    }

    const fetchResult = await fetchWithContract(url, { signal: options?.signal });
    if ('state' in fetchResult) {
      return fetchResult as QueryResult<T>;
    }

    let body: unknown;
    try {
      body = await readJsonBody(fetchResult);
    } catch (error) {
      if (!fetchResult.ok) {
        body = undefined;
      } else if (error instanceof Error) {
        return toTransportErrorResult<T>(error.message, false);
      } else {
        return toTransportErrorResult<T>('Gateway returned an invalid JSON response body.', false);
      }
    }
    if (!fetchResult.ok) {
      return toErrorResult<T>(fetchResult, parseApiError(fetchResult.status, body), options);
    }

    const normalizedPayload = ensureObject(normalizeGatewayJson((body ?? {}) as FetchResponse));
    return {
      state: 'ready',
      data: normalizedPayload as T,
      pageInfo: {
        nextPageToken:
          typeof normalizedPayload.nextPageToken === 'string' ? normalizedPayload.nextPageToken : undefined,
      },
    };
  }

  async post<T extends Record<string, unknown>>(
    path: string,
    body?: Record<string, unknown>,
  ): Promise<QueryResult<T>> {
    const url = new URL(`${this.baseUrl}${path}`, window.location.origin);
    const fetchResult = await fetchWithContract(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createRequestId(),
        'X-Request-Id': createRequestId(),
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    if ('state' in fetchResult) {
      return fetchResult as QueryResult<T>;
    }

    let responseBody: unknown;
    try {
      responseBody = await readJsonBody(fetchResult);
    } catch (error) {
      if (!fetchResult.ok) {
        responseBody = undefined;
      } else if (error instanceof Error) {
        return toTransportErrorResult<T>(error.message, false);
      } else {
        return toTransportErrorResult<T>('Gateway returned an invalid JSON response body.', false);
      }
    }
    if (!fetchResult.ok) {
      return toErrorResult<T>(fetchResult, parseApiError(fetchResult.status, responseBody));
    }
    const normalizedPayload = normalizeGatewayJson(responseBody ?? {});
    return {
      state: 'ready',
      data: normalizedPayload as T,
    };
  }
}
