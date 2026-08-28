export type Json =
  | string
  | number
  | boolean
  | null
  | Json[]
  | { [key: string]: Json };

export interface FetchResponse {
  nextPageToken?: string;
  [key: string]: Json | undefined;
}

export type GatewayFilterKey = 'after_event_id' | 'data_kind' | 'run_id';

export interface ApiError {
  code?: string;
  message: string;
  details?: Record<string, unknown>;
  requestId?: string;
  status: number;
}

export interface ErrorEnvelope {
  error?: {
    code?: string;
    message?: string;
    details?: Record<string, unknown>;
    request_id?: string;
  };
}

export function parseJson<T>(value: Json): T {
  return value as T;
}
