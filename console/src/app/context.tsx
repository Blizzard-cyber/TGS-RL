import type { PropsWithChildren } from 'react';
import { createApiClient } from '../api/client';
import type { ApiClient } from '../api/types';
import { ApiContext } from './apiContext';

type ApiProviderProps = PropsWithChildren<{
  client?: ApiClient;
}>;

export function ApiProvider({ children, client }: ApiProviderProps) {
  return <ApiContext.Provider value={client ?? createApiClient()}>{children}</ApiContext.Provider>;
}
