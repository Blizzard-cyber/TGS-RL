import type { PropsWithChildren } from 'react';
import { createApiClient } from '../api/client';
import type { ApiClient } from '../api/types';
import { ApiContext } from './apiContext';

const defaultApiClient = createApiClient();

type ApiProviderProps = PropsWithChildren<{
  client?: ApiClient;
}>;

export function ApiProvider({ children, client }: ApiProviderProps) {
  return <ApiContext.Provider value={client ?? defaultApiClient}>{children}</ApiContext.Provider>;
}
