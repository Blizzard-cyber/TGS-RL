import { createContext, useContext } from 'react';
import { createApiClient } from '../api/client';
import type { ApiClient } from '../api/types';

const defaultClient = createApiClient();

export const ApiContext = createContext<ApiClient>(defaultClient);

export function useApiClient() {
  return useContext(ApiContext);
}
