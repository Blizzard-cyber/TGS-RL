import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  fullyParallel: false,
  retries: 0,
  reporter: 'line',
  use: {
    baseURL: process.env.TGSRL_BROWSER_BASE_URL ?? 'http://127.0.0.1:4173',
    trace: 'retain-on-failure',
  },
});
