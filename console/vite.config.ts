import { configDefaults, defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 4173,
    // The Docker-only smoke runner reaches the dev server through the Compose
    // service DNS name. Keep the allowlist explicit instead of accepting every
    // Host header.
    allowedHosts: ['console'],
    proxy: {
      '/v1': process.env.VITE_TGSRL_GATEWAY_TARGET ?? 'http://127.0.0.1:8080',
      '/health': process.env.VITE_TGSRL_GATEWAY_TARGET ?? 'http://127.0.0.1:8080',
      '/openapi.json': process.env.VITE_TGSRL_GATEWAY_TARGET ?? 'http://127.0.0.1:8080',
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
    exclude: [...configDefaults.exclude, 'tests/browser/**'],
    css: true,
  },
});
