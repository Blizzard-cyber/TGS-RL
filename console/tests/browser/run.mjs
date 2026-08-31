import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { resolve } from 'node:path';

async function availablePort() {
  const listener = createServer();
  await new Promise((resolveListen, reject) => {
    listener.once('error', reject);
    listener.listen(0, '127.0.0.1', resolveListen);
  });
  const address = listener.address();
  if (!address || typeof address === 'string') throw new Error('cannot allocate browser port');
  await new Promise((resolveClose) => listener.close(resolveClose));
  return address.port;
}

async function waitForServer(url, process) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (process.exitCode !== null) throw new Error(`Vite exited with ${process.exitCode}`);
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch {
      // The server has not bound its socket yet.
    }
    await new Promise((resolveWait) => setTimeout(resolveWait, 100));
  }
  throw new Error('timed out waiting for the browser smoke server');
}

function run(command, args, environment) {
  return spawn(command, args, { cwd: process.cwd(), env: environment, stdio: 'inherit' });
}

const port = await availablePort();
const baseURL = `http://127.0.0.1:${port}`;
const environment = {
  ...process.env,
  TGSRL_BROWSER_BASE_URL: baseURL,
  VITE_TGSRL_API_ADAPTER: 'mock',
};
const vite = run(
  process.execPath,
  [resolve('node_modules/vite/bin/vite.js'), '--host', '127.0.0.1', '--port', String(port), '--strictPort'],
  environment,
);

let exitCode = 1;
try {
  await waitForServer(baseURL, vite);
  const playwright = run(
    process.execPath,
    [resolve('node_modules/@playwright/test/cli.js'), 'test'],
    environment,
  );
  exitCode = await new Promise((resolveExit) => playwright.once('exit', (code) => resolveExit(code ?? 1)));
} finally {
  vite.kill('SIGTERM');
  await Promise.race([
    new Promise((resolveExit) => vite.once('exit', resolveExit)),
    new Promise((resolveWait) => setTimeout(resolveWait, 5_000)),
  ]);
  if (vite.exitCode === null) vite.kill('SIGKILL');
}

process.exitCode = exitCode;
