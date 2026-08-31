import { createReadStream, statSync } from 'node:fs';
import { createServer, request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { extname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const host = process.env.HOST || '127.0.0.1';
const port = Number.parseInt(process.env.PORT || '8080', 10);
const gateway = new URL(process.env.TGSRL_GATEWAY_URL || 'http://127.0.0.1:8081');
const root = resolve(fileURLToPath(new URL('./dist/', import.meta.url)));
const indexPath = resolve(root, 'index.html');
const hopByHopHeaders = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);
const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.ico', 'image/x-icon'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
  ['.txt', 'text/plain; charset=utf-8'],
  ['.webmanifest', 'application/manifest+json'],
]);

if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error(`invalid PORT: ${process.env.PORT}`);
}
if (!['http:', 'https:'].includes(gateway.protocol)) {
  throw new Error('TGSRL_GATEWAY_URL must use http or https');
}

function proxy(request, response, url) {
  const target = new URL(`${url.pathname}${url.search}`, gateway);
  const headers = Object.fromEntries(
    Object.entries(request.headers).filter(([name]) => !hopByHopHeaders.has(name.toLowerCase())),
  );
  headers.host = gateway.host;
  const send = gateway.protocol === 'https:' ? httpsRequest : httpRequest;
  const upstream = send(
    target,
    { method: request.method, headers },
    (upstreamResponse) => {
      const responseHeaders = Object.fromEntries(
        Object.entries(upstreamResponse.headers).filter(
          ([name]) => !hopByHopHeaders.has(name.toLowerCase()),
        ),
      );
      response.writeHead(upstreamResponse.statusCode || 502, responseHeaders);
      upstreamResponse.pipe(response);
    },
  );
  upstream.setTimeout(30_000, () => upstream.destroy(new Error('gateway timeout')));
  upstream.on('error', () => {
    if (response.headersSent) {
      response.destroy();
      return;
    }
    response.writeHead(502, { 'content-type': 'application/json; charset=utf-8' });
    response.end(JSON.stringify({ error: 'gateway unavailable' }));
  });
  request.pipe(upstream);
}

function staticPath(pathname) {
  let decoded;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return null;
  }
  const candidate = resolve(root, `.${decoded}`);
  if (candidate !== root && !candidate.startsWith(`${root}${sep}`)) {
    return null;
  }
  try {
    return statSync(candidate).isFile() ? candidate : indexPath;
  } catch {
    return indexPath;
  }
}

const server = createServer((request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  if (url.pathname === '/healthz') {
    response.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    response.end(JSON.stringify({ status: 'ok' }));
    return;
  }
  if (url.pathname === '/health' || url.pathname.startsWith('/v1/')) {
    proxy(request, response, url);
    return;
  }
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    response.writeHead(405, { allow: 'GET, HEAD' });
    response.end();
    return;
  }
  const file = staticPath(url.pathname);
  if (!file) {
    response.writeHead(400);
    response.end();
    return;
  }
  response.writeHead(200, {
    'cache-control': file === indexPath ? 'no-cache' : 'public, max-age=31536000, immutable',
    'content-type': contentTypes.get(extname(file)) || 'application/octet-stream',
    'x-content-type-options': 'nosniff',
    'x-frame-options': 'DENY',
  });
  if (request.method === 'HEAD') {
    response.end();
    return;
  }
  const stream = createReadStream(file);
  stream.on('error', () => response.destroy());
  stream.pipe(response);
});

server.listen(port, host, () => {
  process.stdout.write(`console listening on http://${host}:${port}\n`);
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
