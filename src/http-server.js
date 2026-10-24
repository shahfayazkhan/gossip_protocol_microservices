import http from 'node:http';
import { readFile } from 'node:fs/promises';

const DASHBOARD = new URL('../public/dashboard.html', import.meta.url);

/**
 * Per-node HTTP API (the gossip itself runs over UDP; this is only for
 * observing and controlling the node).
 *
 *   GET  /              dashboard (cluster-wide matrix of every node's view)
 *   GET  /health        liveness of this process
 *   GET  /members       this node's membership table
 *   GET  /history       membership change events seen by this node
 *   GET  /alerts        alerts this node sent as reporter
 *   GET  /events        Server-Sent Events stream of membership changes
 *   POST /admin/leave   graceful shutdown (SIGTERM)
 *   POST /admin/crash   hard crash (SIGKILL) — for demos
 */
export function startHttpServer(node, notifier, { port, host = '0.0.0.0', allowAdmin = true, logger }) {
  const clients = new Set();

  node.on('member:change', (e) => {
    const frame = `event: change\ndata: ${JSON.stringify(e)}\n\n`;
    for (const res of clients) res.write(frame);
  });

  const json = (res, code, body) => {
    res.writeHead(code, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body, null, 2));
  };

  const server = http.createServer(async (req, res) => {
    res.setHeader('access-control-allow-origin', '*');
    res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS');
    const { pathname } = new URL(req.url, 'http://localhost');

    try {
      if (req.method === 'OPTIONS') return res.writeHead(204).end();

      if (req.method === 'GET') {
        switch (pathname) {
          case '/':
          case '/dashboard': {
            const html = await readFile(DASHBOARD);
            res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
            return res.end(html);
          }
          case '/health':
            return json(res, 200, { id: node.id, status: 'ok', uptimeMs: Date.now() - node.startedAt });
          case '/members':
            return json(res, 200, node.snapshot());
          case '/history':
            return json(res, 200, notifier.history);
          case '/alerts':
            return json(res, 200, notifier.alerts);
          case '/events': {
            res.writeHead(200, {
              'content-type': 'text/event-stream',
              'cache-control': 'no-cache',
              connection: 'keep-alive',
            });
            res.write(`: connected to ${node.id}\n\n`);
            for (const e of notifier.history.slice(-20)) res.write(`event: change\ndata: ${JSON.stringify(e)}\n\n`);
            clients.add(res);
            const keepAlive = setInterval(() => res.write(': ping\n\n'), 15000);
            req.on('close', () => {
              clearInterval(keepAlive);
              clients.delete(res);
            });
            return;
          }
        }
      }

      if (req.method === 'POST' && allowAdmin) {
        if (pathname === '/admin/leave') {
          json(res, 202, { ok: true, action: 'leave', id: node.id });
          setTimeout(() => process.kill(process.pid, 'SIGTERM'), 50);
          return;
        }
        if (pathname === '/admin/crash') {
          json(res, 202, { ok: true, action: 'crash', id: node.id });
          setTimeout(() => process.kill(process.pid, 'SIGKILL'), 50);
          return;
        }
      }

      json(res, 404, { error: 'not found' });
    } catch (err) {
      logger?.error('http error:', err.message);
      if (!res.headersSent) json(res, 500, { error: err.message });
    }
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => resolve(server));
  });
}
