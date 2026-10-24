import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const BIN = fileURLToPath(new URL('../../bin/node.js', import.meta.url));
const COLORS = ['\x1b[36m', '\x1b[33m', '\x1b[35m', '\x1b[32m', '\x1b[34m', '\x1b[96m', '\x1b[93m', '\x1b[95m'];
const RESET = '\x1b[0m';

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Spawns and controls N local node processes (each a real OS process). */
export class ClusterManager {
  constructor({ size = 5, baseGossipPort = 7000, baseHttpPort = 8000, env = {}, onLine = null, seedCount = 2 } = {}) {
    this.size = size;
    this.baseGossipPort = baseGossipPort;
    this.baseHttpPort = baseHttpPort;
    this.env = env;
    this.onLine = onLine ?? ((i, line) => process.stdout.write(`${COLORS[(i - 1) % COLORS.length]}${line}${RESET}\n`));
    this.seeds = Array.from({ length: Math.min(seedCount, size) }, (_, k) => `127.0.0.1:${baseGossipPort + k + 1}`).join(',');
    this.procs = new Map(); // index -> ChildProcess
    this.paused = new Set();
  }

  info(i) {
    return { index: i, id: `node-${i}`, gossipPort: this.baseGossipPort + i, httpPort: this.baseHttpPort + i };
  }

  indexes() {
    return Array.from({ length: this.size }, (_, k) => k + 1);
  }

  isRunning(i) {
    return this.procs.has(i);
  }

  start(i) {
    if (this.procs.has(i)) return this.procs.get(i);
    const { id, gossipPort, httpPort } = this.info(i);
    const child = spawn(
      process.execPath,
      [BIN, '--id', id, '--port', String(gossipPort), '--http-port', String(httpPort), '--seeds', this.seeds],
      { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, FORCE_COLOR: '0', ...this.env } },
    );
    const pipe = (stream) => {
      let buf = '';
      stream.on('data', (d) => {
        buf += d;
        let nl;
        while ((nl = buf.indexOf('\n')) >= 0) {
          this.onLine(i, buf.slice(0, nl));
          buf = buf.slice(nl + 1);
        }
      });
    };
    pipe(child.stdout);
    pipe(child.stderr);
    child.on('exit', () => {
      this.procs.delete(i);
      this.paused.delete(i);
    });
    this.procs.set(i, child);
    return child;
  }

  startAll() {
    for (const i of this.indexes()) this.start(i);
  }

  /** SIGKILL = crash, SIGTERM = graceful leave, SIGSTOP/SIGCONT = hang/resume. */
  signal(i, sig) {
    const child = this.procs.get(i);
    if (!child) return false;
    child.kill(sig);
    if (sig === 'SIGSTOP') this.paused.add(i);
    if (sig === 'SIGCONT') this.paused.delete(i);
    return true;
  }

  async waitExit(i, timeoutMs = 5000) {
    const child = this.procs.get(i);
    if (!child) return;
    await Promise.race([new Promise((r) => child.once('exit', r)), sleep(timeoutMs)]);
  }

  async stopAll() {
    for (const i of this.paused) this.signal(i, 'SIGCONT');
    const waits = [...this.procs.keys()].map((i) => {
      this.signal(i, 'SIGTERM');
      return this.waitExit(i, 3000);
    });
    await Promise.all(waits);
    for (const i of [...this.procs.keys()]) this.signal(i, 'SIGKILL');
  }

  /** This node's membership view, or null if it is unreachable. */
  async view(i) {
    if (!this.procs.has(i) || this.paused.has(i)) return null;
    try {
      const res = await fetch(`http://127.0.0.1:${this.info(i).httpPort}/members`, { signal: AbortSignal.timeout(500) });
      return await res.json();
    } catch {
      return null;
    }
  }

  async views() {
    const out = new Map();
    await Promise.all(this.indexes().map(async (i) => out.set(i, await this.view(i))));
    return out;
  }

  /** Status of `targetId` as seen by observer `i` (or undefined). */
  static statusIn(view, targetId) {
    return view?.members.find((m) => m.id === targetId)?.status;
  }
}

const CELL = {
  alive: '\x1b[32m  alive  \x1b[0m',
  suspect: '\x1b[33m SUSPECT \x1b[0m',
  dead: '\x1b[31m  DEAD   \x1b[0m',
  left: '\x1b[90m  left   \x1b[0m',
  undefined: '\x1b[90m    -    \x1b[0m',
};

/** Render an observer x member matrix: row = "what node X thinks of everyone". */
export function renderMatrix(cluster, views) {
  const ids = cluster.indexes().map((i) => cluster.info(i).id);
  const lines = [];
  lines.push(`${'observer \\ member'.padEnd(18)}${ids.map((id) => id.padStart(7).padEnd(9)).join('')}`);
  for (const i of cluster.indexes()) {
    const v = views.get(i);
    const label = cluster.info(i).id + (v ? '' : cluster.paused.has(i) ? ' (paused)' : ' (down)');
    const cells = v ? ids.map((id) => CELL[ClusterManager.statusIn(v, id)]) : ids.map(() => '         ');
    lines.push(`${label.padEnd(18)}${cells.join('')}`);
  }
  return lines.join('\n');
}
