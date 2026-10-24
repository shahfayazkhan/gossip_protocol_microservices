#!/usr/bin/env node
/**
 * Automated end-to-end demo with 5 real processes. Measures how long the
 * cluster takes to notice each failure.
 *
 *   npm run demo              # narrated output
 *   npm run demo -- --verbose # also show every node's log
 *
 * Scenarios:
 *   1. boot 5 nodes, wait for convergence
 *   2. crash node-3 (SIGKILL)        -> SUSPECT -> DEAD on every survivor + one ALERT
 *   3. restart node-3                -> REJOIN (new incarnation beats the dead record)
 *   4. freeze node-4 (SIGSTOP), then resume -> DEAD, then refutes -> REJOIN
 *   5. graceful stop node-5 (SIGTERM)-> LEFT almost instantly (no timeouts needed)
 */
import { ClusterManager, renderMatrix, sleep } from './lib/cluster.js';

const verbose = process.argv.includes('--verbose');
const TIMING = {
  GOSSIP_INTERVAL_MS: '500',
  FAILURE_CHECK_INTERVAL_MS: '250',
  SUSPECT_AFTER_MS: '1500',
  PROBE_TIMEOUT_MS: '300',
  DEAD_AFTER_MS: '2000',
};

const B = (s) => `\x1b[1m${s}\x1b[0m`;
const step = (s) => console.log(`\n${B('\x1b[36m== ' + s + '\x1b[0m')}`);

const cluster = new ClusterManager({
  size: 5,
  baseGossipPort: 7100,
  baseHttpPort: 8100,
  env: { ...TIMING, LOG_LEVEL: 'info' },
  onLine: (i, line) => {
    if (verbose || /\[(ALERT|REFUTE)\]/.test(line)) console.log(`   node-${i} | ${line.replace(/^\S+ \S+\s+/, '')}`);
  },
});

const statusIn = ClusterManager.statusIn;

/** Poll observers until each sees targetId in one of `want` statuses; returns ms per observer. */
async function measure(targetId, want, observers, timeoutMs = 20000, track = {}) {
  const t0 = Date.now();
  const done = new Map();
  while (done.size < observers.length && Date.now() - t0 < timeoutMs) {
    await Promise.all(
      observers
        .filter((i) => !done.has(i))
        .map(async (i) => {
          const s = statusIn(await cluster.view(i), targetId);
          if (track[s] && !track[s].has(i)) track[s].set(i, Date.now() - t0);
          if (want.includes(s)) done.set(i, Date.now() - t0);
        }),
    );
    await sleep(100);
  }
  return done;
}

function report(label, observers, results, extra = {}) {
  for (const i of observers) {
    const ms = results.get(i);
    const parts = Object.entries(extra)
      .map(([k, m]) => (m.has(i) ? `${k} after ${m.get(i)}ms` : null))
      .filter(Boolean);
    console.log(
      `   node-${i}: ${ms === undefined ? '\x1b[31mTIMEOUT\x1b[0m' : `${label} after ${B(ms + 'ms')}`}${parts.length ? '  (' + parts.join(', ') + ')' : ''}`,
    );
  }
}

const others = (...exclude) => cluster.indexes().filter((i) => !exclude.includes(i));
const matrix = async () => console.log('\n' + renderMatrix(cluster, await cluster.views()));

async function main() {
  console.log(B('Gossip protocol failure-detection demo'));
  console.log(`timings: gossip ${TIMING.GOSSIP_INTERVAL_MS}ms, suspect after ${TIMING.SUSPECT_AFTER_MS}ms silence, dead after ${TIMING.DEAD_AFTER_MS}ms suspicion`);

  step('1. Booting 5 nodes (seeds: node-1, node-2)');
  const t0 = Date.now();
  cluster.startAll();
  const all = cluster.indexes();
  for (;;) {
    const views = await cluster.views();
    const ok = all.every((i) => all.every((j) => statusIn(views.get(i), `node-${j}`) === 'alive'));
    if (ok) break;
    if (Date.now() - t0 > 20000) throw new Error('cluster did not converge');
    await sleep(100);
  }
  console.log(`   all 5 nodes see each other as alive after ${B(Date.now() - t0 + 'ms')}`);
  await matrix();

  step('2. CRASH node-3 (SIGKILL, no goodbye)');
  cluster.signal(3, 'SIGKILL');
  const suspectSeen = new Map();
  const dead3 = await measure('node-3', ['dead'], others(3), 20000, { suspect: suspectSeen });
  report('DEAD', others(3), dead3, { SUSPECT: suspectSeen });
  await matrix();

  step('3. Restart node-3');
  cluster.start(3);
  const back3 = await measure('node-3', ['alive'], others(3));
  report('sees node-3 alive again', others(3), back3);

  step('4. FREEZE node-4 (SIGSTOP) - process exists but stops responding');
  const before = (await cluster.view(1)).members.find((m) => m.id === 'node-4').incarnation;
  cluster.signal(4, 'SIGSTOP');
  const dead4 = await measure('node-4', ['dead'], others(4));
  report('DEAD', others(4), dead4);
  console.log('   resuming node-4 (SIGCONT) - it will learn it was declared dead and refute');
  cluster.signal(4, 'SIGCONT');
  const back4 = await measure('node-4', ['alive'], others(4));
  report('sees node-4 alive again', others(4), back4);
  const after = (await cluster.view(1)).members.find((m) => m.id === 'node-4').incarnation;
  console.log(`   node-4 incarnation ${before} -> ${B(after)} (refutation)`);

  step('5. Graceful stop node-5 (SIGTERM -> broadcasts LEFT)');
  cluster.signal(5, 'SIGTERM');
  const left5 = await measure('node-5', ['left'], others(5));
  report('LEFT', others(5), left5);
  await matrix();

  step('Alerts sent by the reporter node (one per incident, not one per node)');
  for (const i of others(5)) {
    try {
      const alerts = await (await fetch(`http://127.0.0.1:${cluster.info(i).httpPort}/alerts`)).json();
      for (const a of alerts) console.log(`   ${a.at}  ${a.text}`);
    } catch {}
  }
}

try {
  await main();
} catch (err) {
  console.error('\ndemo failed:', err.message);
  process.exitCode = 1;
} finally {
  step('Stopping cluster');
  await cluster.stopAll();
}
