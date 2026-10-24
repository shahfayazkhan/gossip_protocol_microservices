import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { GossipNode } from '../src/gossip-node.js';
import { Notifier } from '../src/notifier.js';

// Fast timings so the integration test runs in a few seconds.
const FAST = {
  gossipIntervalMs: 100,
  failureCheckIntervalMs: 50,
  suspectAfterMs: 400,
  probeTimeoutMs: 100,
  deadAfterMs: 800,
};
const BASE = 9200;
const SEEDS = `127.0.0.1:${BASE + 1},127.0.0.1:${BASE + 2}`;

const nodes = new Map();
const notifiers = new Map();

function makeNode(i, extra = {}) {
  const node = new GossipNode({ id: `node-${i}`, port: BASE + i, seeds: SEEDS, ...FAST, ...extra });
  nodes.set(i, node);
  notifiers.set(i, new Notifier(node));
  return node;
}

async function waitFor(fn, timeoutMs = 5000, label = 'condition') {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (fn()) return Date.now() - t0;
    await new Promise((r) => setTimeout(r, 25));
  }
  assert.fail(`timed out waiting for ${label}`);
}

const statusOf = (observer, id) => nodes.get(observer).membership.get(id)?.status;
const survivors = (...ex) => [...nodes.keys()].filter((i) => !ex.includes(i));

before(async () => {
  for (let i = 1; i <= 5; i++) await makeNode(i).start();
});

after(async () => {
  for (const n of nodes.values()) await n.stop();
});

test('5 nodes converge: everyone sees everyone alive', async () => {
  await waitFor(
    () => [1, 2, 3, 4, 5].every((o) => [1, 2, 3, 4, 5].every((t) => statusOf(o, `node-${t}`) === 'alive')),
    5000,
    'convergence',
  );
});

test('a crashed node is detected as DEAD by every survivor and alerted once', async () => {
  const deadEvents = [];
  for (const i of survivors(3)) nodes.get(i).on('member:dead', (e) => e.id === 'node-3' && deadEvents.push(e));

  await nodes.get(3).crash();
  const ms = await waitFor(() => survivors(3).every((o) => statusOf(o, 'node-3') === 'dead'), 6000, 'node-3 dead');

  assert.equal(new Set(deadEvents.map((e) => e.observer)).size, 4, 'every survivor emitted member:dead');
  assert.ok(deadEvents.some((e) => e.source === 'local'), 'at least one node detected it first-hand');

  const alerts = survivors(3).flatMap((i) => notifiers.get(i).alerts).filter((a) => a.event.id === 'node-3' && a.event.type === 'dead');
  assert.equal(alerts.length, 1, 'exactly one reporter alert');
  assert.equal(alerts[0].reporter, 'node-1');
  console.log(`    node-3 declared dead cluster-wide in ${ms}ms`);
});

test('a restarted node rejoins with a newer incarnation', async () => {
  const oldInc = nodes.get(1).membership.get('node-3').incarnation;
  await makeNode(3).start();
  await waitFor(() => survivors(3).every((o) => statusOf(o, 'node-3') === 'alive'), 4000, 'node-3 rejoin');
  assert.ok(nodes.get(1).membership.get('node-3').incarnation > oldInc);
});

test('a false suspicion is refuted by the accused node', async () => {
  const before = nodes.get(2).membership.self.incarnation;
  nodes.get(1).membership.suspect('node-2', 'test: injected false positive');
  assert.equal(statusOf(1, 'node-2'), 'suspect');
  await waitFor(() => statusOf(1, 'node-2') === 'alive', 2000, 'refutation');
  assert.ok(nodes.get(2).membership.self.incarnation > before);
  assert.ok(survivors().every((o) => statusOf(o, 'node-2') !== 'dead'));
});

test('a graceful leave is propagated quickly as LEFT (no timeouts)', async () => {
  await nodes.get(5).leave();
  const ms = await waitFor(() => survivors(5).every((o) => statusOf(o, 'node-5') === 'left'), 1000, 'node-5 left');
  console.log(`    node-5 LEFT seen cluster-wide in ${ms}ms`);
});

test('the cluster still works with 20% packet loss', async () => {
  for (const i of [1, 2, 3, 4]) nodes.get(i).transport.lossRate = 0.2;
  await nodes.get(4).crash();
  await waitFor(() => [1, 2, 3].every((o) => statusOf(o, 'node-4') === 'dead'), 8000, 'node-4 dead under loss');
  assert.ok([1, 2, 3].every((o) => [1, 2, 3].every((t) => statusOf(o, `node-${t}`) === 'alive')), 'no false positives');
});
