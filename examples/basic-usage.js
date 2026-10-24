/**
 * Using the library directly: 5 nodes inside ONE process, reacting to events.
 *
 *   node examples/basic-usage.js
 *
 * Shows how you would wire the gossip layer into your own service: listen
 * for member:dead to stop routing traffic, member:rejoin to add it back, etc.
 */
import { GossipNode, createLogger } from '../src/index.js';

const BASE = 9300;
const seeds = [`127.0.0.1:${BASE + 1}`];
const fast = { gossipIntervalMs: 300, suspectAfterMs: 1000, deadAfterMs: 1500, probeTimeoutMs: 200, failureCheckIntervalMs: 150 };

const nodes = [];
for (let i = 1; i <= 5; i++) {
  const node = new GossipNode({ id: `node-${i}`, port: BASE + i, seeds, ...fast, logger: createLogger(`node-${i}`, 'warn') });
  nodes.push(node);
}

// Your application logic: e.g. a load balancer living on node-1.
const lb = nodes[0];
const upstreams = new Set();
lb.on('member:join', (e) => upstreams.add(e.id));
lb.on('member:rejoin', (e) => upstreams.add(e.id));
lb.on('member:dead', (e) => {
  upstreams.delete(e.id);
  console.log(`\n>>> ${e.id} is DEAD (${e.source === 'local' ? 'I probed it' : 'told by ' + e.via}). Upstreams now: ${[...upstreams].join(', ')}`);
});
lb.on('member:left', (e) => upstreams.delete(e.id));

for (const n of nodes) await n.start();

setTimeout(() => console.log(`converged, node-1 routes to: ${[...upstreams].join(', ')}`), 1500);
setTimeout(() => {
  console.log('\n>>> crashing node-4 ...');
  nodes[3].crash();
}, 2500);
setTimeout(async () => {
  console.log('\nfinal view from node-2:');
  console.table(nodes[1].membership.snapshot().map(({ id, status, incarnation, heartbeat }) => ({ id, status, incarnation, heartbeat })));
  for (const n of nodes) await n.stop();
}, 7000);
