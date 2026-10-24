import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Membership, Status } from '../src/membership.js';

const entry = (over = {}) => ({
  id: 'b',
  host: '127.0.0.1',
  port: 7002,
  httpPort: 8002,
  meta: {},
  incarnation: 100,
  heartbeat: 1,
  status: Status.ALIVE,
  ...over,
});

function setup(opts) {
  let t = 1_000;
  const now = () => t;
  const m = new Membership({ id: 'a', host: '127.0.0.1', port: 7001 }, { now, ...opts });
  const events = [];
  m.on('change', (e) => events.push(e));
  return { m, events, advance: (ms) => (t += ms) };
}

test('learns a new member from gossip and emits join', () => {
  const { m, events } = setup();
  m.merge([entry()], 'c');
  assert.equal(m.get('b').status, Status.ALIVE);
  assert.deepEqual(events.map((e) => e.type), ['join']);
  assert.equal(events[0].via, 'c');
});

test('higher heartbeat refreshes lastSeen, lower is ignored', () => {
  const { m, advance } = setup();
  m.apply(entry({ heartbeat: 5 }));
  advance(500);
  m.apply(entry({ heartbeat: 3 }));
  assert.equal(m.get('b').heartbeat, 5);
  assert.equal(m.get('b').lastSeen, 1000);
  m.apply(entry({ heartbeat: 6 }));
  assert.equal(m.get('b').lastSeen, 1500);
});

test('suspect/dead override alive at the same incarnation', () => {
  const { m, events } = setup();
  m.apply(entry());
  m.apply(entry({ status: Status.SUSPECT }));
  m.apply(entry({ status: Status.ALIVE, heartbeat: 9 })); // cannot undo suspicion
  assert.equal(m.get('b').status, Status.SUSPECT);
  m.apply(entry({ status: Status.DEAD }));
  assert.equal(m.get('b').status, Status.DEAD);
  assert.deepEqual(events.map((e) => e.type), ['join', 'suspect', 'dead']);
});

test('higher incarnation overrides suspicion (alive) and death (rejoin)', () => {
  const { m, events } = setup();
  m.apply(entry());
  m.apply(entry({ status: Status.SUSPECT }));
  m.apply(entry({ incarnation: 101 }));
  assert.equal(m.get('b').status, Status.ALIVE);
  m.apply(entry({ incarnation: 101, status: Status.DEAD }));
  m.apply(entry({ incarnation: 999, heartbeat: 0 })); // restarted process
  assert.equal(m.get('b').status, Status.ALIVE);
  assert.deepEqual(events.map((e) => e.type), ['join', 'suspect', 'alive', 'dead', 'rejoin']);
});

test('stale (lower incarnation) information is ignored', () => {
  const { m } = setup();
  m.apply(entry({ incarnation: 200 }));
  m.apply(entry({ incarnation: 150, status: Status.DEAD }));
  assert.equal(m.get('b').status, Status.ALIVE);
});

test('a node refutes rumours about itself by bumping its incarnation', () => {
  const { m } = setup();
  const refutes = [];
  m.on('refute', (e) => refutes.push(e));
  const inc = m.self.incarnation;
  m.apply({ ...entry(), id: 'a', port: 7001, incarnation: inc, status: Status.SUSPECT }, 'b');
  assert.equal(m.self.incarnation, inc + 1);
  assert.equal(m.self.status, Status.ALIVE);
  assert.equal(refutes[0].accusedBy, 'b');
});

test('local detector: alive -> suspect -> dead, with source=local', () => {
  const { m, events } = setup();
  m.apply(entry());
  assert.equal(m.declareDead('b'), false, 'must be suspect first');
  assert.equal(m.suspect('b', 'probe failed'), true);
  assert.equal(m.declareDead('b', 'timeout'), true);
  assert.deepEqual(events.slice(1).map((e) => [e.type, e.source]), [['suspect', 'local'], ['dead', 'local']]);
});

test('left beats dead at the same incarnation', () => {
  const { m } = setup();
  m.apply(entry({ status: Status.DEAD }));
  m.apply(entry({ status: Status.LEFT }));
  assert.equal(m.get('b').status, Status.LEFT);
});

test('cleanup tombstones dead nodes; only a newer incarnation can resurrect them', () => {
  const { m, advance } = setup({ cleanupAfterMs: 1000 });
  m.apply(entry());
  m.apply(entry({ status: Status.DEAD }));
  advance(1001);
  m.cleanup();
  assert.equal(m.get('b'), undefined);
  m.apply(entry({ status: Status.DEAD }));
  m.apply(entry({ heartbeat: 50 })); // same incarnation, stale gossip
  assert.equal(m.get('b'), undefined);
  m.apply(entry({ incarnation: 101 }));
  assert.equal(m.get('b').status, Status.ALIVE);
});

test('reporter is the lowest alive id (numeric aware)', () => {
  const { m } = setup();
  m.apply(entry({ id: 'node-10' }));
  m.apply(entry({ id: 'node-2' }));
  const m2 = new Membership({ id: 'node-3', host: 'h', port: 1 });
  m2.apply(entry({ id: 'node-10' }));
  m2.apply(entry({ id: 'node-2' }));
  assert.equal(m2.reporterId(), 'node-2');
  m2.apply(entry({ id: 'node-2', status: Status.DEAD }));
  assert.equal(m2.reporterId(), 'node-3');
});
