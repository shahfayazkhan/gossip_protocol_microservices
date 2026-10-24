import { EventEmitter } from 'node:events';
import { Membership, Status } from './membership.js';
import { UdpTransport } from './transport.js';
import { defaults } from './config.js';
import { silentLogger } from './logger.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Fisher–Yates sample of `k` random items. */
function sample(arr, k) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, k);
}

/** Resolves true as soon as any promise resolves true, false if all resolve false. */
function anyTrue(promises) {
  return new Promise((resolve) => {
    let left = promises.length;
    if (!left) return resolve(false);
    for (const p of promises) {
      p.then((v) => {
        if (v) resolve(true);
        else if (--left === 0) resolve(false);
      });
    }
  });
}

export function parseSeeds(seeds) {
  const list = Array.isArray(seeds) ? seeds : String(seeds || '').split(',');
  return list
    .map((s) => (typeof s === 'string' ? s.trim() : s))
    .filter(Boolean)
    .map((s) => {
      if (typeof s === 'object') return s;
      const idx = s.lastIndexOf(':');
      return { host: s.slice(0, idx), port: Number(s.slice(idx + 1)) };
    })
    .filter((s) => s.host && Number.isInteger(s.port));
}

/**
 * A cluster member running:
 *  1. Gossip dissemination  — every round, push-pull the membership table with
 *                             `fanout` random peers (epidemic spread, O(log N) rounds).
 *  2. Heartbeats            — own counter increments each round and travels with gossip.
 *  3. SWIM failure detector — stale heartbeat -> direct PING -> k indirect PING-REQ
 *                             -> SUSPECT -> (no refutation) -> DEAD.
 *
 * Wire messages (UDP/JSON, every message piggybacks the membership digest):
 *   syn / syn-ack   push-pull gossip exchange
 *   ping / ack      direct probe
 *   ping-req        "please ping <target> for me" (indirect probe)
 *   leave           graceful departure announcement
 *
 * Events: 'member:change' (all), and 'member:<type>' for join|suspect|dead|left|alive|rejoin
 */
export class GossipNode extends EventEmitter {
  constructor(options) {
    super();
    this.opts = { ...defaults, ...options };
    const { id, host = '127.0.0.1', port, httpPort = null, meta = {} } = this.opts;
    if (!id || !port) throw new Error('GossipNode requires { id, port }');

    this.id = id;
    this.host = host;
    this.port = port;
    this.log = this.opts.logger ?? silentLogger;
    this.seeds = parseSeeds(this.opts.seeds).filter(
      (s) => !(s.port === port && ['127.0.0.1', 'localhost', host].includes(s.host)),
    );

    this.membership = new Membership({ id, host, port, httpPort, meta }, { cleanupAfterMs: this.opts.cleanupAfterMs });
    this.membership.on('change', (e) => {
      this._logChange(e);
      this.emit('member:change', e);
      this.emit(`member:${e.type}`, e);
    });
    this.membership.on('refute', (e) => {
      this.log.warn(`[REFUTE] ${e.accusedBy ?? 'a peer'} says I am ${e.claimed}; bumping incarnation -> ${e.incarnation}`);
      this.emit('refute', e);
    });
    this.membership.on('removed', (e) => this.log.debug(`removed ${e.id} from table (tombstoned)`));

    this.transport = new UdpTransport({
      bindHost: this.opts.bindHost ?? '0.0.0.0',
      port,
      lossRate: this.opts.lossRate,
      logger: this.log,
    });
    this.transport.on('message', (msg, rinfo) => this._onMessage(msg, rinfo));

    this.seq = 0;
    this.pending = new Map(); // seq -> resolver
    this.probing = new Set(); // ids currently being probed
    this.timers = [];
    this.running = false;
    this.startedAt = null;
    this.stats = { sent: 0, received: 0, gossipRounds: 0, probes: 0, indirectProbes: 0 };
  }

  // ---------------------------------------------------------------- lifecycle

  async start() {
    await this.transport.start();
    this.running = true;
    this.startedAt = Date.now();
    this.timers.push(setInterval(() => this._gossipRound(), this.opts.gossipIntervalMs));
    this.timers.push(setInterval(() => this._detectFailures(), this.opts.failureCheckIntervalMs));
    this._contactSeeds();
    this.log.info(
      `started on udp ${this.host}:${this.port} (incarnation ${this.membership.self.incarnation}), seeds: ${
        this.seeds.map((s) => `${s.host}:${s.port}`).join(', ') || 'none'
      }`,
    );
    return this;
  }

  /** Graceful shutdown: tell peers we are leaving so they don't wait for timeouts. */
  async leave() {
    if (!this.running) return;
    this.membership.leave();
    const peers = this.membership.peers(Status.ALIVE, Status.SUSPECT);
    for (let i = 0; i < 2; i++) {
      for (const p of peers) this._send('leave', p.host, p.port);
      await sleep(50);
    }
    this.log.info('left the cluster gracefully');
    await this.stop();
  }

  /** Stop without telling anyone (used by tests to simulate a crash). */
  async stop() {
    if (!this.running) return;
    this.running = false;
    this.timers.forEach(clearInterval);
    this.timers = [];
    for (const resolve of this.pending.values()) resolve(false);
    this.pending.clear();
    await this.transport.stop();
  }

  crash() {
    return this.stop();
  }

  // ------------------------------------------------------------------ gossip

  _send(type, host, port, extra = {}) {
    this.stats.sent++;
    this.transport.send({ type, from: this.id, ...extra, members: this.membership.digest() }, host, port);
  }

  _contactSeeds() {
    for (const s of this.seeds) this._send('syn', s.host, s.port);
  }

  _gossipRound() {
    if (!this.running) return;
    this.stats.gossipRounds++;
    this.membership.beat();

    const live = this.membership.peers(Status.ALIVE, Status.SUSPECT);
    if (live.length === 0) {
      this._contactSeeds(); // isolated (or first boot): keep knocking on the seeds
      return;
    }
    for (const p of sample(live, this.opts.fanout)) this._send('syn', p.host, p.port);

    // Occasionally gossip to a DEAD node: heals partitions and lets a
    // recovered node learn it was declared dead (so it can refute).
    const dead = this.membership.peers(Status.DEAD);
    if (dead.length && Math.random() < this.opts.deadGossipProbability) {
      const [d] = sample(dead, 1);
      this._send('syn', d.host, d.port);
    }
  }

  _onMessage(msg, rinfo) {
    if (!this.running || !msg || typeof msg.type !== 'string' || msg.from === this.id) return;
    this.stats.received++;
    this.membership.merge(msg.members, msg.from);
    this.membership.heard(msg.from);

    switch (msg.type) {
      case 'syn':
        this._send('syn-ack', rinfo.address, rinfo.port); // push-pull: reply with our view
        break;
      case 'ping':
        this._send('ack', rinfo.address, rinfo.port, { seq: msg.seq });
        break;
      case 'ack':
        this.pending.get(msg.seq)?.(true);
        break;
      case 'ping-req':
        this._handlePingReq(msg, rinfo);
        break;
      case 'syn-ack':
      case 'leave':
        break; // digest already merged
      default:
        this.log.debug(`unknown message type ${msg.type}`);
    }
  }

  // -------------------------------------------------------- failure detection

  _detectFailures() {
    if (!this.running) return;
    const now = Date.now();
    for (const m of this.membership.list()) {
      if (m.id === this.id) continue;
      if (m.status === Status.ALIVE && now - m.lastSeen > this.opts.suspectAfterMs) {
        this._probe(m);
      } else if (m.status === Status.SUSPECT && now - m.statusChangedAt > this.opts.deadAfterMs) {
        this.membership.declareDead(m.id, `suspect for ${this.opts.deadAfterMs}ms with no refutation`);
      }
    }
    this.membership.cleanup();
  }

  _awaitAck(seq, timeoutMs) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pending.delete(seq);
        resolve(false);
      }, timeoutMs);
      this.pending.set(seq, (ok) => {
        clearTimeout(timer);
        this.pending.delete(seq);
        resolve(ok);
      });
    });
  }

  _ping(host, port, timeoutMs) {
    const seq = ++this.seq;
    const p = this._awaitAck(seq, timeoutMs);
    this._send('ping', host, port, { seq });
    return p;
  }

  _pingReq(helper, target, timeoutMs) {
    const seq = ++this.seq;
    const p = this._awaitAck(seq, timeoutMs);
    this._send('ping-req', helper.host, helper.port, { seq, target: target.id });
    return p;
  }

  async _handlePingReq(msg, rinfo) {
    const target = this.membership.get(msg.target);
    if (!target) return;
    const ok = await this._ping(target.host, target.port, this.opts.probeTimeoutMs);
    if (ok && this.running) this._send('ack', rinfo.address, rinfo.port, { seq: msg.seq });
  }

  /**
   * SWIM probe. Direct ping first; if that fails ask k other members to ping
   * the target. Only if *nobody* can reach it do we raise a suspicion. This
   * protects against false positives caused by a bad link between two nodes.
   */
  async _probe(m) {
    if (this.probing.has(m.id)) return;
    this.probing.add(m.id);
    this.stats.probes++;
    try {
      const { probeTimeoutMs, indirectProbes } = this.opts;
      let ok = await this._ping(m.host, m.port, probeTimeoutMs);
      if (!ok && this.running) {
        const helpers = sample(
          this.membership.peers(Status.ALIVE).filter((p) => p.id !== m.id),
          indirectProbes,
        );
        if (helpers.length) {
          this.stats.indirectProbes++;
          this.log.debug(`direct ping to ${m.id} failed, asking ${helpers.map((h) => h.id).join(', ')}`);
          ok = await anyTrue(helpers.map((h) => this._pingReq(h, m, probeTimeoutMs * 2)));
        }
      }
      if (!this.running) return;
      if (ok) this.membership.heard(m.id);
      else this.membership.suspect(m.id, 'no ACK to direct ping or indirect ping-req');
    } finally {
      this.probing.delete(m.id);
    }
  }

  // ------------------------------------------------------------------ helpers

  snapshot() {
    const self = this.membership.self;
    return {
      id: this.id,
      address: `${this.host}:${this.port}`,
      incarnation: self.incarnation,
      heartbeat: self.heartbeat,
      uptimeMs: this.startedAt ? Date.now() - this.startedAt : 0,
      reporter: this.membership.reporterId(),
      stats: { ...this.stats },
      config: {
        gossipIntervalMs: this.opts.gossipIntervalMs,
        fanout: this.opts.fanout,
        suspectAfterMs: this.opts.suspectAfterMs,
        deadAfterMs: this.opts.deadAfterMs,
        probeTimeoutMs: this.opts.probeTimeoutMs,
        lossRate: this.opts.lossRate,
      },
      members: this.membership.snapshot(),
    };
  }

  _logChange(e) {
    const how = e.source === 'local' ? `detected locally: ${e.reason}` : `learned via gossip from ${e.via}`;
    switch (e.type) {
      case 'join':
        return this.log.info(`[JOIN]    ${e.id} joined the cluster (${how})`);
      case 'suspect':
        return this.log.warn(`[SUSPECT] ${e.id} is suspected (${how})`);
      case 'dead':
        return this.log.error(`[DEAD]    ${e.id} is DEAD (${how})`);
      case 'left':
        return this.log.info(`[LEFT]    ${e.id} left gracefully (${how})`);
      case 'alive':
        return this.log.info(`[ALIVE]   ${e.id} refuted suspicion, it is alive (incarnation ${e.incarnation})`);
      case 'rejoin':
        return this.log.info(`[REJOIN]  ${e.id} is back (was ${e.from}, incarnation ${e.incarnation})`);
      default:
        return this.log.info(`[${e.type}] ${e.id}`);
    }
  }
}
