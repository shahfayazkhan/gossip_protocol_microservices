import { EventEmitter } from 'node:events';

export const Status = Object.freeze({
  ALIVE: 'alive',
  SUSPECT: 'suspect',
  DEAD: 'dead',
  LEFT: 'left',
});

/**
 * Precedence of states *within the same incarnation*.
 * A worse state always overrides a better one, so a suspicion/death rumour
 * spreads through the cluster. Only the node itself can "undo" it, by
 * bumping its incarnation number (see refutation in apply()).
 */
const RANK = { alive: 0, suspect: 1, dead: 2, left: 3 };

const isEntry = (e) =>
  e &&
  typeof e.id === 'string' &&
  typeof e.host === 'string' &&
  Number.isInteger(e.port) &&
  Number.isFinite(e.incarnation) &&
  Number.isFinite(e.heartbeat) &&
  e.status in RANK;

/**
 * The membership table: this node's view of the cluster.
 *
 * Each entry: { id, host, port, httpPort, meta, incarnation, heartbeat, status,
 *               lastSeen, statusChangedAt }
 *  - heartbeat   : counter the node increments every gossip round (liveness signal)
 *  - incarnation : version of the node's *identity*. Starts at Date.now() so a
 *                  restarted process always beats its old (dead) record, and is
 *                  incremented to refute a false suspicion.
 *  - lastSeen / statusChangedAt are LOCAL clocks only, never gossiped.
 *
 * Emits:
 *  'change'  { type: join|suspect|dead|left|alive|rejoin, id, from, to, source, via, reason, ... }
 *  'refute'  { incarnation, accusedBy }
 *  'removed' { id }
 */
export class Membership extends EventEmitter {
  constructor(self, { cleanupAfterMs = 60000, now = Date.now } = {}) {
    super();
    this.selfId = self.id;
    this.now = now;
    this.cleanupAfterMs = cleanupAfterMs;
    const t = now();
    this.members = new Map();
    this.members.set(self.id, {
      id: self.id,
      host: self.host,
      port: self.port,
      httpPort: self.httpPort ?? null,
      meta: self.meta ?? {},
      incarnation: t,
      heartbeat: 0,
      status: Status.ALIVE,
      lastSeen: t,
      statusChangedAt: t,
    });
    this.tombstones = new Map(); // id -> { incarnation, at }
  }

  get self() {
    return this.members.get(this.selfId);
  }

  get(id) {
    return this.members.get(id);
  }

  list() {
    return [...this.members.values()];
  }

  /** Peers (excluding self) whose status is one of `statuses`. */
  peers(...statuses) {
    return this.list().filter((m) => m.id !== this.selfId && statuses.includes(m.status));
  }

  /** Increment our own heartbeat — called once per gossip round. */
  beat() {
    const s = this.self;
    s.heartbeat += 1;
    s.lastSeen = this.now();
  }

  /** The gossiped representation of the table (no local timestamps). */
  digest() {
    return this.list().map(({ id, host, port, httpPort, meta, incarnation, heartbeat, status }) => ({
      id,
      host,
      port,
      httpPort,
      meta,
      incarnation,
      heartbeat,
      status,
    }));
  }

  merge(entries, via = null) {
    if (!Array.isArray(entries)) return;
    for (const e of entries) if (isEntry(e)) this.apply(e, via);
  }

  /** Direct evidence that `id` is reachable (it sent us a message / ACK). */
  heard(id) {
    const m = this.members.get(id);
    if (m && m.status === Status.ALIVE) m.lastSeen = this.now();
  }

  /** Merge a single remote entry `r` into the local table (CRDT-like, order-independent). */
  apply(r, via = null) {
    const now = this.now();

    // --- Rumours about ourselves -------------------------------------------
    if (r.id === this.selfId) {
      const s = this.self;
      if (s.status === Status.LEFT) return;
      if (r.status !== Status.ALIVE && r.incarnation >= s.incarnation) {
        // Someone thinks we are suspect/dead. Refute by out-versioning the rumour.
        s.incarnation = r.incarnation + 1;
        this.emit('refute', { incarnation: s.incarnation, accusedBy: via, claimed: r.status });
      }
      return;
    }

    // --- Removed (tombstoned) nodes may only come back with a newer incarnation
    const tomb = this.tombstones.get(r.id);
    if (tomb) {
      if (r.status === Status.ALIVE && r.incarnation > tomb.incarnation) this.tombstones.delete(r.id);
      else return;
    }

    let l = this.members.get(r.id);
    if (!l) {
      l = {
        id: r.id,
        host: r.host,
        port: r.port,
        httpPort: r.httpPort ?? null,
        meta: r.meta ?? {},
        incarnation: r.incarnation,
        heartbeat: r.heartbeat,
        status: r.status,
        lastSeen: now,
        statusChangedAt: now,
      };
      this.members.set(r.id, l);
      // Learning about an already-dead node is not news; only announce live joins.
      if (r.status === Status.ALIVE) this._emit(l, null, 'gossip', via);
      return;
    }

    const prev = l.status;
    if (r.incarnation > l.incarnation) {
      // Newer identity version always wins (restart or refutation).
      Object.assign(l, {
        host: r.host,
        port: r.port,
        httpPort: r.httpPort ?? null,
        meta: r.meta ?? {},
        incarnation: r.incarnation,
        heartbeat: r.heartbeat,
        status: r.status,
        lastSeen: now,
      });
    } else if (r.incarnation === l.incarnation) {
      if (RANK[r.status] > RANK[l.status]) l.status = r.status;
      if (r.heartbeat > l.heartbeat) {
        l.heartbeat = r.heartbeat;
        if (l.status === Status.ALIVE) l.lastSeen = now; // fresh heartbeat = proof of life
      }
    } else {
      return; // stale information
    }

    if (l.status !== prev) {
      l.statusChangedAt = now;
      this._emit(l, prev, 'gossip', via);
    }
  }

  /** Local failure detector: probe failed -> SUSPECT. */
  suspect(id, reason) {
    const m = this.members.get(id);
    if (!m || m.id === this.selfId || m.status !== Status.ALIVE) return false;
    m.status = Status.SUSPECT;
    m.statusChangedAt = this.now();
    this._emit(m, Status.ALIVE, 'local', this.selfId, reason);
    return true;
  }

  /** Local failure detector: suspicion not refuted in time -> DEAD. */
  declareDead(id, reason) {
    const m = this.members.get(id);
    if (!m || m.id === this.selfId || m.status !== Status.SUSPECT) return false;
    m.status = Status.DEAD;
    m.statusChangedAt = this.now();
    this._emit(m, Status.SUSPECT, 'local', this.selfId, reason);
    return true;
  }

  /** Mark ourselves as LEFT (graceful shutdown). The next gossip carries it. */
  leave() {
    const s = this.self;
    s.status = Status.LEFT;
    s.statusChangedAt = this.now();
  }

  /** Drop DEAD/LEFT entries after cleanupAfterMs, keeping a tombstone. */
  cleanup() {
    const now = this.now();
    for (const m of this.list()) {
      if (m.id === this.selfId) continue;
      if ((m.status === Status.DEAD || m.status === Status.LEFT) && now - m.statusChangedAt > this.cleanupAfterMs) {
        this.members.delete(m.id);
        this.tombstones.set(m.id, { incarnation: m.incarnation, at: now });
        this.emit('removed', { id: m.id });
      }
    }
    for (const [id, t] of this.tombstones) {
      if (now - t.at > this.cleanupAfterMs * 2) this.tombstones.delete(id);
    }
  }

  /**
   * Deterministic "reporter": the alive node with the lowest id in our view.
   * Used to de-duplicate external alerts (only one node sends the webhook).
   */
  reporterId() {
    const alive = this.list()
      .filter((m) => m.status === Status.ALIVE)
      .map((m) => m.id)
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
    return alive[0] ?? null;
  }

  snapshot() {
    const now = this.now();
    return this.list()
      .map((m) => ({
        ...m,
        isSelf: m.id === this.selfId,
        lastSeenAgoMs: now - m.lastSeen,
        statusAgeMs: now - m.statusChangedAt,
      }))
      .sort((a, b) => a.id.localeCompare(b.id, undefined, { numeric: true }));
  }

  _emit(m, from, source, via, reason) {
    let type;
    if (m.status === Status.ALIVE) {
      if (from === null) type = 'join';
      else if (from === Status.SUSPECT) type = 'alive'; // suspicion refuted (false positive)
      else type = 'rejoin'; // came back from dead/left
    } else {
      type = m.status; // suspect | dead | left
    }
    this.emit('change', {
      type,
      id: m.id,
      from,
      to: m.status,
      incarnation: m.incarnation,
      source, // 'local' = this node detected it, 'gossip' = learned from a peer
      via,
      reason: reason ?? null,
      observer: this.selfId,
      at: new Date(this.now()).toISOString(),
    });
  }
}
