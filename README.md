# Gossip Protocol Cluster (Node.js)

A cluster of Node.js nodes that use a **gossip protocol** with **SWIM-style failure detection**. When a node dies, every other node finds out within a few seconds and a single alert goes out. There is no central coordinator, no shared database and no npm dependencies.

```
npm test            # unit + 5-node integration tests (~4s)
npm run demo        # automated 5-node scenario with measured detection times
npm run cluster     # interactive 5-node cluster you can kill / pause / restart
open http://localhost:8001   # live dashboard (while `npm run cluster` is running)
```

Requires Node.js 18.17 or newer (tested on 22).

---

## 1. Architecture

```
            ┌─────────── UDP gossip (syn / syn-ack, ping / ack, ping-req, leave) ───────────┐
            │                                                                               │
   ┌────────┴───────┐      ┌────────────────┐      ┌────────────────┐      ┌────────────────┴┐   ┌────────────────┐
   │ node-1 (seed)  │◄────►│ node-2 (seed)  │◄────►│     node-3     │◄────►│     node-4      │◄─►│     node-5     │
   │ UDP 7001       │      │ UDP 7002       │      │ UDP 7003       │      │ UDP 7004        │   │ UDP 7005       │
   │ HTTP 8001      │      │ HTTP 8002      │      │ HTTP 8003      │      │ HTTP 8004       │   │ HTTP 8005      │
   └────────────────┘      └────────────────┘      └────────────────┘      └─────────────────┘   └────────────────┘
          ▲  every node holds a full membership table and talks to `fanout` random peers per round
          │
          └── reporter (lowest alive id) ──► webhook / Slack: ONE alert per incident
```

Each node is built from these layers:

| Layer | File | Responsibility |
|---|---|---|
| Transport | `src/transport.js` | UDP + JSON. Messages can be lost, and the protocol is built to cope with that. |
| Membership table | `src/membership.js` | State of every node plus a conflict-free merge (`incarnation`, `status`, `heartbeat`). |
| Gossip + failure detector | `src/gossip-node.js` | Gossip rounds, heartbeats, SWIM ping / ping-req, suspicion timers. |
| Notifier | `src/notifier.js` | Event history, a JSONL log per node, and de-duplicated webhook alerts. |
| HTTP API + dashboard | `src/http-server.js`, `public/dashboard.html` | Observability and admin actions (crash/leave). |

### 1.1 Gossip dissemination (how information spreads)

Every `gossipIntervalMs` (1s by default), each node:

1. increments its own **heartbeat** counter,
2. picks `fanout` (3) random live peers,
3. sends them its membership digest (`syn`). Each peer merges the digest and replies with its own (`syn-ack`). This is **push-pull** gossip.

A rumour reaches all N nodes in about **O(log N)** rounds. With 5 nodes the cluster converges in under a second. Every message (including pings and acks) carries the digest, so state also spreads on every packet.

### 1.2 Membership entry and merge rules

```js
{ id, host, port, httpPort, incarnation, heartbeat, status }   // status: alive | suspect | dead | left
```

Merging is order-independent and idempotent, so nodes end up with the same table whatever order messages arrive in:

1. **Higher `incarnation` always wins.**
2. **Same incarnation, then the worse status wins:** `alive < suspect < dead < left`. This is how a suspicion or death spreads.
3. **Same incarnation and same status, then a higher `heartbeat` wins.** A new heartbeat counts as proof of life and refreshes `lastSeen`.
4. **Only a node can clear its own name.** If it hears it is `suspect` or `dead`, it sets `incarnation = rumour + 1`. That beats the rumour everywhere (this is called **refutation**).
5. `incarnation` starts at `Date.now()`, so a **restarted process automatically outranks its old dead record**.
6. DEAD/LEFT entries are removed after `cleanupAfterMs` and leave a **tombstone**, so stale gossip can't bring a removed node back.

### 1.3 Failure detection (how a dead node is found)

```
             heartbeat silent > suspectAfterMs
   ALIVE ────────────────────────────────────►  probe:
     ▲                                             1. direct PING  ──ack?──► still ALIVE
     │                                             2. PING-REQ via k other nodes ──ack?──► still ALIVE
     │ refutation                                  3. nobody got an ACK
     │ (incarnation + 1)                                  │
     │                                                    ▼
     ├──────────────────────────────────────────── SUSPECT  (gossiped to everyone)
     │                                                    │ not refuted within deadAfterMs
     │                                                    ▼
     └──────────── restart / resume ─────────────── DEAD     (gossiped + ALERT)

   graceful SIGTERM ─────────────────────────────► LEFT     (instant, no timeouts)
```

* **Heartbeat staleness** is cheap and passive: a node whose counter stops rising gets probed.
* **Indirect probing (ping-req)** prevents false positives. If A can't reach C but B can, then C is fine and only the A↔C link is bad.
* The **SUSPECT window** gives a node that is slow (GC pause, overloaded) or briefly partitioned time to refute before it is declared dead.
* **Graceful leave** sends `left` straight away, so planned shutdowns never cause false alarms.

Rough worst-case detection time with the defaults: `suspectAfter 3s + probes ~1.5s + deadAfter 4s ≈ 9s`. The demo uses faster timings and gets about **4.4s**, and the tests get about **1.6s**. Tune it in `src/config.js` or with environment variables.

### 1.4 How the system notifies you that a node is dead

| Channel | Who | What |
|---|---|---|
| `member:dead` / `member:suspect` / `member:left` / `member:rejoin` events | every node, in-process | Hook your own logic in here (drop the node from a load balancer, re-shard, etc.). See `examples/basic-usage.js`. |
| Console log | every node | `[SUSPECT]`, `[DEAD]`, `[REJOIN]`, `[LEFT]`, each saying whether it was *detected locally* or *learned via gossip from X*. |
| `logs/<node>.events.jsonl` | every node | Audit trail of membership changes. |
| `GET /events` (SSE), `GET /history` | every node | Live stream for dashboards and other services. |
| **Webhook / Slack** (`WEBHOOK_URL`) | **reporter only** | One alert per incident. |

**Alert de-duplication without coordination:** every node computes `reporter = lowest alive id` from its own (converged) table, and only that node sends the webhook. So you get one alert instead of five. If the reporter itself dies, the next-lowest node becomes the reporter automatically.

---

## 2. Running it

### 2.1 Automated demo (recommended first run)

```bash
npm run demo           # add -- --verbose to see every node's log
```

It starts 5 processes and then:
1. waits for convergence,
2. **SIGKILLs node-3** and measures when each survivor sees SUSPECT → DEAD,
3. restarts node-3 (REJOIN),
4. **SIGSTOPs node-4** (a hung process), which gets declared DEAD; on SIGCONT it refutes and REJOINs with a bumped incarnation,
5. **SIGTERMs node-5**, which shows up as LEFT in about 100ms,
6. prints the alerts (exactly one per incident).

Sample output:

```
== 2. CRASH node-3 (SIGKILL, no goodbye)
   node-1 | [ALERT] [gossip-cluster] Node node-3 is DEAD (no heartbeat, failed direct + indirect probes). Reported by node-1.
   node-1: DEAD after 4401ms  (SUSPECT after 2307ms)
   node-2: DEAD after 4401ms  (SUSPECT after 2414ms)
   node-4: DEAD after 4402ms  (SUSPECT after 2414ms)
   node-5: DEAD after 4503ms  (SUSPECT after 2414ms)

observer \ member  node-1   node-2   node-3   node-4   node-5
node-1              alive    alive    DEAD     alive    alive
node-2              alive    alive    DEAD     alive    alive
node-3 (down)
node-4              alive    alive    DEAD     alive    alive
node-5              alive    alive    DEAD     alive    alive
```

### 2.2 Interactive cluster

```bash
npm run cluster        # or: node scripts/cluster.js 7
gossip> status         # matrix of every node's view
gossip> kill 3         # crash
gossip> pause 4        # freeze (looks dead)
gossip> resume 4       # refutes, REJOIN
gossip> stop 5         # graceful LEFT
gossip> start 3        # bring it back
gossip> watch          # live matrix every second
```

Open **http://localhost:8001** (any node's port works) for the dashboard. It shows each node's view and has crash/leave buttons.

### 2.3 Real alerts end-to-end

```bash
node examples/webhook-receiver.js                                  # terminal 1
WEBHOOK_URL=http://127.0.0.1:9999/alert npm run cluster            # terminal 2, then: kill 3
```

To send alerts to Slack, use a Slack incoming-webhook URL instead. The payload already has a `text` field.

### 2.4 Separate terminals / separate machines

```bash
node bin/node.js --id node-1 --port 7001 --http-port 8001 --seeds 127.0.0.1:7001,127.0.0.1:7002
node bin/node.js --id node-2 --port 7002 --http-port 8002 --seeds 127.0.0.1:7001,127.0.0.1:7002
node bin/node.js --id node-3 --port 7003 --http-port 8003 --seeds 127.0.0.1:7001,127.0.0.1:7002
# ...
```

On different machines, set `--host <ip other nodes can reach>` and point `--seeds` at one or two stable nodes. Seeds are only needed for the first contact; after that, nodes learn about each other through gossip.

### 2.5 Docker (5 containers)

```bash
docker compose up --build
docker compose kill node-3      # crash
docker compose pause node-4     # hang
docker compose unpause node-4
docker compose stop node-5      # graceful
```

---

## 3. Configuration

| Option | Env var | Default | Meaning |
|---|---|---|---|
| `--id` | `NODE_ID` | `node-<port>` | Unique node name |
| `--host` | `ADVERTISE_HOST` | `127.0.0.1` | Address peers use to reach this node |
| `--bind` | `BIND_HOST` | `0.0.0.0` | Interface to bind |
| `--port` | `GOSSIP_PORT` | `7001` | UDP gossip port |
| `--http-port` | `HTTP_PORT` | port + 1000 | HTTP API / dashboard |
| `--seeds` | `SEEDS` | – | `host:port,host:port` |
| `--webhook` | `WEBHOOK_URL` | – | Alert endpoint |
| `--loss-rate` | `LOSS_RATE` | `0` | Simulated packet loss (chaos) |
| | `GOSSIP_INTERVAL_MS` | 1000 | Gossip round period |
| | `FANOUT` | 3 | Peers per round |
| | `SUSPECT_AFTER_MS` | 3000 | Heartbeat silence before probing |
| | `PROBE_TIMEOUT_MS` | 500 | Direct ping timeout (ping-req uses 2×) |
| | `INDIRECT_PROBES` | 2 | k helpers for ping-req |
| | `DEAD_AFTER_MS` | 4000 | Suspicion window before DEAD |
| | `CLEANUP_AFTER_MS` | 60000 | Remove DEAD/LEFT entries after this |
| | `LOG_LEVEL` | info | debug / info / warn / error |

**Tuning trade-off:** shorter timeouts detect failures faster but give more false positives on slow networks or during GC pauses. The refutation mechanism limits the damage, because a falsely suspected node clears itself in one gossip round.

## 4. HTTP API (per node)

| Method | Path | Description |
|---|---|---|
| GET | `/` | Dashboard |
| GET | `/health` | Process liveness |
| GET | `/members` | This node's membership table, stats and config |
| GET | `/history` | Recent membership events |
| GET | `/alerts` | Alerts this node sent as reporter |
| GET | `/events` | Server-Sent Events stream |
| POST | `/admin/crash` | SIGKILL itself (demo) |
| POST | `/admin/leave` | Graceful leave |

## 5. Using it as a library

```js
import { GossipNode } from './src/index.js';

const node = new GossipNode({ id: 'api-1', port: 7001, seeds: ['10.0.0.5:7001'] });
node.on('member:dead',   (e) => loadBalancer.remove(e.id));
node.on('member:rejoin', (e) => loadBalancer.add(e.id));
node.on('member:join',   (e) => loadBalancer.add(e.id));
await node.start();
process.on('SIGTERM', () => node.leave().then(() => process.exit(0)));
```

## 6. Tests

```bash
npm test
```

* `test/membership.test.js`: merge rules, refutation, tombstones and reporter election, using a deterministic clock.
* `test/cluster.test.js`: 5 real UDP nodes in one process. It covers convergence, crash detection by every survivor with exactly one alert, rejoin with a newer incarnation, refuting a false suspicion, graceful leave, and correct detection under **20% packet loss** with no false positives.

## 7. Going to production: what to change and why

This project shows the core ideas. Real systems (HashiCorp memberlist/Serf/Consul, Cassandra, ScyllaDB, Redis Cluster, Akka Cluster) add the following:

* **Delta gossip / piggyback buffers.** This project sends the full table on every message, which is fine for tens of nodes. For hundreds or thousands, gossip only recent changes, each re-sent about `λ·log N` times, plus a periodic full push-pull sync (usually over TCP).
* **Round-robin probe order** (SWIM) instead of probing only stale nodes. This puts a firm upper bound on time-to-first-detection.
* **Lifeguard extensions** (memberlist): a node backs off its own timeouts when it notices it is slow itself, and suspicion timeouts shrink as more nodes independently confirm the suspicion.
* **Phi-accrual failure detector** (Cassandra, Akka): instead of fixed timeouts, it outputs a suspicion level based on the observed distribution of heartbeat intervals.
* **Security.** Encrypt and authenticate gossip (for example AES-GCM with a shared cluster key) so nobody can inject "node X is dead" messages.
* **Message-size limits.** Keep UDP packets under about 1400 bytes (the MTU) and fall back to TCP for large syncs.
* **Persisting the event log** somewhere central, and alerting from metrics (Prometheus `cluster_members{status="dead"}`) as well as webhooks.
