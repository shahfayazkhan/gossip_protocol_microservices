/**
 * Default timing / protocol parameters.
 *
 * Worst-case time to declare a crashed node DEAD on an observer:
 *   suspectAfterMs            (no fresh heartbeat seen)
 * + failureCheckIntervalMs    (detector tick granularity)
 * + probeTimeoutMs * 3        (direct ping + indirect ping-req)
 * + deadAfterMs               (suspicion window, lets the node refute)
 * ≈ 3000 + 500 + 1500 + 4000 ≈ 9s with the defaults below.
 *
 * Lower values = faster detection but more false positives on a busy/slow network.
 */
export const defaults = Object.freeze({
  gossipIntervalMs: 1000, // how often each node gossips its membership table
  fanout: 3, // how many random peers receive each gossip round
  failureCheckIntervalMs: 500, // how often the local failure detector runs
  suspectAfterMs: 3000, // heartbeat silence before probing a peer
  probeTimeoutMs: 500, // wait for an ACK to a direct ping
  indirectProbes: 2, // k helpers asked to ping-req on our behalf
  deadAfterMs: 4000, // time a node stays SUSPECT before it is declared DEAD
  cleanupAfterMs: 60000, // DEAD/LEFT entries are removed (tombstoned) after this
  deadGossipProbability: 0.1, // chance per round to gossip to a DEAD node (partition healing)
  lossRate: 0, // simulated outgoing packet loss 0..1 (chaos testing)
});

const ENV_MAP = {
  gossipIntervalMs: 'GOSSIP_INTERVAL_MS',
  fanout: 'FANOUT',
  failureCheckIntervalMs: 'FAILURE_CHECK_INTERVAL_MS',
  suspectAfterMs: 'SUSPECT_AFTER_MS',
  probeTimeoutMs: 'PROBE_TIMEOUT_MS',
  indirectProbes: 'INDIRECT_PROBES',
  deadAfterMs: 'DEAD_AFTER_MS',
  cleanupAfterMs: 'CLEANUP_AFTER_MS',
  deadGossipProbability: 'DEAD_GOSSIP_PROBABILITY',
  lossRate: 'LOSS_RATE',
};

/** Read numeric overrides from environment variables. */
export function configFromEnv(env = process.env) {
  const out = {};
  for (const [key, name] of Object.entries(ENV_MAP)) {
    if (env[name] !== undefined && env[name] !== '') {
      const n = Number(env[name]);
      if (Number.isNaN(n)) throw new Error(`${name} must be a number, got "${env[name]}"`);
      out[key] = n;
    }
  }
  return out;
}
