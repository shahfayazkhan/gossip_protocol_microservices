#!/usr/bin/env node
/**
 * Run ONE cluster member.
 *
 *   node bin/node.js --id node-1 --port 7001 --http-port 8001 --seeds 127.0.0.1:7001,127.0.0.1:7002
 *
 * Every flag also has an env var (handy for Docker):
 *   NODE_ID, ADVERTISE_HOST, BIND_HOST, GOSSIP_PORT, HTTP_PORT, SEEDS, WEBHOOK_URL, LOG_LEVEL
 *   + timing overrides from src/config.js (GOSSIP_INTERVAL_MS, SUSPECT_AFTER_MS, DEAD_AFTER_MS, ...)
 */
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';
import { GossipNode } from '../src/gossip-node.js';
import { Notifier } from '../src/notifier.js';
import { startHttpServer } from '../src/http-server.js';
import { configFromEnv } from '../src/config.js';
import { createLogger } from '../src/logger.js';

const { values: args } = parseArgs({
  options: {
    id: { type: 'string' },
    host: { type: 'string' }, // address other nodes use to reach us
    bind: { type: 'string' }, // local interface to bind UDP/HTTP on
    port: { type: 'string' },
    'http-port': { type: 'string' },
    seeds: { type: 'string' },
    webhook: { type: 'string' },
    'loss-rate': { type: 'string' },
  },
});

const env = process.env;
const port = Number(args.port ?? env.GOSSIP_PORT ?? 7001);
const httpPort = Number(args['http-port'] ?? env.HTTP_PORT ?? port + 1000);
const id = args.id ?? env.NODE_ID ?? `node-${port}`;
const host = args.host ?? env.ADVERTISE_HOST ?? '127.0.0.1';
const bindHost = args.bind ?? env.BIND_HOST ?? '0.0.0.0';
const seeds = args.seeds ?? env.SEEDS ?? '';
const webhookUrl = args.webhook ?? env.WEBHOOK_URL ?? null;

const timing = configFromEnv();
if (args['loss-rate'] !== undefined) timing.lossRate = Number(args['loss-rate']);

const logger = createLogger(id);
const node = new GossipNode({ id, host, bindHost, port, httpPort, seeds, logger, ...timing });
const logFile = fileURLToPath(new URL(`../logs/${id}.events.jsonl`, import.meta.url));
const notifier = new Notifier(node, { webhookUrl, logFile, logger });

await node.start();
const server = await startHttpServer(node, notifier, { port: httpPort, host: bindHost, logger });
logger.info(`http api + dashboard on http://localhost:${httpPort}`);

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  setTimeout(() => process.exit(1), 3000).unref(); // safety net
  logger.info(`${signal} received, leaving cluster`);
  await node.leave();
  server.close();
  process.exit(0);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
