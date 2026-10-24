#!/usr/bin/env node
/**
 * Interactive local cluster: spawns N real node processes and gives you a
 * shell to break them.
 *
 *   npm run cluster            # 5 nodes
 *   node scripts/cluster.js 7  # 7 nodes
 */
import readline from 'node:readline';
import { ClusterManager, renderMatrix } from './lib/cluster.js';

const size = Number(process.argv[2] ?? process.env.NODES ?? 5);
const cluster = new ClusterManager({ size });

const HELP = `
Commands (n = node number 1..${size}):
  status        show every node's view of the cluster (observer x member matrix)
  kill n        CRASH node n (SIGKILL)  -> others detect it via gossip + probes
  stop n        graceful shutdown (SIGTERM) -> node announces LEFT
  pause n       freeze node n (SIGSTOP) -> looks dead to everyone
  resume n      unfreeze (SIGCONT)      -> node refutes and REJOINs
  start n       (re)start node n
  watch         print status every second (press Enter to stop)
  help          this help
  quit          stop all nodes and exit
Dashboard: http://localhost:${cluster.baseHttpPort + 1}
`;

console.log(`Starting ${size} gossip nodes (UDP ${cluster.baseGossipPort + 1}-${cluster.baseGossipPort + size}, HTTP ${cluster.baseHttpPort + 1}-${cluster.baseHttpPort + size})`);
console.log(HELP);
cluster.startAll();

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: 'gossip> ' });
let watchTimer = null;

async function printStatus() {
  console.log('\n' + renderMatrix(cluster, await cluster.views()) + '\n');
}

let quitting = false;
async function quit() {
  if (quitting) return;
  quitting = true;
  clearInterval(watchTimer);
  console.log('stopping all nodes...');
  await cluster.stopAll();
  process.exit(0);
}

rl.on('line', async (line) => {
  if (watchTimer) {
    clearInterval(watchTimer);
    watchTimer = null;
    rl.prompt();
    return;
  }
  const [cmd, arg] = line.trim().split(/\s+/);
  const n = Number(arg);
  const valid = Number.isInteger(n) && n >= 1 && n <= size;
  const need = () => {
    if (!valid) console.log(`usage: ${cmd} <1..${size}>`);
    return valid;
  };

  switch (cmd) {
    case '':
      break;
    case 'status':
    case 's':
      await printStatus();
      break;
    case 'kill':
      if (need()) console.log(cluster.signal(n, 'SIGKILL') ? `node-${n} crashed (SIGKILL)` : `node-${n} is not running`);
      break;
    case 'stop':
      if (need()) console.log(cluster.signal(n, 'SIGTERM') ? `node-${n} stopping gracefully` : `node-${n} is not running`);
      break;
    case 'pause':
      if (need()) console.log(cluster.signal(n, 'SIGSTOP') ? `node-${n} frozen (SIGSTOP)` : `node-${n} is not running`);
      break;
    case 'resume':
      if (need()) console.log(cluster.signal(n, 'SIGCONT') ? `node-${n} resumed (SIGCONT)` : `node-${n} is not running`);
      break;
    case 'start':
      if (need()) {
        if (cluster.isRunning(n)) console.log(`node-${n} already running`);
        else cluster.start(n), console.log(`node-${n} starting`);
      }
      break;
    case 'watch':
      watchTimer = setInterval(printStatus, 1000);
      return;
    case 'help':
      console.log(HELP);
      break;
    case 'quit':
    case 'exit':
      return quit();
    default:
      console.log(`unknown command "${cmd}", type help`);
  }
  rl.prompt();
});

rl.on('close', quit);
process.on('SIGINT', quit);
rl.prompt();
