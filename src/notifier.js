import { appendFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { silentLogger } from './logger.js';

const ALERT_TYPES = new Set(['dead', 'left', 'rejoin']);

const MESSAGES = {
  dead: (e) => `Node ${e.id} is DEAD (no heartbeat, failed direct + indirect probes).`,
  left: (e) => `Node ${e.id} left the cluster gracefully.`,
  rejoin: (e) => `Node ${e.id} is back online (incarnation ${e.incarnation}).`,
};

/**
 * Turns membership changes into notifications.
 *
 * Every node:   keeps an in-memory history, appends JSONL to logFile.
 * One node:     the "reporter" (lowest alive id in its own view) sends the
 *               external alert (webhook / Slack / PagerDuty...). Because every
 *               node converges on the same membership, they agree on the
 *               reporter without any extra coordination, so you get ONE alert
 *               per incident instead of N. If the reporter itself dies, the
 *               next-lowest node automatically takes over.
 */
export class Notifier {
  constructor(node, { webhookUrl = null, logFile = null, historySize = 200, logger = silentLogger } = {}) {
    this.node = node;
    this.webhookUrl = webhookUrl;
    this.logFile = logFile;
    this.historySize = historySize;
    this.log = logger;
    this.history = [];
    this.alerts = [];
    node.on('member:change', (e) => this.handle(e).catch((err) => this.log.error('notifier:', err.message)));
  }

  async handle(e) {
    this.history.push(e);
    if (this.history.length > this.historySize) this.history.shift();

    if (this.logFile) {
      await mkdir(dirname(this.logFile), { recursive: true });
      await appendFile(this.logFile, JSON.stringify(e) + '\n');
    }

    if (!ALERT_TYPES.has(e.type)) return;
    const reporter = this.node.membership.reporterId();
    if (reporter !== this.node.id) return;

    const text = `[gossip-cluster] ${MESSAGES[e.type](e)} Reported by ${this.node.id}.`;
    const alert = { text, event: e, reporter: this.node.id, at: new Date().toISOString() };
    this.alerts.push(alert);
    this.log.warn(`[ALERT]   ${text}`);

    if (this.webhookUrl) {
      try {
        const res = await fetch(this.webhookUrl, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(alert), // `text` field makes it Slack-incoming-webhook compatible
          signal: AbortSignal.timeout(3000),
        });
        if (!res.ok) this.log.warn(`webhook responded ${res.status}`);
      } catch (err) {
        this.log.warn(`webhook failed: ${err.message}`);
      }
    }
  }
}
