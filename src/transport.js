import dgram from 'node:dgram';
import { EventEmitter } from 'node:events';
import { silentLogger } from './logger.js';

/**
 * Minimal UDP transport. Gossip protocols are designed for an unreliable,
 * connectionless network: messages may be lost or reordered and the protocol
 * still converges because every round re-sends the full (or delta) state.
 */
export class UdpTransport extends EventEmitter {
  constructor({ bindHost = '0.0.0.0', port, lossRate = 0, logger = silentLogger }) {
    super();
    this.bindHost = bindHost;
    this.port = port;
    this.lossRate = lossRate;
    this.log = logger;
    this.socket = null;
    this.closed = false;
  }

  start() {
    return new Promise((resolve, reject) => {
      const socket = dgram.createSocket('udp4');
      this.socket = socket;
      socket.on('message', (buf, rinfo) => {
        let msg;
        try {
          msg = JSON.parse(buf.toString('utf8'));
        } catch {
          this.log.warn(`dropping malformed packet from ${rinfo.address}:${rinfo.port}`);
          return;
        }
        this.emit('message', msg, rinfo);
      });
      socket.once('error', reject);
      socket.bind(this.port, this.bindHost, () => {
        socket.off('error', reject);
        socket.on('error', (err) => this.log.error('udp error:', err.message));
        resolve();
      });
    });
  }

  send(msg, host, port) {
    if (!this.socket || this.closed) return;
    // Chaos testing: drop a percentage of outgoing packets.
    if (this.lossRate > 0 && Math.random() < this.lossRate) return;
    const buf = Buffer.from(JSON.stringify(msg));
    this.socket.send(buf, port, host, (err) => {
      if (err) this.log.debug(`send to ${host}:${port} failed: ${err.message}`);
    });
  }

  stop() {
    this.closed = true;
    return new Promise((resolve) => {
      if (!this.socket) return resolve();
      try {
        this.socket.close(() => resolve());
      } catch {
        resolve();
      }
    });
  }
}
