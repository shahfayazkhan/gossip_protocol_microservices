/**
 * A tiny alert receiver so you can see the end-to-end notification path.
 *
 *   Terminal 1:  node examples/webhook-receiver.js
 *   Terminal 2:  WEBHOOK_URL=http://127.0.0.1:9999/alert npm run cluster
 *                gossip> kill 3
 *
 * Exactly ONE alert arrives per incident (sent by the reporter node).
 * Swap the URL for a Slack incoming-webhook URL; the payload has a `text` field.
 */
import http from 'node:http';

const PORT = Number(process.env.PORT ?? 9999);

http
  .createServer((req, res) => {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      try {
        const alert = JSON.parse(body);
        console.log(`${new Date().toLocaleTimeString()}  ALERT  ${alert.text}`);
      } catch {
        console.log('received non-JSON body:', body);
      }
      res.writeHead(204).end();
    });
  })
  .listen(PORT, () => console.log(`webhook receiver listening on http://127.0.0.1:${PORT}/alert`));
