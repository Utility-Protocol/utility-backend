import 'dotenv/config';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import cors from 'cors';

import { closeDb, initDb } from './db/schema.js';
import { createRouter } from './api/routes.js';
import { attachWebSocket, broadcast } from './api/websocket.js';
import { startPoller } from './indexer/poller.js';

const PORT = Number(process.env.PORT ?? 4000);

export function createApp() {
  const app = express();
  app.use(cors());
  app.use(express.json());
  app.use('/', createRouter());
  return app;
}

function main() {
  initDb();
  const app = createApp();
  const server = http.createServer(app);
  const wss = attachWebSocket(server);
  const stopPoller = startPoller((payload) => broadcast(wss, payload));

  server.listen(PORT, () => {
    console.log(`[api] listening on http://localhost:${PORT}`);
    console.log(`[db] connected to ${process.env.DB_PATH ?? './data/indexer.db'}`);
  });

  let shuttingDown = false;
  function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[api] ${signal} received, shutting down gracefully...`);

    stopPoller();
    server.close(() => {
      wss.close();
      closeDb();
      process.exit(0);
    });

    setTimeout(() => {
      console.warn('[api] shutdown timed out; forcing exit');
      process.exit(1);
    }, 5000).unref();
  }

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

const isMain =
  typeof process.argv[1] === 'string' &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  main();
}