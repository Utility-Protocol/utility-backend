import http from 'node:http';
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';

import { closeDb } from './db/schema.js';
import { apiRouter, healthRouter } from './api/routes.js';
import { mockRouter } from './api/mock_telemetry.js';
import { closeWebSocket, initWebSocket } from './api/websocket.js';
import { createPoller } from './indexer/poller.js';

dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '4000', 10),
  rpcUrl: process.env.SOROBAN_RPC_URL || 'https://soroban-testnet.stellar.org:443',
  contractId: process.env.CONTRACT_ID || 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
  pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS || '2000', 10),
  dbPath: process.env.DB_PATH || './data/utility_indexer.db',
};

async function defaultPollFn(rpcUrl, params) {
  const res = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'getEvents',
      params,
    }),
  });
  if (!res.ok) throw new Error(`RPC responded ${res.status}`);
  const body = await res.json();
  if (body.error) throw new Error(`RPC error: ${body.error.message}`);
  return body.result;
}

/** Builds the express app and attaches the websocket server. */
export function createApp({ pollFn = defaultPollFn, startPoller = false } = {}) {
  const app = express();
  app.use(cors());
  app.use(express.json({ limit: '256kb' }));

  const poller = createPoller({
    rpcUrl: config.rpcUrl,
    contractId: config.contractId,
    intervalMs: config.pollIntervalMs,
    pollFn,
  });

  app.use(healthRouter(poller));
  app.use('/api', apiRouter);

  if (process.env.NODE_ENV !== 'production') {
    app.use('/api/telemetry', mockRouter);
  }

  // JSON 404 + error handler
  app.use('/api', (_req, res) => res.status(404).json({ error: 'Not found' }));
  app.use((err, _req, res, _next) => {
    console.error('[api] unhandled error:', err);
    res.status(500).json({ error: 'Internal server error' });
  });

  const server = http.createServer(app);
  initWebSocket(server);

  if (startPoller) poller.start();

  return { app, server, poller };
}

const isEntrypoint = process.argv[1] && process.argv[1].endsWith('src/index.js');

if (isEntrypoint) {
  const { server, poller } = createApp();

  server.listen(config.port, () => {
    console.log(`[utility-backend] listening on :${config.port}`);
    console.log(`[utility-backend] rpc    ${config.rpcUrl}`);
    console.log(`[utility-backend] contract ${config.contractId}`);
    console.log(`[utility-backend] db     ${config.dbPath}`);
    poller.start();
  });

  const shutdown = (signal) => {
    console.log(`[utility-backend] ${signal} received, shutting down`);
    poller.stop();
    closeWebSocket();
    server.close(() => {
      closeDb();
      process.exit(0);
    });
    setTimeout(() => process.exit(1), 5000).unref();
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}