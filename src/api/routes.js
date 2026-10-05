import { Router } from 'express';
import {
  getConsumer,
  getConsumerHistory,
  getGridSummary,
  getOperatorStats,
} from '../db/schema.js';
import { clientCount } from './websocket.js';

export const apiRouter = Router();

apiRouter.get('/grid/metrics', (_req, res) => {
  res.json(getGridSummary());
});

apiRouter.get('/consumers/:address', (req, res) => {
  const { address } = req.params;
  const consumer = getConsumer(address);
  if (!consumer) {
    return res.status(404).json({ error: 'Consumer not indexed', address });
  }
  const limit = Math.min(parseInt(req.query.limit, 10) || 50, 500);
  return res.json({
    address: consumer.address,
    escrow_balance: consumer.escrow_balance,
    total_units_consumed: consumer.total_units_consumed,
    last_meter_sequence: consumer.last_meter_sequence,
    last_seen: consumer.last_seen,
    history: getConsumerHistory(address, limit),
  });
});

apiRouter.get('/operators/:address', (req, res) => {
  const { address } = req.params;
  const operator = getOperatorStats(address);
  if (!operator) {
    return res.status(404).json({ error: 'Operator not indexed', address });
  }
  return res.json({
    address: operator.address,
    resource_type: operator.resource_type,
    staked_amount: operator.staked_amount,
    total_earned: operator.total_earned,
  });
});

export function healthRouter(poller) {
  const router = Router();

  router.get('/health', (_req, res) => {
    let rpcReachable = false;
    let cursorLag = null;
    try {
      if (poller?.rpcUrl) {
        rpcReachable = true;
      }
    } catch {
      rpcReachable = false;
    }

    res.json({
      status: 'ok',
      rpc_reachable: rpcReachable,
      cursor: poller?.getCursor?.() ?? null,
      cursor_lag: cursorLag,
      ws_clients: clientCount(),
      uptime_s: Math.round(process.uptime()),
    });
  });

  return router;
}