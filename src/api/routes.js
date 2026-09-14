import { Router } from 'express';
import * as store from '../db/schema.js';

function toApiDevice(row) {
  return {
    id: row.id,
    owner: row.owner,
    rate_per_unit: maybeNumber(row.rate_per_unit),
    deposit_balance: maybeNumber(row.deposit_balance),
    total_units_consumed: row.total_units_consumed,
    last_seen_timestamp: row.last_seen_timestamp,
    updated_at: row.updated_at,
  };
}

function maybeNumber(value) {
  const raw = String(value);
  if (!/^-?\d+$/.test(raw)) return raw;
  const num = Number(raw);
  return Number.isSafeInteger(num) ? num : raw;
}

function queryInt(raw, fallback, min, max) {
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed)) return fallback;
  if (min !== undefined && parsed < min) return min;
  if (max !== undefined && parsed > max) return max;
  return parsed;
}

export function createRouter() {
  const router = Router();

  router.get('/health', (_req, res) => {
    res.json({ status: 'ok', uptime: process.uptime() });
  });

  router.get('/api/devices', (_req, res) => {
    try {
      const devices = store.listDevices().map(toApiDevice);
      res.json({ devices });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.get('/api/devices/:id', (req, res) => {
    try {
      const device = store.getDevice(req.params.id);
      if (!device) {
        return res.status(404).json({ error: 'Device not found' });
      }
      res.json(toApiDevice(device));
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.get('/api/devices/:id/readings', (req, res) => {
    try {
      const limit = queryInt(req.query.limit, 50, 1, 200);
      const offset = queryInt(req.query.offset, 0, 0);
      const readings = store.getDeviceReadings(req.params.id, limit, offset);
      res.json({ device_id: req.params.id, limit, offset, readings });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.get('/api/metrics/summary', (_req, res) => {
    try {
      res.json(store.getMetricsSummary());
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  return router;
}