import { Router } from 'express';
import { applyTick, DEFAULT_INITIAL_ESCROW, ReplayError, DuplicateEventError } from '../db/schema.js';
import { broadcastEvent } from './websocket.js';
import { publicFrame } from '../indexer/poller.js';

export const mockRouter = Router();

/** Fallback tariff used when the caller does not supply an explicit cost. */
const DEFAULT_RATE_PER_UNIT = BigInt(process.env.DEFAULT_RATE_PER_UNIT || '1000');

/**
 * POST /api/telemetry/mock-tick
 *
 * Simulates on-chain `util_tick` intake so the frontend and e2e harness can be
 * exercised without a funded ledger. Runs through the same `applyTick` path as
 * the real poller, so the replay guard behaves identically.
 */
mockRouter.post('/mock-tick', (req, res) => {
  if (process.env.NODE_ENV === 'production') {
    return res.status(403).json({
      error: 'Mock telemetry ingestion is disabled in production environments.',
    });
  }

  const {
    consumer,
    operator,
    resource_type: resourceType = 'SOLAR',
    units_drawn: unitsDrawn,
    meter_sequence: meterSequence,
    cost,
    timestamp = Math.floor(Date.now() / 1000),
    event_id: eventId = `sim-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
  } = req.body ?? {};

  if (!consumer || !operator) {
    return res.status(400).json({
      error: 'Missing required fields: consumer, operator',
    });
  }

  // Validate numerically BEFORE any BigInt conversion; BigInt(NaN) throws and
  // would otherwise surface as a 500 instead of a 400.
  const units = Number(unitsDrawn);
  const sequence = Number(meterSequence);

  if (!Number.isInteger(units) || units <= 0) {
    return res.status(400).json({
      error: 'units_drawn must be a positive integer',
      received: unitsDrawn,
    });
  }
  if (!Number.isInteger(sequence) || sequence <= 0) {
    return res.status(400).json({
      error: 'meter_sequence must be a positive integer',
      received: meterSequence,
    });
  }
  if (!Number.isInteger(Number(timestamp))) {
    return res.status(400).json({ error: 'timestamp must be an integer' });
  }

  let costBig;
  try {
    costBig = BigInt(cost ?? BigInt(units) * DEFAULT_RATE_PER_UNIT);
  } catch {
    return res.status(400).json({ error: 'cost must be a base-10 integer string', received: cost });
  }
  if (costBig < 0n) {
    return res.status(400).json({ error: 'cost must not be negative' });
  }

  try {
    const { balance } = applyTick({
      eventId,
      consumer,
      operator,
      resourceType,
      unitsDrawn: units,
      cost: costBig.toString(),
      meterSequence: sequence,
      timestamp: Number(timestamp),
    });

    broadcastEvent({
      type: 'UTIL_TICK',
      data: publicFrame({
        eventId,
        consumer,
        operator,
        resourceType,
        unitsDrawn: units,
        cost: costBig.toString(),
        meterSequence: sequence,
        timestamp: Number(timestamp),
      }),
    });

    return res.status(201).json({
      status: 'ingested',
      event_id: eventId,
      consumer,
      operator,
      cost: costBig.toString(),
      meter_sequence: sequence,
      escrow_balance: balance,
    });
  } catch (err) {
    if (err instanceof ReplayError) {
      return res.status(409).json({ error: err.message, event_id: eventId });
    }
    if (err instanceof DuplicateEventError) {
      return res
        .status(409)
        .json({ error: 'Duplicate event_id detected. Event already indexed.', event_id: eventId });
    }
    console.error('[mock-telemetry] ingest failed:', err);
    return res.status(500).json({ error: 'Internal database error during mock tick ingestion.' });
  }
});

export { DEFAULT_INITIAL_ESCROW };