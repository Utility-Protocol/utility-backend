import { applyTick, getCursor, setCursor } from '../db/schema.js';
import { broadcastEvent } from '../api/websocket.js';

const TOPIC_UTIL_TICK = 'util_tick';

/**
 * Decodes a raw Soroban contract event into a flat tick payload.
 *
 * The contract publishes:
 *   topics: (util_tick, consumer, resource_type)
 *   data:   (operator, units_drawn, total_cost, timestamp)
 *
 * Event shapes differ between RPC versions, so both the SDK-parsed form
 * ({type, value}) and the bare form are handled.
 */
export function decodeUtilTick(event) {
  const topics = event.topics ?? [];
  if (topics.length < 3) return null;

  const name = readSymbol(topics[0]);
  if (name !== TOPIC_UTIL_TICK) return null;

  const data = unwrap(event.data);
  if (!Array.isArray(data) || data.length < 4) return null;

  const [operator, units, cost, timestamp] = data;

  return {
    eventId: event.id ?? event.txHash ?? `${name}-${timestamp}`,
    consumer: readAddress(topics[1]),
    resourceType: readSymbol(topics[2]),
    operator: readAddress(operator),
    unitsDrawn: Number(units ?? 0),
    cost: String(cost ?? 0),
    timestamp: Number(timestamp ?? 0),
    meterSequence: Number(event.meterSequence ?? 0),
  };
}

function unwrap(v) {
  // SDK 13 wraps event data as {type: 'Vec', value: [...]} in some versions.
  if (v && typeof v === 'object' && 'value' in v && !Array.isArray(v)) return v.value;
  return v;
}

function readSymbol(v) {
  const raw = unwrap(v);
  if (typeof raw === 'string') return raw;
  if (raw && typeof raw === 'object' && 'sym' in raw) {
    // Soroban RPC encodes ScSymbol as a hex string.
    const bytes = Buffer.from(raw.sym, 'hex');
    return bytes.length ? bytes.toString('utf8').replace(/\0+$/, '') : '';
  }
  return '';
}

function readAddress(v) {
  const raw = unwrap(v);
  if (typeof raw === 'string') return raw;
  if (raw && typeof raw === 'object' && 'address' in raw) return String(raw.address);
  return '';
}

/**
 * Polls the Soroban RPC for `util_tick` events and indexes each one.
 * `startCursor` / `pollFn` are injectable so the loop can be tested offline.
 */
export function createPoller({
  rpcUrl,
  contractId,
  intervalMs = 2000,
  startCursor = 'now',
  pollFn,
  onEvent = broadcastEvent,
  log = console,
} = {}) {
  let cursor = startCursor === 'now' ? null : String(startCursor);
  let timer = null;
  let stopped = false;

  async function pollOnce() {
    const params = {
      contractIds: [contractId],
      topics: [],
      pagination: { limit: 200 },
    };
    if (cursor) params.cursor = cursor;

    const res = await pollFn(rpcUrl, params);

    const events = res?.events ?? [];
    for (const event of events) {
      const tick = decodeUtilTick(event);
      if (!tick) continue;
      try {
        applyTick(tick);
        onEvent({ type: 'UTIL_TICK', data: publicFrame(tick) });
      } catch (err) {
        // Replays and duplicates are expected in a lagging indexer.
        if (err.code === 'REPLAY' || err.code === 'DUPLICATE_EVENT') continue;
        throw err;
      }
    }

    if (events.length) {
      cursor = res.cursor || cursor;
      if (cursor) setCursor(`${contractId}:cursor`, cursor);
    }

    return { indexed: events.length, cursor };
  }

  async function tick() {
    if (stopped) return;
    try {
      await pollOnce();
    } catch (err) {
      log.error('[poller] poll failed:', err.message);
    }
    if (!stopped) timer = setTimeout(tick, intervalMs);
  }

  return {
    start() {
      stopped = false;
      tick();
    },
    stop() {
      stopped = true;
      if (timer) clearTimeout(timer);
      timer = null;
    },
    pollOnce,
    getCursor: () => cursor,
    resumeCursor: () => getCursor(`${contractId}:cursor`),
  };
}

export function publicFrame(tick) {
  return {
    event_id: tick.eventId,
    consumer: tick.consumer,
    operator: tick.operator,
    resource_type: tick.resourceType,
    units_drawn: tick.unitsDrawn,
    cost: tick.cost,
    meter_sequence: tick.meterSequence,
    timestamp: tick.timestamp,
  };
}