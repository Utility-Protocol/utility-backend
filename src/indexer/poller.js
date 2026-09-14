import { rpc, StrKey } from '@stellar/stellar-sdk';
import * as store from '../db/schema.js';

const CONTRACT_ID = process.env.CONTRACT_ID;
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 3000);
const POLL_LIMIT = Number(process.env.POLL_LIMIT ?? 100);
const START_LEDGER_WINDOW = Number(process.env.START_LEDGER_WINDOW ?? 20);
const BILLED_TOPIC = 'billed';

let running = false;
let timer = null;
let server = null;
let ingest = null;

export function getServer() {
  if (!server) {
    server = new rpc.Server(process.env.SOROBAN_RPC_URL, { allowHttp: true });
  }
  return server;
}

export function startPoller(broadcastCallback) {
  if (running) return () => {};
  if (!CONTRACT_ID || !process.env.SOROBAN_RPC_URL) {
    console.warn(
      '[indexer] CONTRACT_ID and/or SOROBAN_RPC_URL not configured; polling disabled.',
    );
    return () => {};
  }
  running = true;
  console.log(
    `[indexer] polling events for contract ${CONTRACT_ID} every ${POLL_INTERVAL_MS}ms`,
  );
  scheduleNext(broadcastCallback);
  return () => {
    running = false;
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
  };
}

function scheduleNext(broadcastCallback) {
  if (!running) return;
  timer = setTimeout(async () => {
    try {
      await pollEvents(broadcastCallback);
    } catch (err) {
      console.error(
        '[indexer] poll cycle failed:',
        err instanceof Error ? err.message : err,
      );
    } finally {
      scheduleNext(broadcastCallback);
    }
  }, POLL_INTERVAL_MS);
}

export async function pollEvents(broadcastCallback) {
  const rpcServer = getServer();

  const request = {
    filters: [{ type: 'contract', contractIds: [CONTRACT_ID] }],
    limit: POLL_LIMIT,
  };

  const cursor = store.getLatestCursor();
  if (cursor) {
    request.cursor = cursor;
  } else {
    const latest = await rpcServer.getLatestLedger();
    request.startLedger = Math.max(latest.sequence - START_LEDGER_WINDOW, 1);
  }

  const { events } = await rpcServer.getEvents(request);

  for (const event of events) {
    if (event.inSuccessfulContractCall === false) continue;

    let decoded;
    try {
      decoded = decodeBilledEvent(event);
    } catch (err) {
      console.error(
        '[indexer] failed to decode event',
        event.id,
        err instanceof Error ? err.message : err,
      );
      continue;
    }
    if (!decoded) continue;

    ingestReading(decoded);
    if (typeof broadcastCallback === 'function') {
      broadcastCallback({ type: 'METER_READING', data: toWebPayload(decoded) });
    }
  }

  if (events.length > 0) {
    const last = events[events.length - 1];
    store.setLatestCursor(last.paging_token ?? last.id);
  }
}

function ingestReading(decoded) {
  if (!ingest) {
    const db = store.initDb();
    ingest = db.transaction((reading) => {
      store.upsertDevice({ id: reading.device_id });
      const inserted = store.insertReading({
        event_id: reading.event_id,
        device_id: reading.device_id,
        delta_units: reading.delta_units,
        delta_cost: reading.delta_cost,
        ledger_timestamp: reading.timestamp,
      });
      if (inserted) {
        store.bumpDeviceUsage(reading.device_id, reading.delta_units, reading.timestamp);
      }
    });
  }
  ingest(decoded);
}

function toSafeNumber(value) {
  const num = Number(value);
  return Number.isSafeInteger(num) ? num : value.toString();
}

function toWebPayload(decoded) {
  return {
    device_id: decoded.device_id,
    delta_units: toSafeNumber(decoded.delta_units),
    delta_cost: decoded.delta_cost,
    timestamp: toSafeNumber(decoded.timestamp),
  };
}

export function decodeBilledEvent(rpcEvent) {
  const contractEvent = rpcEvent.event;
  let topics;
  let dataValue;

  if (contractEvent) {
    const v0 = contractEvent.body().v0();
    topics = v0.topics();
    dataValue = v0.data();
  } else {
    topics = rpcEvent.topic;
    dataValue = rpcEvent.value;
  }

  if (!topics || topics.length < 2) return null;
  if (!isSymbol(topics[0], BILLED_TOPIC)) return null;

  const deviceId = addressFromScVal(topics[1]);

  const dataParts = vecOf(dataValue);
  if (!dataParts || dataParts.length !== 3) return null;

  return {
    event_id: String(rpcEvent.paging_token ?? rpcEvent.id ?? ''),
    device_id: deviceId,
    delta_units: u64ToBigInt(dataParts[0]),
    delta_cost: i128ToBigInt(dataParts[1]).toString(),
    timestamp: u64ToBigInt(dataParts[2]),
    cursor: rpcEvent.paging_token ?? null,
  };
}

function isSymbol(scVal, expected) {
  if (!scVal || typeof scVal.sym !== 'function') return false;
  try {
    return scVal.sym().toString() === expected;
  } catch {
    return false;
  }
}

function vecOf(scVal) {
  if (!scVal || typeof scVal.vec !== 'function') return null;
  try {
    return scVal.vec();
  } catch {
    return null;
  }
}

function u64ToBigInt(scVal) {
  if (!scVal || typeof scVal.u64 !== 'function') {
    throw new Error('expected u64 scVal');
  }
  return BigInt(scVal.u64().toString());
}

function i128ToBigInt(scVal) {
  if (!scVal || typeof scVal.i128 !== 'function') {
    throw new Error('expected i128 scVal');
  }
  const parts = scVal.i128();
  const hi = BigInt(parts.hi().toString());
  const lo = BigInt(parts.lo().toString());
  return BigInt.asIntN(128, (hi << 64n) | lo);
}

export function addressFromScVal(scVal) {
  if (!scVal || typeof scVal.address !== 'function') {
    throw new Error('expected address scVal');
  }
  const scAddress = scVal.address();
  const arm = scAddress.arm();

  if (arm === 'accountId') {
    const pk = scAddress.accountId().ed25519();
    return StrKey.encodeEd25519PublicKey(pk);
  }

  if (arm === 'contractId') {
    return StrKey.encodeContract(scAddress.contractId());
  }

  throw new Error(`unsupported ScAddress arm: ${arm}`);
}