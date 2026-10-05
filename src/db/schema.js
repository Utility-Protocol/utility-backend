import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import dotenv from 'dotenv';

dotenv.config();

const DB_PATH = process.env.DB_PATH || './data/utility_indexer.db';

fs.mkdirSync(path.dirname(path.resolve(DB_PATH)), { recursive: true });

export const db = new Database(path.resolve(DB_PATH));

db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS consumers (
    address              TEXT PRIMARY KEY,
    escrow_balance       TEXT    NOT NULL DEFAULT '0',
    total_units_consumed INTEGER NOT NULL DEFAULT 0,
    last_meter_sequence  INTEGER NOT NULL DEFAULT 0,
    last_seen            INTEGER NOT NULL DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS operators (
    address       TEXT PRIMARY KEY,
    resource_type TEXT    NOT NULL DEFAULT 'UNKNOWN',
    staked_amount TEXT    NOT NULL DEFAULT '0',
    total_earned  TEXT    NOT NULL DEFAULT '0'
  );

  CREATE TABLE IF NOT EXISTS utility_ticks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      TEXT    NOT NULL UNIQUE,
    consumer      TEXT    NOT NULL,
    operator      TEXT    NOT NULL,
    resource_type TEXT    NOT NULL,
    units_drawn   INTEGER NOT NULL,
    cost          TEXT    NOT NULL,
    meter_sequence INTEGER NOT NULL DEFAULT 0,
    timestamp     INTEGER NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_ticks_consumer ON utility_ticks(consumer, id DESC);
  CREATE INDEX IF NOT EXISTS idx_ticks_operator ON utility_ticks(operator, id DESC);

  CREATE TABLE IF NOT EXISTS sync_cursor (
    key   TEXT PRIMARY KEY,
    value TEXT
  );
`);

/** Reads a persisted cursor value, if any. */
export function getCursor(key) {
  const row = db.prepare('SELECT value FROM sync_cursor WHERE key = ?').get(key);
  return row ? row.value : null;
}

/** Persists a cursor value. */
export function setCursor(key, value) {
  db.prepare(
    'INSERT INTO sync_cursor (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
  ).run(key, String(value));
}

export class ReplayError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ReplayError';
    this.code = 'REPLAY';
  }
}

export class DuplicateEventError extends Error {
  constructor(message) {
    super(message);
    this.name = 'DuplicateEventError';
    this.code = 'DUPLICATE_EVENT';
  }
}

/** Escrow granted to a consumer the first time it is seen by the indexer. */
export const DEFAULT_INITIAL_ESCROW = process.env.DEFAULT_INITIAL_ESCROW || '10000000';

/**
 * Applies one settled meter tick: persists the tick row and moves escrow,
 * consumption, and operator earnings in a single transaction.
 *
 * Mirrors the contract's replay guard: `meterSequence` must be strictly
 * greater than the consumer's last accepted sequence, otherwise the tick is
 * rejected rather than silently clamped. Without this the indexer would
 * diverge from chain state after any duplicate or out-of-order delivery.
 */
export function applyTick({
  eventId,
  consumer,
  operator,
  resourceType = 'SOLAR',
  unitsDrawn,
  cost,
  meterSequence = 0,
  timestamp,
  initialEscrow = DEFAULT_INITIAL_ESCROW,
}) {
  const run = db.transaction(() => {
    const existing = db
      .prepare('SELECT last_meter_sequence FROM consumers WHERE address = ?')
      .get(consumer);

    if (existing && meterSequence > 0 && meterSequence <= existing.last_meter_sequence) {
      throw new ReplayError(
        `meter_sequence ${meterSequence} not greater than last accepted ${existing.last_meter_sequence}`
      );
    }

    insertTick({
      event_id: eventId,
      consumer,
      operator,
      resource_type: resourceType,
      units_drawn: unitsDrawn,
      cost: String(cost),
      meter_sequence: meterSequence,
      timestamp,
    });

    const costBig = BigInt(cost);
    const prev = getConsumer(consumer);
    const prevBalance = prev ? BigInt(prev.escrow_balance) : BigInt(initialEscrow);

    db.prepare(
      `INSERT INTO consumers (address, escrow_balance, total_units_consumed, last_meter_sequence, last_seen)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(address) DO UPDATE SET
         escrow_balance      = ?,
         total_units_consumed = consumers.total_units_consumed + excluded.total_units_consumed,
         last_meter_sequence  = excluded.last_meter_sequence,
         last_seen            = excluded.last_seen`
    ).run(
      consumer,
      (prevBalance - costBig).toString(),
      unitsDrawn,
      meterSequence,
      timestamp,
      (prevBalance - costBig).toString()
    );

    const op = getOperatorStats(operator);
    db.prepare(
      `INSERT INTO operators (address, resource_type, staked_amount, total_earned)
       VALUES (?, ?, '0', ?)
       ON CONFLICT(address) DO UPDATE SET
         resource_type = excluded.resource_type,
         total_earned  = CAST(CAST(operators.total_earned AS INTEGER) + CAST(? AS INTEGER) AS TEXT)`
    ).run(
      operator,
      resourceType,
      costBig.toString(),
      op ? costBig.toString() : costBig.toString()
    );

    return { balance: (prevBalance - costBig).toString() };
  });

  try {
    return run();
  } catch (err) {
    if (err.code === 'SQLITE_CONSTRAINT_UNIQUE' || /UNIQUE/i.test(err.message || '')) {
      throw new DuplicateEventError(`duplicate event_id ${eventId}`);
    }
    throw err;
  }
}

export function upsertOperator({ address, resourceType, stakedAmount }) {
  db.prepare(
    `INSERT INTO operators (address, resource_type, staked_amount, total_earned)
     VALUES (?, ?, ?, '0')
     ON CONFLICT(address) DO UPDATE SET
       resource_type = excluded.resource_type,
       staked_amount = excluded.staked_amount`
  ).run(address, resourceType || 'UNKNOWN', String(stakedAmount ?? 0));
}

export function insertTick(tick) {
  db.prepare(
    `INSERT INTO utility_ticks
       (event_id, consumer, operator, resource_type, units_drawn, cost, meter_sequence, timestamp)
     VALUES
       (@event_id, @consumer, @operator, @resource_type, @units_drawn, @cost, @meter_sequence, @timestamp)`
  ).run(tick);
}

export function getConsumer(address) {
  return db.prepare('SELECT * FROM consumers WHERE address = ?').get(address) ?? null;
}

export function getConsumerHistory(address, limit = 50) {
  return db
    .prepare(
      `SELECT event_id, operator, resource_type, units_drawn, cost, meter_sequence, timestamp
       FROM utility_ticks WHERE consumer = ? ORDER BY id DESC LIMIT ?`
    )
    .all(address, limit);
}

export function getOperatorStats(address) {
  return db.prepare('SELECT * FROM operators WHERE address = ?').get(address) ?? null;
}

export function getGridSummary() {
  const consumers = db
    .prepare(
      `SELECT COUNT(*) AS active_consumers, COALESCE(SUM(total_units_consumed), 0) AS total_units
       FROM consumers`
    )
    .get();

  const operators = db
    .prepare(
      `SELECT COUNT(*) AS active_operators, COALESCE(SUM(total_earned), 0) AS total_earned
       FROM operators`
    )
    .get();

  const totals = db
    .prepare(
      `SELECT COUNT(*) AS total_ticks, COALESCE(SUM(units_drawn), 0) AS total_units,
              COALESCE(SUM(CAST(cost AS INTEGER)), 0) AS total_settled
       FROM utility_ticks`
    )
    .get();

  const byResource = db
    .prepare(
      `SELECT resource_type, COALESCE(SUM(units_drawn), 0) AS units
       FROM utility_ticks GROUP BY resource_type`
    )
    .all();

  return {
    ...totals,
    ...consumers,
    ...operators,
    total_settled: String(totals.total_settled),
    total_earned: String(operators.total_earned),
    operators: db.prepare('SELECT * FROM operators ORDER BY total_earned DESC').all(),
    distribution: Object.fromEntries(byResource.map((r) => [r.resource_type, r.units])),
  };
}

export function closeDb() {
  try {
    db.close();
  } catch {
    /* already closed */
  }
}