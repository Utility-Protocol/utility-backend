import { mkdirSync } from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS devices (
  id                    TEXT PRIMARY KEY,
  owner                 TEXT    NOT NULL DEFAULT '',
  rate_per_unit         TEXT    NOT NULL DEFAULT '0',
  deposit_balance       TEXT    NOT NULL DEFAULT '0',
  total_units_consumed  INTEGER NOT NULL DEFAULT 0,
  last_seen_timestamp   INTEGER NOT NULL DEFAULT 0,
  updated_at            DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meter_readings (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id          TEXT UNIQUE NOT NULL,
  device_id         TEXT NOT NULL REFERENCES devices(id),
  delta_units       INTEGER NOT NULL,
  delta_cost        TEXT    NOT NULL,
  ledger_timestamp  INTEGER NOT NULL,
  created_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_meter_readings_device
  ON meter_readings(device_id);

CREATE TABLE IF NOT EXISTS sync_state (
  key   TEXT PRIMARY KEY,
  value TEXT
);
`;

const CURSOR_KEY = 'latest_cursor';

let db = null;
let stmts = null;

export function initDb(dbPath) {
  if (db) return db;

  const requested = dbPath ?? process.env.DB_PATH ?? './data/indexer.db';
  if (requested === ':memory:') {
    db = new Database(':memory:');
  } else {
    const resolved = path.resolve(requested);
    mkdirSync(path.dirname(resolved), { recursive: true });
    db = new Database(resolved);
  }

  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.exec(SCHEMA_SQL);
  prepareStatements();
  return db;
}

export function closeDb() {
  if (db) {
    db.close();
    db = null;
    stmts = null;
  }
}

function prepareStatements() {
  stmts = {
    upsertDevice: db.prepare(`
      INSERT INTO devices (
        id, owner, rate_per_unit, deposit_balance,
        total_units_consumed, last_seen_timestamp
      ) VALUES (
        @id, @owner, @rate_per_unit, @deposit_balance,
        @total_units_consumed, @last_seen_timestamp
      )
      ON CONFLICT(id) DO UPDATE SET
        owner                  = excluded.owner,
        rate_per_unit          = excluded.rate_per_unit,
        deposit_balance        = excluded.deposit_balance,
        updated_at             = CURRENT_TIMESTAMP
    `),
    getDevice: db.prepare('SELECT * FROM devices WHERE id = ?'),
    listDevices: db.prepare('SELECT * FROM devices ORDER BY updated_at DESC'),
    insertReading: db.prepare(`
      INSERT OR IGNORE INTO meter_readings (
        event_id, device_id, delta_units, delta_cost, ledger_timestamp
      ) VALUES (
        @event_id, @device_id, @delta_units, @delta_cost, @ledger_timestamp
      )
    `),
    getDeviceReadings: db.prepare(`
      SELECT id, event_id, device_id, delta_units, delta_cost, ledger_timestamp, created_at
      FROM meter_readings
      WHERE device_id = @device_id
      ORDER BY id DESC
      LIMIT @limit OFFSET @offset
    `),
    bumpDeviceUsage: db.prepare(`
      UPDATE devices
      SET total_units_consumed = total_units_consumed + @delta_units,
          last_seen_timestamp  = @timestamp,
          updated_at           = CURRENT_TIMESTAMP
      WHERE id = @device_id
    `),
    getCursor: db.prepare('SELECT value FROM sync_state WHERE key = ?'),
    setCursor: db.prepare(`
      INSERT INTO sync_state (key, value) VALUES (@key, @value)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `),
    sumUnits: db.prepare('SELECT COALESCE(SUM(total_units_consumed), 0) AS total FROM devices'),
    countActiveDevices: db.prepare(
      'SELECT COUNT(*) AS count FROM devices WHERE total_units_consumed > 0'
    ),
    allDeltaCosts: db.prepare('SELECT delta_cost FROM meter_readings'),
  };
}

function toSqliteInt(value) {
  const raw = typeof value === 'bigint' ? value : BigInt(value);
  if (raw <= BigInt(Number.MAX_SAFE_INTEGER) && raw >= BigInt(Number.MIN_SAFE_INTEGER)) {
    return Number(raw);
  }
  return raw.toString();
}

function parseIo128(value) {
  const raw = String(value);
  if (!/^-?\d+$/.test(raw)) return raw;
  const num = Number(raw);
  return Number.isSafeInteger(num) ? num : raw;
}

export function upsertDevice({
  id,
  owner = '',
  rate_per_unit = 0,
  deposit_balance = 0,
  total_units_consumed = 0,
  last_seen_timestamp = 0,
} = {}) {
  return stmts.upsertDevice.run({
    id: String(id),
    owner: String(owner),
    rate_per_unit: String(rate_per_unit),
    deposit_balance: String(deposit_balance),
    total_units_consumed: toSqliteInt(total_units_consumed),
    last_seen_timestamp: toSqliteInt(last_seen_timestamp),
  });
}

export function insertReading({ event_id, device_id, delta_units, delta_cost, ledger_timestamp }) {
  const result = stmts.insertReading.run({
    event_id: String(event_id),
    device_id: String(device_id),
    delta_units: toSqliteInt(delta_units),
    delta_cost: String(delta_cost),
    ledger_timestamp: toSqliteInt(ledger_timestamp),
  });
  return result.changes > 0;
}

export function bumpDeviceUsage(deviceId, deltaUnits, timestamp) {
  return stmts.bumpDeviceUsage.run({
    device_id: String(deviceId),
    delta_units: toSqliteInt(deltaUnits),
    timestamp: toSqliteInt(timestamp),
  });
}

export function getDevice(id) {
  return stmts.getDevice.get(String(id)) ?? null;
}

export function listDevices() {
  return stmts.listDevices.all();
}

export function getDeviceReadings(deviceId, limit = 50, offset = 0) {
  return stmts.getDeviceReadings.all({
    device_id: String(deviceId),
    limit: toSqliteInt(limit),
    offset: toSqliteInt(offset),
  });
}

export function getLatestCursor() {
  const row = stmts.getCursor.get(CURSOR_KEY);
  return row ? row.value : null;
}

export function setLatestCursor(cursor) {
  if (cursor === null || cursor === undefined) return;
  stmts.setCursor.run({ key: CURSOR_KEY, value: String(cursor) });
}

export function getTotalRevenueBilled() {
  return stmts.allDeltaCosts.all().reduce((acc, row) => acc + BigInt(row.delta_cost), 0n);
}

export function getMetricsSummary() {
  const { total } = stmts.sumUnits.get();
  const { count } = stmts.countActiveDevices.get();
  return {
    total_units_consumed: total,
    total_revenue_billed: parseIo128(getTotalRevenueBilled()),
    active_devices: count,
  };
}