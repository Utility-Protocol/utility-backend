import test from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';

// The DB module is a singleton; point it at a throwaway file before import.
process.env.DB_PATH = `./data/test-${process.pid}.db`;
process.env.NODE_ENV = 'test';

const { createApp } = await import('../src/index.js');
const { applyTick, getConsumer, getGridSummary, getOperatorStats, db, closeDb } = await import(
  '../src/db/schema.js'
);
const { decodeUtilTick } = await import('../src/indexer/poller.js');

const CONSUMER = 'GCONSUMER_TEST_ADDRESS_0001';
const OPERATOR = 'GOPERATOR_TEST_ADDRESS_0001';

test.before(() => {
  db.exec('DELETE FROM utility_ticks; DELETE FROM consumers; DELETE FROM operators;');
});

test.after(() => {
  db.exec('DELETE FROM utility_ticks; DELETE FROM consumers; DELETE FROM operators;');
  closeDb();
});

test('applyTick debits escrow and credits operator', () => {
  applyTick({
    eventId: 'evt-1',
    consumer: CONSUMER,
    operator: OPERATOR,
    resourceType: 'SOLAR',
    unitsDrawn: 25,
    cost: '25000',
    meterSequence: 1,
    timestamp: 1000,
  });

  const c = getConsumer(CONSUMER);
  assert.equal(c.total_units_consumed, 25);
  assert.equal(c.last_meter_sequence, 1);
  // initial escrow 10000000 - 25000
  assert.equal(c.escrow_balance, '9975000');

  const o = getOperatorStats(OPERATOR);
  assert.equal(o.total_earned, '25000');
  assert.equal(o.resource_type, 'SOLAR');
});

test('applyTick rejects replayed meter sequences', () => {
  assert.throws(
    () =>
      applyTick({
        eventId: 'evt-replay',
        consumer: CONSUMER,
        operator: OPERATOR,
        unitsDrawn: 25,
        cost: '25000',
        meterSequence: 1,
        timestamp: 1001,
      }),
    /not greater than last accepted/
  );
});

test('applyTick rejects duplicate event ids', () => {
  assert.throws(
    () =>
      applyTick({
        eventId: 'evt-1',
        consumer: CONSUMER,
        operator: OPERATOR,
        unitsDrawn: 1,
        cost: '1',
        meterSequence: 2,
        timestamp: 1002,
      }),
    /duplicate event_id/
  );
});

test('decodeUtilTick parses the 3-topic contract event', () => {
  const sym = (s) => ({ sym: Buffer.from(s).toString('hex'), type: 'symbol' });
  const decoded = decodeUtilTick({
    id: 'evt-x',
    topics: [sym('util_tick'), { address: CONSUMER }, sym('SOLAR')],
    data: [{ address: OPERATOR }, 25, '25000', 1700000000],
  });

  assert.equal(decoded.consumer, CONSUMER);
  assert.equal(decoded.operator, OPERATOR);
  assert.equal(decoded.resourceType, 'SOLAR');
  assert.equal(decoded.unitsDrawn, 25);
  assert.equal(decoded.cost, '25000');
  assert.equal(decoded.timestamp, 1700000000);
});

test('decodeUtilTick ignores non-util_tick topics', () => {
  const sym = (s) => ({ sym: Buffer.from(s).toString('hex'), type: 'symbol' });
  const decoded = decodeUtilTick({
    topics: [sym('other_event'), { address: CONSUMER }, sym('SOLAR')],
    data: [{ address: OPERATOR }, 1, '1', 1],
  });
  assert.equal(decoded, null);
});

test('GET /health responds', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app).get('/health');
  assert.equal(res.status, 200);
  assert.equal(res.body.status, 'ok');
});

test('GET /api/grid/metrics aggregates indexed state', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app).get('/api/grid/metrics');
  assert.equal(res.status, 200);
  assert.equal(res.body.total_ticks, 1);
  assert.equal(res.body.active_consumers, 1);
  assert.equal(res.body.active_operators, 1);
});

test('GET /api/consumers/:address returns escrow and history', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app).get(`/api/consumers/${CONSUMER}`);
  assert.equal(res.status, 200);
  assert.equal(res.body.total_units_consumed, 25);
  assert.equal(res.body.escrow_balance, '9975000');
  assert.equal(res.body.last_meter_sequence, 1);
  assert.equal(res.body.history.length, 1);
});

test('GET /api/consumers/unknown returns 404', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app).get('/api/consumers/GDOES_NOT_EXIST');
  assert.equal(res.status, 404);
});

test('GET /api/operators/:address returns earnings', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app).get(`/api/operators/${OPERATOR}`);
  assert.equal(res.status, 200);
  assert.equal(res.body.total_earned, '25000');
  assert.equal(res.body.resource_type, 'SOLAR');
});

test('POST /api/telemetry/mock-tick ingests and broadcasts', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app)
    .post('/api/telemetry/mock-tick')
    .send({
      consumer: CONSUMER,
      operator: OPERATOR,
      resource_type: 'SOLAR',
      units_drawn: 10,
      meter_sequence: 2,
      cost: '10000',
    });

  assert.equal(res.status, 201);
  assert.equal(res.body.status, 'ingested');

  const c = getConsumer(CONSUMER);
  assert.equal(c.total_units_consumed, 35);
  assert.equal(c.last_meter_sequence, 2);
  assert.equal(c.escrow_balance, '9965000');
});

test('POST /api/telemetry/mock-tick rejects out-of-order sequences with 409', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app)
    .post('/api/telemetry/mock-tick')
    .send({ consumer: CONSUMER, operator: OPERATOR, units_drawn: 5, meter_sequence: 1 });

  assert.equal(res.status, 409);
  assert.match(res.body.error, /not greater than last accepted/);
});

test('POST /api/telemetry/mock-tick rejects non-numeric units with 400', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app)
    .post('/api/telemetry/mock-tick')
    .send({ consumer: CONSUMER, operator: OPERATOR, units_drawn: 'abc', meter_sequence: 99 });

  assert.equal(res.status, 400);
  assert.match(res.body.error, /positive integer/);
});

test('POST /api/telemetry/mock-tick requires consumer and operator', async () => {
  const { app } = createApp({ startPoller: false });
  const res = await request(app)
    .post('/api/telemetry/mock-tick')
    .send({ units_drawn: 1, meter_sequence: 1 });

  assert.equal(res.status, 400);
  assert.match(res.body.error, /consumer, operator/);
});

test('getGridSummary reports per-resource distribution', () => {
  const summary = getGridSummary();
  assert.ok(summary.distribution.SOLAR > 0);
});