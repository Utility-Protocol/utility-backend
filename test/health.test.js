import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import { xdr } from '@stellar/stellar-sdk';

import { closeDb, initDb, upsertDevice, insertReading, bumpDeviceUsage, getDevice, getDeviceReadings, listDevices, getLatestCursor, setLatestCursor, getMetricsSummary } from '../src/db/schema.js';
import { createApp } from '../src/index.js';
import { decodeBilledEvent } from '../src/indexer/poller.js';

const app = createApp();

before(() => {
  initDb(':memory:');
});

after(() => {
  closeDb();
});

describe('GET /health', () => {
  it('returns 200 with status ok', async () => {
    const res = await request(app).get('/health');
    assert.equal(res.status, 200);
    assert.equal(res.body.status, 'ok');
    assert.equal(typeof res.body.uptime, 'number');
  });
});

describe('Database methods', () => {
  const testDevice = {
    id: 'CATESTDEVICE123456789012345678901234567890',
    owner: 'GOWNER1234567890123456789012345678901234567890123',
    rate_per_unit: '100',
    deposit_balance: '5000',
    total_units_consumed: 0,
    last_seen_timestamp: 0,
  };

  it('upserts a device and retrieves it', () => {
    upsertDevice(testDevice);
    const device = getDevice(testDevice.id);
    assert.ok(device, 'device should exist');
    assert.equal(device.id, testDevice.id);
    assert.equal(device.owner, testDevice.owner);
    assert.equal(device.rate_per_unit, '100');
    assert.equal(device.deposit_balance, '5000');
    assert.equal(device.total_units_consumed, 0);
  });

  it('inserts a meter reading and marks duplicate as ignored', () => {
    const reading1 = {
      event_id: 'event-001',
      device_id: testDevice.id,
      delta_units: 10n,
      delta_cost: '1000',
      ledger_timestamp: 1000,
    };
    assert.equal(insertReading(reading1), true, 'first insert should succeed');

    const reading2 = {
      event_id: 'event-001',
      device_id: testDevice.id,
      delta_units: 10n,
      delta_cost: '1000',
      ledger_timestamp: 1000,
    };
    assert.equal(insertReading(reading2), false, 'duplicate event_id should be ignored');
  });

  it('returns readings ordered by id descending (newest first)', () => {
    insertReading({
      event_id: 'event-002',
      device_id: testDevice.id,
      delta_units: 5n,
      delta_cost: '500',
      ledger_timestamp: 2000,
    });
    const readings = getDeviceReadings(testDevice.id, 10, 0);
    assert.ok(Array.isArray(readings));
    assert.equal(readings.length, 2);
    assert.equal(readings[0].event_id, 'event-002');
    assert.equal(readings[1].event_id, 'event-001');
  });

  it('respects limit and offset query params', () => {
    const page1 = getDeviceReadings(testDevice.id, 1, 0);
    assert.equal(page1.length, 1);
    assert.equal(page1[0].event_id, 'event-002');

    const page2 = getDeviceReadings(testDevice.id, 1, 1);
    assert.equal(page2.length, 1);
    assert.equal(page2[0].event_id, 'event-001');
  });

  it('lists all devices', () => {
    const devices = listDevices();
    assert.ok(Array.isArray(devices));
    assert.equal(devices.length, 1);
    assert.equal(devices[0].id, testDevice.id);
  });

  it('stores and retrieves the latest cursor', () => {
    assert.equal(getLatestCursor(), null);
    setLatestCursor('page-123-456-789');
    assert.equal(getLatestCursor(), 'page-123-456-789');
    setLatestCursor('page-999');
    assert.equal(getLatestCursor(), 'page-999');
  });

  it('computes the metrics summary', () => {
    bumpDeviceUsage(testDevice.id, 15n, 2000);
    const summary = getMetricsSummary();
    assert.equal(summary.active_devices, 1);
    assert.equal(summary.total_units_consumed, 15);
    assert.equal(summary.total_revenue_billed, 1500);
  });
});

describe('REST endpoints', () => {
  const deviceId = 'CAPI_ENDPOINT_DEVICE';

  before(() => {
    upsertDevice({ id: deviceId, owner: 'GOWNER_API_TEST', rate_per_unit: '200', deposit_balance: '10000' });
    insertReading({
      event_id: 'api-event-001',
      device_id: deviceId,
      delta_units: 50n,
      delta_cost: '10000',
      ledger_timestamp: 3000,
    });
  });

  it('GET /api/devices returns a list', async () => {
    const res = await request(app).get('/api/devices');
    assert.equal(res.status, 200);
    assert.ok(Array.isArray(res.body.devices));
    assert.ok(res.body.devices.length >= 2);
  });

  it('GET /api/devices/:id returns device details', async () => {
    const res = await request(app).get(`/api/devices/${deviceId}`);
    assert.equal(res.status, 200);
    assert.equal(res.body.id, deviceId);
    assert.equal(res.body.rate_per_unit, 200);
  });

  it('GET /api/devices/:id returns 404 for unknown device', async () => {
    const res = await request(app).get('/api/devices/NONEXISTENT');
    assert.equal(res.status, 404);
  });

  it('GET /api/devices/:id/readings returns readings', async () => {
    const res = await request(app).get(`/api/devices/${deviceId}/readings`);
    assert.equal(res.status, 200);
    assert.equal(res.body.device_id, deviceId);
    assert.ok(Array.isArray(res.body.readings));
    assert.equal(res.body.readings.length, 1);
  });

  it('GET /api/metrics/summary returns summary', async () => {
    const res = await request(app).get('/api/metrics/summary');
    assert.equal(res.status, 200);
    assert.equal(typeof res.body.total_units_consumed, 'number');
    assert.equal(typeof res.body.active_devices, 'number');
  });
});

function buildSyntheticEvent({ pk, deltaUnits, deltaCost, timestamp }) {
  const accountId = new xdr.AccountId('publicKeyTypeEd25519', pk);
  const scAddress = xdr.ScAddress.scAddressTypeAccount(accountId);
  const topics = [
    xdr.ScVal.scvSymbol('billed'),
    xdr.ScVal.scvAddress(scAddress),
  ];
  const data = [
    xdr.ScVal.scvU64(new xdr.Uint64(String(deltaUnits))),
    xdr.ScVal.scvI128(new xdr.Int128Parts({
      hi: new xdr.Uint64(String(BigInt(deltaCost) >> 64n)),
      lo: new xdr.Uint64(String(BigInt(deltaCost) & 0xFFFFFFFFFFFFFFFFn)),
    })),
    xdr.ScVal.scvU64(new xdr.Uint64(String(timestamp))),
  ];
  const v0 = new xdr.ContractEventV0({ topics, data: xdr.ScVal.scvVec(data) });
  const body = new xdr.ContractEventBody(0, v0);

  return {
    event: new xdr.ContractEvent({
      ext: new xdr.ExtensionPoint(0),
      contractId: Buffer.alloc(32, 3),
      type: xdr.ContractEventType.diagnostic(),
      body,
    }),
    paging_token: 'ledger-tx-index',
    inSuccessfulContractCall: true,
  };
}

describe('XDR event decoding', () => {
  it('decodes a synthetic billed event from XDR', () => {
    const pk = Buffer.alloc(32, 7);
    const fakeEvent = buildSyntheticEvent({
      pk,
      deltaUnits: 42,
      deltaCost: '5000',
      timestamp: 1700000000,
    });
    const decoded = decodeBilledEvent(fakeEvent);
    assert.ok(decoded);
    assert.equal(typeof decoded.device_id, 'string');
    assert.equal(decoded.device_id.startsWith('G'), true);
    assert.equal(decoded.delta_units, 42n);
    assert.equal(decoded.delta_cost, '5000');
    assert.equal(decoded.timestamp, 1700000000n);
    assert.equal(decoded.cursor, 'ledger-tx-index');
  });

  it('rejects events with a non-billed topic', () => {
    const pk = Buffer.alloc(32, 1);
    const accountId = new xdr.AccountId('publicKeyTypeEd25519', pk);
    const scAddress = xdr.ScAddress.scAddressTypeAccount(accountId);
    const topics = [xdr.ScVal.scvSymbol('something-else'), xdr.ScVal.scvAddress(scAddress)];
    const data = xdr.ScVal.scvVec([
      xdr.ScVal.scvU64(new xdr.Uint64('1')),
      xdr.ScVal.scvI128(new xdr.Int128Parts({ hi: new xdr.Uint64('0'), lo: new xdr.Uint64('1') })),
      xdr.ScVal.scvU64(new xdr.Uint64('1')),
    ]);
    const v0 = new xdr.ContractEventV0({ topics, data });
    const body = new xdr.ContractEventBody(0, v0);
    const rpcEvent = {
      event: new xdr.ContractEvent({
        ext: new xdr.ExtensionPoint(0),
        contractId: Buffer.alloc(32, 3),
        type: xdr.ContractEventType.diagnostic(),
        body,
      }),
      paging_token: 'tok',
      inSuccessfulContractCall: true,
    };
    assert.equal(decodeBilledEvent(rpcEvent), null);
  });
});