import { readFileSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

import sdk from '@stellar/stellar-sdk';
import { TransactionBuilder, Operation, Keypair, xdr, StrKey, hash, Address } from '@stellar/stellar-sdk';

const rpcNs = sdk.SorobanRpc ?? sdk.rpc;

const [,, wasmPath, secret, outPath] = process.argv;
const rpcUrl = process.env.E2E_RPC_URL ?? 'https://soroban-testnet.stellar.org';
const NETWORK = process.env.E2E_NETWORK_PASSPHRASE ?? 'Test SDF Network ; September 2015';

const server = new rpcNs.Server(rpcUrl, { allowHttp: true });
const kp = Keypair.fromSecret(secret);
const publicKey = kp.publicKey();
const wasm = readFileSync(wasmPath);

async function rawGetTransaction(txHash) {
  const resp = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getTransaction', params: { hash: txHash } }),
  });
  const j = await resp.json();
  return j.result;
}

async function sendTx(build) {
  const account = await server.getAccount(publicKey);
  const tx = await build(account);
  const sim = await server.simulateTransaction(tx);
  if (!sim.result) throw new Error('simulate failed: ' + JSON.stringify(sim));
  const built = rpcNs.assembleTransaction(tx, sim).build();
  built.sign(kp);
  const sent = await server.sendTransaction(built);
  if (sent.status !== 'PENDING' && sent.status !== 'DUPLICATE') {
    throw new Error('send failed: ' + JSON.stringify(sent));
  }
  let res;
  let i = 0;
  do {
    await new Promise((r) => setTimeout(r, 2000));
    res = await rawGetTransaction(sent.hash);
    i++;
    if (i > 30) throw new Error('tx timeout: ' + JSON.stringify(res));
  } while (res.status === 'NOT_FOUND');
  if (res.status !== 'SUCCESS') throw new Error('tx not success: ' + JSON.stringify(res));
  return res;
}

// 1. upload wasm
await sendTx((account) =>
  new TransactionBuilder(account, { fee: '100', networkPassphrase: NETWORK })
    .addOperation(
      Operation.invokeHostFunction({
        func: xdr.HostFunction.hostFunctionTypeUploadContractWasm(wasm),
        auth: [],
      })
    )
    .setTimeout(30)
    .build()
);
const wasmId = hash(wasm);
console.log('uploaded, wasmId hash:', wasmId.toString('hex').slice(0, 24) + '...');

// 2. create contract from address+salt
const salt = createHash('sha256').update('utility-e2e').digest();
const scAddress = new Address(publicKey).toScAddress();
const preimage = xdr.ContractIdPreimage.contractIdPreimageFromAddress(
  new xdr.ContractIdPreimageFromAddress({ address: scAddress, salt })
);

await sendTx((account) =>
  new TransactionBuilder(account, { fee: '100', networkPassphrase: NETWORK })
    .addOperation(
      Operation.invokeHostFunction({
        func: xdr.HostFunction.hostFunctionTypeCreateContract(
          new xdr.CreateContractArgs({
            contractIdPreimage: preimage,
            executable: xdr.ContractExecutable.contractExecutableWasm(Buffer.from(wasmId)),
          })
        ),
        auth: [],
      })
    )
    .setTimeout(30)
    .build()
);
console.log('createContract done');

const networkId = hash(Buffer.from(NETWORK, 'utf8'));
const idPreimage = xdr.HashIdPreimage.envelopeTypeContractId(
  new xdr.HashIdPreimageContractId({
    networkId: Buffer.from(networkId),
    contractIdPreimage: preimage,
  })
);
const contractId = StrKey.encodeContract(hash(idPreimage.toXDR()));
console.log('contractId:', contractId);

// 3. validate derived id against RPC
try {
  const w = await server.getContractWasmByContractId(contractId);
  console.log('validated, wasm bytes:', w.byteLength);
} catch (e) {
  console.log('validation error (id may be wrong):', String(e.message || e).slice(0, 200));
}

writeFileSync(outPath, contractId + '\n');