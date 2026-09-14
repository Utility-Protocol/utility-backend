#!/usr/bin/env bash
# End-to-end integration test for the Utility Protocol stack.
#
# Verifies the complete flow against live testnet:
#   1. builds the Soroban contract wasm
#   2. funds a fresh testnet account (friendbot)
#   3. deploys the contract and runs the full lifecycle
#      (initialize / register_device / deposit / submit_reading)
#   4. starts the backend indexer and waits for the emitted "billed" event
#   5. starts the frontend dashboard and checks it renders the telemetry
#
# Exit code 0 = PASS, non-zero = FAIL.
#
# Config (env overrides, all optional):
#   CONTRACTS_DIR   path to Utility-contracts   (default: ../Utility-contracts)
#   FRONTEND_DIR    path to utility-frontend    (default: ../utility-frontend)
#   BACKEND_PORT    backend API port            (default: 4100)
#   FRONTEND_PORT   frontend HTTP port          (default: 3100)
#   E2E_RPC_URL     Soroban RPC endpoint        (default: soroban-testnet)
#   E2E_NETWORK_PASSPHRASE                      (default: testnet)
#   KEEP=1         keep temp files / DB for inspection

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONTRACTS_DIR="${CONTRACTS_DIR:-$BACKEND_DIR/../Utility-contracts}"
FRONTEND_DIR="${FRONTEND_DIR:-$BACKEND_DIR/../utility-frontend}"
BACKEND_PORT="${BACKEND_PORT:-4100}"
FRONTEND_PORT="${FRONTEND_PORT:-3100}"
E2E_RPC_URL="${E2E_RPC_URL:-https://soroban-testnet.stellar.org}"
E2E_NETWORK_PASSPHRASE="${E2E_NETWORK_PASSPHRASE:-Test SDF Network ; September 2015}"

TMPDIR_E2E="$(mktemp -d /tmp/utility-e2e.XXXXXX)"
DB_PATH="$TMPDIR_E2E/indexer.db"
WASM_PATH="$TMPDIR_E2E/utility-contracts.wasm"
CONTRACT_ID_FILE="$TMPDIR_E2E/contract-id.txt"
ACCOUNT_FILE="$TMPDIR_E2E/account.env"
BACKEND_LOG="$TMPDIR_E2E/backend.log"
FRONTEND_LOG="$TMPDIR_E2E/frontend.log"

BACKEND_PID=""
FRONTEND_PID=""
FAILURES=()

cleanup() {
  for pid in "$BACKEND_PID" "$FRONTEND_PID"; do
    [ -n "$pid" ] || continue
    kill "$pid" 2>/dev/null || true
    pkill -P "$pid" 2>/dev/null || true
  done
  sleep 1
  for pid in "$BACKEND_PID" "$FRONTEND_PID"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  if [ "${KEEP:-}" != "1" ]; then
    rm -rf "$TMPDIR_E2E"
  else
    echo ""
    echo "[e2e] keeping artifacts in $TMPDIR_E2E"
  fi
}
trap cleanup EXIT

step() { echo ""; echo "=== $* ==="; }
pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*"; FAILURES+=("$*"); }

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[FATAL] missing required command: $1" >&2
    exit 1
  fi
}

curl_once() { curl -s -m 20 "$@"; }

for cmd in node curl stellar cargo; do require_cmd "$cmd"; done

step "Config"
echo "  backend:    $BACKEND_DIR"
echo "  contracts:  $CONTRACTS_DIR"
echo "  frontend:   $FRONTEND_DIR"
echo "  rpc:        $E2E_RPC_URL"
echo "  backend:    :$BACKEND_PORT  frontend: :$FRONTEND_PORT"

step "1/5 Build contract wasm"
mkdir -p "$(dirname "$WASM_PATH")"
if [ -n "${E2E_SKIP_BUILD:-}" ]; then
  cp "$CONTRACTS_DIR/target/wasm32-unknown-unknown/release/utility_contracts.wasm" "$WASM_PATH"
elif [ -f "$CONTRACTS_DIR/target/wasm32-unknown-unknown/release/utility_contracts.wasm" ]; then
  cp "$CONTRACTS_DIR/target/wasm32-unknown-unknown/release/utility_contracts.wasm" "$WASM_PATH"
else
  (cd "$CONTRACTS_DIR" && cargo build --release --target wasm32-unknown-unknown) > /dev/null
  cp "$CONTRACTS_DIR/target/wasm32-unknown-unknown/release/utility_contracts.wasm" "$WASM_PATH"
fi
[ -s "$WASM_PATH" ] && pass "wasm ready ($(stat -c %s "$WASM_PATH") bytes)" || fail "wasm build missing"

step "2/5 Fund fresh testnet account"
ACCOUNT_META="$(node -e "const { Keypair } = require('@stellar/stellar-sdk'); const kp = Keypair.random(); process.stdout.write(kp.secret() + ' ' + kp.publicKey());")"
SECRET="${ACCOUNT_META%% *}"
PUBLIC="${ACCOUNT_META##* }"
echo "  account: $PUBLIC"
TRIES=0
FUNDED=0
until [ "$FUNDED" = "1" ]; do
  TRIES=$((TRIES + 1))
  RESP="$(curl_once "https://friendbot.stellar.org?addr=$PUBLIC" || true)"
  if printf '%s' "$RESP" | grep -qE '"successful"\s*:\s*true'; then
    FUNDED=1
  elif [ "$TRIES" -gt 8 ]; then
    fail "friendbot funding (final response: $(printf '%s' "$RESP" | head -c 200))"
    break
  else
    echo "  friendbot retry $TRIES..."; sleep 3
  fi
done
printf 'SECRET=%s\nPUBLIC=%s\n' "$SECRET" "$PUBLIC" > "$ACCOUNT_FILE"
[ "$FUNDED" = "1" ] && pass "account funded"

step "3/5 Deploy contract + run lifecycle"
node "$SCRIPT_DIR/deploy.mjs" "$WASM_PATH" "$SECRET" "$CONTRACT_ID_FILE"
CONTRACT_ID="$(cat "$CONTRACT_ID_FILE")"
echo "  contract: $CONTRACT_ID"
pass "deployed"

INVOKE=(stellar contract invoke --id "$CONTRACT_ID" --source "$SECRET"
  --rpc-url "$E2E_RPC_URL" --network-passphrase "$E2E_NETWORK_PASSPHRASE")

"${INVOKE[@]}" -- initialize --admin "$PUBLIC" > /dev/null && pass "initialize"
"${INVOKE[@]}" -- register_device --device_id "$PUBLIC" --owner "$PUBLIC" --rate_per_unit 100 > /dev/null && pass "register_device"
"${INVOKE[@]}" -- deposit --device_id "$PUBLIC" --amount 1000000 > /dev/null && pass "deposit"
READING_UNITS=250
"${INVOKE[@]}" -- submit_reading --device_id "$PUBLIC" --delta_units "$READING_UNITS" > /dev/null && pass "submit_reading ($READING_UNITS units)"

step "4/5 Start backend and wait for event ingestion"
(
  cd "$BACKEND_DIR"
  exec env PORT="$BACKEND_PORT" \
    SOROBAN_RPC_URL="$E2E_RPC_URL" \
    CONTRACT_ID="$CONTRACT_ID" \
    POLL_INTERVAL_MS=2000 \
    DB_PATH="$DB_PATH" \
    node src/index.js
) > "$BACKEND_LOG" 2>&1 &
BACKEND_PID=$!

for i in $(seq 1 30); do
  sleep 2
  DEVICES="$(
    curl_once "http://localhost:$BACKEND_PORT/api/devices" 2>/dev/null \
      | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{const j=JSON.parse(s);console.log((j.devices||[]).filter(d=>d.id==='$PUBLIC').length)}catch{console.log(0)}})" 2>/dev/null
  )"
  [ "$DEVICES" = "1" ] && break
done
if [ "$DEVICES" = "1" ]; then
  pass "device indexed"
else
  fail "device not indexed within timeout (see $BACKEND_LOG)"
fi

SUMMARY="$(curl_once "http://localhost:$BACKEND_PORT/api/metrics/summary" 2>/dev/null || true)"
echo "  summary: $SUMMARY"
EXPECTED_REVENUE=$((READING_UNITS * 100))
if echo "$SUMMARY" | grep -qE "\"total_revenue_billed\":$EXPECTED_REVENUE"; then
  pass "revenue matches reading ($READING_UNITS x 100 = $EXPECTED_REVENUE)"
else
  fail "summary revenue unexpected; got $SUMMARY"
fi

READINGS="$(curl_once "http://localhost:$BACKEND_PORT/api/devices/$PUBLIC/readings" 2>/dev/null || true)"
if echo "$READINGS" | grep -q "\"delta_units\":$READING_UNITS"; then
  pass "reading record present"
else
  fail "reading not exposed by API; got $READINGS"
fi

step "5/5 Build and start frontend, verify dashboard"
(
  cd "$FRONTEND_DIR"
  [ -d node_modules ] || npm install --no-audit --no-fund > /dev/null 2>&1
  NEXT_PUBLIC_BACKEND_API_URL="http://localhost:$BACKEND_PORT" \
  NEXT_PUBLIC_BACKEND_WS_URL="ws://localhost:$BACKEND_PORT/stream" \
  NEXT_PUBLIC_SOROBAN_RPC_URL="$E2E_RPC_URL" \
  NEXT_PUBLIC_CONTRACT_ID="$CONTRACT_ID" \
  NEXT_PUBLIC_STELLAR_NETWORK_PASSPHRASE="$E2E_NETWORK_PASSPHRASE" \
    npx next build > /dev/null 2>&1
  exec env NEXT_PUBLIC_BACKEND_API_URL="http://localhost:$BACKEND_PORT" \
    NEXT_PUBLIC_BACKEND_WS_URL="ws://localhost:$BACKEND_PORT/stream" \
    NEXT_PUBLIC_SOROBAN_RPC_URL="$E2E_RPC_URL" \
    NEXT_PUBLIC_CONTRACT_ID="$CONTRACT_ID" \
    NEXT_PUBLIC_STELLAR_NETWORK_PASSPHRASE="$E2E_NETWORK_PASSPHRASE" \
    npx next start -p "$FRONTEND_PORT"
) > "$FRONTEND_LOG" 2>&1 &
FRONTEND_PID=$!

for i in $(seq 1 120); do
  sleep 2
  CODE="$(curl_once -o /tmp/e2e-page.html -w '%{http_code}' "http://localhost:$FRONTEND_PORT/" 2>/dev/null || true)"
  [ "$CODE" = "200" ] && break
done
if [ "$CODE" = "200" ]; then
  pass "frontend serves HTTP 200"
  grep -q "Utility" /tmp/e2e-page.html && pass "dashboard rendered" || fail "dashboard content missing"
else
  fail "frontend not serving (status $CODE); see $FRONTEND_LOG"
fi

echo ""
if [ "${#FAILURES[@]}" -eq 0 ]; then
  echo "== E2E PASS =="
  echo "  contract: $CONTRACT_ID"
  exit 0
else
  for f in "${FAILURES[@]}"; do echo "  FAILED: $f"; done
  echo "== E2E FAIL =="
  exit 1
fi