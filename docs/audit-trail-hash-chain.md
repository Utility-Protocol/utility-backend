# Audit Trail with Tamper-Evident Hash Chain Verification

## Architecture

The audit trail is an append-only, system-wide ledger for security-relevant events emitted by API, ingestion, settlement, identity, gateway, and storage services. Each event stores canonical metadata, a SHA-256 hash of the serialized payload, the previous event hash, and its own SHA-256 event hash. The first record uses the all-zero genesis previous hash.

Critical paths only calculate a payload hash and append one row, keeping the target below 100ms P99. Verification runs asynchronously or on demand and walks ordered events to prove that every record is contiguous and that each stored hash matches the canonical fields.

## Event hash input

The canonical event hash includes:

1. sequence number
2. occurrence timestamp in nanoseconds
3. actor
4. service
5. action
6. resource
7. payload hash
8. previous hash

Fields are separated with an ASCII unit separator to avoid ambiguous concatenation.

## Monitoring and alerting

Prometheus metrics:

- `utility_audit_events_verified_total` tracks the number of events successfully checked.
- `utility_audit_verification_failures_total{reason}` tracks tamper evidence, sequence gaps, and broken hash links.

Alert when any verification failure occurs over a five-minute window; page security and freeze destructive maintenance until the chain head is reconciled.

## Deployment

Use blue-green deployment:

1. Apply `db/audit_events.sql` to both blue and green databases.
2. Deploy writers in shadow mode to green and compare audit event counts.
3. Enable canary traffic for one service at a time.
4. Run hash-chain verification after each canary step.
5. Promote green only when verification succeeds and P99 latency remains under 100ms.

## Runbook

If verification fails:

1. Capture the verification report and the reported `first_invalid_sequence`.
2. Stop audit compaction/export jobs.
3. Compare the suspect row against the prior row's `hash` and the event payload source.
4. If `previous_hash` is wrong, inspect concurrent writers for sequence allocation bugs.
5. If `hash` is wrong, treat the row as tampered or corrupted and start incident response.
6. Preserve database snapshots and application logs for security review.
