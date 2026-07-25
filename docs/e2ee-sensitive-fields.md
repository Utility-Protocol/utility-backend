# End-to-End Encryption for Sensitive Payload Fields

Sensitive payload fields are encrypted before a payload crosses a service boundary and are decrypted only by services that hold an authorized data-encryption key. The implementation uses an envelope per field so non-sensitive metadata remains queryable while confidential values stay opaque in logs, queues, and storage.

## Architecture

- `FieldEncryptor` walks JSON objects and arrays, transforming only configured sensitive field names.
- Each encrypted field is stored as an envelope containing `alg`, `version`, `key_id`, `nonce_hex`, and `ciphertext_hex`.
- AES-256-GCM provides confidentiality and integrity. The `key_id` is authenticated as additional data so envelopes cannot be silently rebound to a different key.
- Random 96-bit nonces are generated for each field encryption, which keeps repeated plaintext values from producing deterministic ciphertext.
- Older decryption keys can be registered alongside the primary key to support blue-green deploys and key rotation canaries.

## Operational targets

- Keep encryption on the application edge and before durable persistence to avoid plaintext exposure in downstream systems.
- Monitor `utility_e2ee_field_latency_seconds` for the `< 100ms` P99 target on critical paths.
- Alert on `utility_e2ee_field_operations_total{status="failure"}` increases because failures indicate malformed envelopes, key mismatch, or tampering.
- During blue-green deployment, deploy readers with old and new keys first, switch writers to the new primary key, canary traffic, then retire the old key after stored envelopes have aged out or been rewrapped.

## Security review checklist

1. Confirm sensitive field allowlists cover regulatory identifiers, payment/account fields, precise location, and settlement wallet destinations.
2. Confirm keys are sourced from an approved KMS or secret manager in production and never logged.
3. Confirm logs and metrics include field counts and status only, not plaintext or ciphertext bodies.
4. Confirm decryption is limited to services with a documented business need.
