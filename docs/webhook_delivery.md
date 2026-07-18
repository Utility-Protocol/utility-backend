# Webhook Delivery Service

## Architecture

Application services publish webhook events after their business transaction commits. The delivery worker reads those events from the outbox boundary, serializes the event payload, signs it with HMAC-SHA256, and sends it to each subscribed endpoint through the `WebhookTransport` abstraction.

The service keeps request critical paths fast by avoiding synchronous third-party webhook calls in user-facing API handlers. Delivery attempts are bounded by a retry policy so a failing downstream cannot monopolize worker capacity.

## Security

Every delivery includes an `x-utility-webhook-signature` header in the form `t=<unix timestamp>,v1=<hex hmac>`. Consumers should reject signatures outside the five-minute tolerance and compare the expected HMAC in constant time. Endpoint secrets must be rotated by accepting old and new secrets during the consumer migration window.

## Retry Policy

The default policy attempts delivery five times. HTTP `408`, `429`, and `5xx` responses are treated as transient. Other non-2xx statuses are permanent failures and should move to the dead-letter workflow.

## Monitoring and Alerts

Prometheus exports:

- `utility_webhook_deliveries_total{endpoint_id,status}` for success and failed outcomes.
- `utility_webhook_retries_total{endpoint_id}` for retry pressure.
- `utility_webhook_delivery_latency_seconds` for end-to-end latency.

Alert on a falling success rate, sustained retry growth, or latency regression during canary rollout.

## Deployment Runbook

1. Deploy the new worker pool with blue-green routing disabled.
2. Enable a small canary slice and compare delivery success, retry rate, and latency against the current pool.
3. Promote the green pool only when canary metrics remain healthy.
4. If failures spike, disable the canary, pause noisy endpoints, and replay dead-lettered events after downstream recovery.
