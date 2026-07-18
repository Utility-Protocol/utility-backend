# Runbook: Distributed Job Scheduler

## Alerts

- `JobSchedulerClaimLatencyHigh`: P99 claim latency exceeds 100 ms for 5 minutes.
- `JobSchedulerLeaseStealSpike`: reclaimed expired leases exceed baseline, indicating worker crashes
  or handler stalls.
- `JobSchedulerFailureSpike`: failed jobs increase faster than completed jobs for a queue.

## Blue-green and canary deployment

1. Deploy the green scheduler code with workers disabled.
2. Enable one canary worker per queue at 5% of normal concurrency.
3. Compare claim latency, failures, completed jobs, and expired-lease reclaims for 30 minutes.
4. Increase green concurrency to 50%, then 100%, while draining blue workers.
5. Roll back by disabling green workers; outstanding leases expire and blue can reclaim them.
