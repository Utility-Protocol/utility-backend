# Dashboard: Distributed Job Scheduler

Recommended panels:

- Enqueued, claimed, completed, and failed jobs from `utility_job_scheduler_*_total`.
- Lease heartbeat rate from `utility_job_scheduler_heartbeats_total`.
- Claim critical-path latency P50/P95/P99 once the production store exports operation histograms.
- Queue depth by queue and age of oldest due job from the backing store.

Use queue and worker labels in the production store exporter to separate noisy queues from
system-wide availability indicators.
