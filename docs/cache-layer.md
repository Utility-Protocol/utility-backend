# Cache Layer Architecture

The cache layer provides a system-wide cache-aside abstraction with an in-process tier and an optional Redis tier. Critical paths read the local memory tier first, then Redis, and only fall back to the backing service or database after both tiers miss.

## Configuration

`src/config/default.toml` defines the default cache settings:

- `default_ttl_ms`: default expiration for entries that do not pass an operation-specific TTL.
- `max_entries`: maximum in-process entries retained before the earliest-expiring entries are evicted.
- `redis_url`: Redis endpoint for the shared tier.
- `namespace`: key prefix used to isolate environments and services.

## Runtime Behavior

1. `CacheLayer::get` checks the in-memory tier.
2. On memory miss, the Redis tier is checked when configured.
3. Redis hits are promoted back into memory using the configured default TTL.
4. `CacheLayer::set` writes through to memory and Redis with the same TTL.
5. `CacheLayer::delete` invalidates both tiers.

## Monitoring and Alerts

The layer emits Prometheus counters for hit and miss totals by tier:

- `utility_cache_hits_total{tier="memory|redis"}`
- `utility_cache_misses_total{tier="memory|redis"}`

Recommended alerts:

- Page if Redis miss rate exceeds the service SLO baseline for 10 minutes.
- Warn if memory hit rate drops below the expected canary baseline after deployment.
- Page if Redis connectivity errors cause sustained request latency above the 100ms P99 target.

## Deployment Runbook

Use blue-green deployment with canary analysis:

1. Deploy the cache-enabled build to the green environment with Redis configured.
2. Send 5% canary traffic and compare P99 latency, error rate, cache hit ratio, and Redis CPU/memory usage.
3. Increase traffic to 25%, 50%, then 100% when metrics stay within SLO thresholds.
4. Roll back by routing traffic to blue and disabling `redis_url` if cache dependency health degrades.
