# Configuration Dashboard

Track these Prometheus metrics on the operations dashboard:

- `utility_config_reload_success_total`: successful initial loads and reloads.
- `utility_config_reload_failure_total`: rejected reload attempts or file stat failures.
- `utility_config_schema_version`: active schema version across instances.

Suggested alert: page when reload failures increase during a rollout and warn when instances in the same deployment report different schema versions for more than 10 minutes.
