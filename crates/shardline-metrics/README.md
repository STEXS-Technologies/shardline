# shardline-metrics

Prometheus metrics instrumentation for storage, transfer, protocol, and system
operations. Defines and registers metric counters, gauges, histograms, and
summaries across all server subsystems: CAS operations, HTTP request latency,
chunk transfers, reconstruction, GC/fsck runs, provider events, and backend
storage performance. Exposed at the `/metrics` endpoint.

See the [main Shardline README](../../README.md) for the project overview.
