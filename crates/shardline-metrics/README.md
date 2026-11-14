# shardline-metrics

Prometheus metrics instrumentation for storage, transfer, protocol, and system
operations. Defines and registers metric counters, gauges, histograms, and
summaries across all server subsystems: CAS operations, HTTP request latency,
chunk transfers, reconstruction, GC/fsck runs, provider events, and backend
storage performance. Exposed at the `/metrics` endpoint.

Metric recording and Prometheus text encoding work without an HTTP framework.
The default `http` feature retains the public `middleware` module. Storage and
client libraries can use `default-features = false` when they only record or
encode metrics. Server crates enable `http` explicitly; metric names, registration,
and scrape output are shared.

See the [main Shardline README](../../README.md) for the project overview.
