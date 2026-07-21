# shardline-protocol-adapters

Protocol adapter implementations for Git LFS and Bazel HTTP Remote Cache frontends.
Translates LFS Batch API requests and Bazel AC/CAS requests into CAS coordinator
calls, enabling chunk-level deduplication for Git LFS and Bazel clients. These
adapters bridge external protocol semantics to the internal CAS storage model.

See the [main Shardline README](../../README.md) for the project overview.
