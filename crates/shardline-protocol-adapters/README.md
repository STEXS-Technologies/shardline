# shardline-protocol-adapters

Shared Xet wire contracts and protocol adapter implementations for Git LFS and
Bazel HTTP Remote Cache frontends.
Translates LFS Batch API requests and Bazel AC/CAS requests into CAS coordinator
calls, enabling chunk-level deduplication for Git LFS and Bazel clients. These
adapters bridge external protocol semantics to the internal CAS storage model.

Disable default features to use Xet wire types, route constants and reconstruction
version conversion without server authorization, database or object-store
dependencies. The default `storage` feature preserves the LFS and Bazel adapters.

See the [main Shardline README](../../README.md) for the project overview.
