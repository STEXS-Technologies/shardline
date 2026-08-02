# shardline-xet-core

Core Xet data structures: Merkle hash trees, metadata shard format, xorb object
serialization, and compression schemes. Implements the binary format for
metadata shards (file records, chunk references), xorb objects (chunk grouping,
packing, compression), and Merkle hash tree computation. Content-defined
chunking itself lives in `shardline-server`'s upload ingest, not in this
crate. These are the foundational data formats that the xet adapter and
server operate on.

See the [main Shardline README](../../README.md) for the project overview.
