# Cache Adapters

Shardline can cache reconstruction responses so repeated downloads do not need to
rebuild the same file plan from durable metadata every time.

This cache is optional acceleration.
It is never authoritative state.

## Goals

Cache adapters exist to:

- reduce repeated reconstruction lookup cost
- absorb hot read traffic for the same file version
- keep cache policy independent from metadata and object storage
- allow operators to choose local memory, a Redis-compatible shared cache, or no cache

## Guarantees

Shardline treats reconstruction cache entries as disposable.

Required behavior:

- a cache miss must fall back to durable metadata
- a cache adapter failure must not corrupt or block correct reconstruction results
- latest-file lookups must not return stale reconstruction state after a write
- immutable version-specific cache entries may be reused safely

Current behavior:

- immutable version-specific reconstruction responses are cacheable
- latest-file reconstruction responses bypass the cache until explicit latest-key
  invalidation is available for every metadata adapter

## Supported Adapters

### Disabled

Use this mode when operators want the simplest deployment shape.

```text
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=disabled
```

### In-Memory

The in-memory adapter is bounded and TTL-based.
It is the default.

```text
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory
SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS=30
SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES=4096
```

This mode works well for:

- single-node deployments
- local development
- small self-hosted installations

The memory cache is process-local.
Entries are lost on restart.

### Redis-Compatible Caches

Use the `redis` cache adapter when reconstruction cache state should survive process
restarts or be shared across multiple Shardline instances.
Shardline speaks the Redis protocol for its shared reconstruction cache.
Garnet is one deployment option and is the one used in the shipped local and Kubernetes
examples because it performs well for this workload.

```text
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=redis
SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL=redis://default:dev_password@garnet:6379
SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS=30
```

Redis-compatible caches improve cache reuse across horizontally scaled API nodes while
keeping the cache separate from durable metadata.

## Cache Keys

Shardline keys reconstruction cache entries by:

- file identifier
- optional immutable content hash
- optional repository scope

That keeps:

- repository-scoped reconstructions isolated
- immutable file versions safe to reuse
- latest-file cache keys separate from immutable version entries when latest caching is
  enabled

## Operational Guidance

Choose adapters by deployment profile:

- `disabled`: lowest operational complexity
- `memory`: fast local default with bounded memory use
- `redis`: shared Redis-compatible cache for multiple API nodes

If a shared Redis-compatible cache is unavailable, Shardline should still be able to
serve reconstructions from durable metadata, though with higher latency.
