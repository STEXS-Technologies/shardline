# DuckDB integration

Issue: [#61](https://github.com/STEXS-Technologies/shardline/issues/61)

## Decision gate

Shardline does not embed DuckDB in the main API process. The first supported
capability is DuckDB as an external S3 client. This already provides SQL,
Parquet schema discovery, projection and predicate pushdown, globs, joins,
aggregates, and Parquet export while keeping analytical failures and resource
usage outside the server process. The real-client E2E lane validates this path.

The Hub now exposes a bounded native Arrow/Parquet query path for authorized
dataset previews. DuckDB remains external for full SQL. An optional
server-side DuckDB service remains a follow-up design. It must run
in a separate role or process and accept a structured, revision-pinned request;
raw SQL is not an API contract. Before implementation, benchmark that service
against native Arrow/Parquet readers and the external-client path.

## External DuckDB workflow

Create a repository-scoped Shardline token and configure DuckDB's `httpfs`
secret. The token is used as `KEY_ID`; the secret value is intentionally
unused by Shardline's documented S3 compatibility layer. Production endpoints
must use TLS.

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE SECRET shardline (
  TYPE S3,
  KEY_ID 'REPOSITORY_SCOPED_TOKEN',
  SECRET 'unused',
  ENDPOINT 'shardline.example:443',
  REGION 'us-east-1',
  URL_STYLE 'path',
  USE_SSL true
);

SELECT *
FROM read_parquet('s3://owner.dataset/data/train/*.parquet')
WHERE label = 'positive'
LIMIT 100;

COPY (
  SELECT id, count(*) AS rows
  FROM read_parquet('s3://owner.dataset/data/train/*.parquet')
  GROUP BY id
) TO 's3://owner.dataset/results/summary.parquet' (FORMAT PARQUET);
```

The bucket is the repository scope (`owner.dataset`), and every request is
authorized by the token. Use exact object paths when a revision-pinned file
identity is required; mutable globs are appropriate only for external ad-hoc
analysis.

## Server-side query boundary

The typed request contract is available as
`shardline_hub_api::query::DatasetQueryRequest`. If Hub previews later use an
isolated DuckDB query worker, the worker contract must
pin repository, immutable revision, split, and file SHA before execution and
allow only selected columns, validated predicates, bounded ordering/cursors,
limits, and a small aggregate allowlist. It must enforce read-only access,
deadlines, cancellation, memory/CPU/thread/scanned-byte/result-row limits,
bounded spill space, tenant admission control, and no unapproved network or
filesystem access. Metrics may record queue/execution time, ranges, bytes,
rows, and stable error classes, but never SQL, credentials, paths, or row data.

SQLite/Postgres remain authoritative for publication, authorization metadata,
coordination, and GC; DuckDB is analytical only.

The native endpoint is `POST /api/datasets/{namespace}/{repo}/query`. It pins
the request to an authorized immutable revision and exact file SHA, supports
selected columns, bounded pagination, allow-listed predicates, and bounded
aggregates. Parquet reads use range requests and enforce an 8 MiB request
chunk and 128 MiB scanned-byte budget; execution is moved to a blocking worker
with a 30-second deadline, an eight-query admission limit, and a 16 MiB result
limit. Prometheus exposes query counts, rejection/failure/cancellation
counters (including stable `deadline` and `client_disconnect` cancellation
reasons), range requests, queue/execution duration, scanned/reconstructed and
returned bytes and rows, plus explicit cache hit/miss counters (the native path
currently records a miss because it has no result cache). Results and errors
are bounded and do not expose SQL, credentials, or internal paths.

The native route is enabled by default for backwards-compatible deployments.
Set `SHARDLINE_ENABLE_DATASET_QUERY=false` (or `0`, `no`, or `off`) to omit
the route entirely while retaining the existing CSV/JSONL/Parquet preview
endpoints. CSV and JSONL previews use bounded range-backed line streaming as
well: they retain only a storage chunk and the current line, support deep
pagination, cap text scans at 128 MiB and individual lines at 8 MiB. This is
the supported opt-out when analytics should remain outside the Hub process.

## Design and benchmark gate

The selected production boundary for this release is external DuckDB over the
S3 frontend. This is the only path enabled by default and is covered by the
real-client E2E lane, including a Docker-backed Postgres + MinIO deployment
shape. A server-side worker is not enabled until it beats the
external path and a native range-backed reader on the same fixture. Record cold
and warm latency, peak RSS, source/reconstructed bytes, range count, backend
operations, spill bytes, and concurrent upload/download impact for: exact-file
schema reads, narrow projections, selective and non-selective filters, deep
pagination, multi-file globs, and Parquet export. Rollout requires no API
latency regression, bounded RSS, and passing cancellation, redaction, and
cross-repository authorization drills. Use `duckdb` plus the real-client E2E
fixture as the reproducible baseline; do not promote a sidecar based on
synthetic SQL-only benchmarks.

Run `cargo make shardline-bench-duckdb` with the six required `SHARDLINE_*`
variables documented in the script to record comparable native-versus-external
latencies. The harness configures DuckDB `httpfs` with the same scoped token;
set `SHARDLINE_S3_ENDPOINT` when the S3 endpoint differs from the Hub URL.
Keep the raw CSV with the rollout evidence; benchmark results are not treated
as a correctness or security test.
