# Fsck

`shardline fsck` verifies that stored metadata still matches the immutable chunk bytes
it references.

It exists for operators who need a direct answer to one question:

can this deployment root still serve the file versions it claims to have?

## Command

Current command:

```bash
shardline fsck
```

Run the command from the project or deployment directory.
Shardline uses the nearest `.shardline/data` state root automatically.
Use `--root <path>` only when operating on a different state root.

When `SHARDLINE_INDEX_POSTGRES_URL` is set, the command reads file-record metadata from
Postgres while validating chunk and retained-shard objects through the configured
object-store adapter.
Local deployments use the resolved state root for local metadata.

The command exits with:

- `0` when no integrity issues were found
- `1` when the scan completed and found one or more integrity issues
- `2` when the scan could not complete because of an operational failure

## Current Checks

The checker scans file records through the configured record-store adapter:

- local deployments use SQLite-backed record rows in `.shardline/data/metadata.sqlite3`
- Postgres-backed deployments use `shardline_file_records`

Object validation is performed through the configured object-store adapter.

For each record it verifies:

- the record JSON is readable
- the record path matches its declared file ID and content hash
- the record reconstruction plan remains structurally valid
- chunk offsets are contiguous
- chunk terms are non-empty
- the logical byte count matches the recorded chunk lengths
- native Xet chunk ranges and packed byte ranges stay ordered and non-empty
- the record content hash matches the recorded reconstruction terms
- every referenced chunk file exists
- every referenced chunk body hashes to the recorded chunk hash
- every referenced chunk body length matches the recorded chunk length
- native Xet records still have the unpacked chunk objects implied by their referenced
  xorbs
- every visible latest record still has a matching immutable version record
- every visible latest record still matches the immutable version record it points at
- quarantine metadata points at existing objects, retains the observed object length,
  and does not target currently reachable live objects
- active retention holds point at existing objects and do not coexist with quarantine
  state
- processed webhook delivery claims have plausible processing timestamps
- reconstruction rows reference registered xorbs and are not empty
- provider repository lifecycle state uses valid repository identity and plausible
  timestamps

For dedupe-shard mappings it also verifies:

- every indexed dedupe mapping points at an existing retained-shard object
- every retained-shard body can be parsed
- every parsed retained-shard actually contains the chunk hash claimed by the mapping

Retained-shard inspection uses the bounded shard parser directly and does not create a
local shard parsing workspace.
Object inventory and retained-shard reads go through the configured object-store
adapter.

## Output

The command prints a summary:

```text
root: /srv/assets/.shardline/data
latest_records: 12
version_records: 19
inspected_chunk_references: 94
inspected_dedupe_shard_mappings: 18
inspected_reconstructions: 12
inspected_webhook_deliveries: 12
inspected_provider_repository_states: 3
issue_count: 0
```

When issues are found, each issue is reported with a stable machine-readable kind:

```text
issue: missing_chunk location=/srv/assets/.shardline/data/chunks/ab/abcdef... detail=referenced by record record:latest:asset.bin
```

Issue kinds currently include:

- `invalid_record_json`
- `invalid_file_id`
- `invalid_content_hash`
- `record_path_mismatch`
- `non_contiguous_chunks`
- `empty_chunk`
- `total_bytes_mismatch`
- `invalid_chunk_range`
- `invalid_packed_range`
- `record_hash_mismatch`
- `missing_chunk`
- `chunk_hash_mismatch`
- `chunk_length_mismatch`
- `missing_version_record`
- `mismatched_version_record`
- `missing_dedupe_shard_object`
- `invalid_retained_shard`
- `invalid_dedupe_shard_mapping`
- `empty_reconstruction`
- `missing_reconstruction_xorb`
- `invalid_quarantine_candidate`
- `missing_quarantined_object`
- `quarantine_length_mismatch`
- `reachable_quarantined_object`
- `invalid_retention_hold`
- `missing_held_object`
- `held_quarantined_object`
- `invalid_webhook_delivery_timestamp`
- `invalid_provider_repository_state`
- `invalid_provider_repository_state_timestamp`

## Scope

The implementation validates file-record metadata through the configured record-store
adapter and validates chunk and retained-shard objects through the configured
object-store adapter.
It also validates reconstruction, lifecycle, webhook, retention, quarantine, dedupe, and
provider repository metadata through the configured index-store adapter.

`fsck` does not call external Git providers.
Provider repository state checks validate the durable lifecycle metadata already stored
by Shardline; live provider drift is handled by lifecycle reconciliation and repair
workflows.
