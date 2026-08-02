# CDC Chunking and Storage Representation

## Overview

Content-defined chunking (CDC) replaces fixed-size chunking for all non-Xet ingest
paths (LFS, Bazel, OCI, Hub). Instead of splitting uploads at fixed byte offsets,
boundaries are chosen by content, so identical data inserted at different offsets
produces the same chunk hashes and deduplicates.

## Chunker

The chunker is `CdcChunker` in `crates/shardline-server/src/upload_ingest/cdc.rs`.
It is a FastCDC-style gear-hash chunker: the same algorithm and gear table family
used by `gearhash-0.1.3` and xet-data's deduplication chunker.

### Parameters

| Parameter | Value | Notes |
|---|---|---|
| target_chunk_size | Configurable, default 64 KiB | Must be a power of two and > 64 |
| min_chunk_size | target / 8 | Prevents tiny chunks |
| max_chunk_size | target * 2 | Hard upper bound |
| window_size | 64 bytes | Gear-hash scanning window |

### Configuration

- `SHARDLINE_CHUNK_SIZE` (or legacy alias `SHARDLINE_CHUNK_SIZE_BYTES`) sets the
  target chunk size as a byte-size string (for example `64KB`, `1MB`).
  Default: `64KB`. Maximum: `1GiB`.
- There is no `DedupStrategy` enum and no `SHARDLINE_DEDUP_STRATEGY` variable:
  CDC is the single chunking mode for the non-Xet ingest paths.

## Storage representation

Each stored file record carries an explicit `storage_repr` field:

- `fixed_chunk_v1` — uncompressed fixed-size chunks (legacy; also the default for
  records written before the field existed).
- `xorb_cdc_v1` — CDC chunks, each LZ4-compressed with `lz4_flex::compress_prepend_size`
  (a 4-byte u32 LE size header), optionally packed into xorb containers at upload
  finish.

Chunk hashes are always computed over raw bytes, so dedup works across compressed
and uncompressed records.

## Read path

Decompression is decided by `storage_repr`, not by comparing stored size to record
length: `xorb_cdc_v1` records are always LZ4-decompressed, even when the compressed
size equals the raw size (small or incompressible payloads). `fixed_chunk_v1`
records are served raw. Xorb-backed files use a single-GET fast path.

## Garbage collection

All three formats are handled:

- Individual chunk paths and xorb container paths are referenced by the GC.
- Xorb containers are resolved to their constituent chunk hashes.
- Xorb cache sidecar files are deleted when their parent xorb is swept.

## Compatibility

No data migration is required: old records remain readable, reconstructable, and
protected from garbage collection.
