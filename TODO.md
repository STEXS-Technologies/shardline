# TODO

## Per-Frontend Dedup Strategy

**Status:** Proposed  
**Priority:** Medium  
**Effort:** ~300 lines  

### Motivation

Different workloads benefit from different deduplication strategies:
- HFT parquet updates → CDC (content-defined chunking) gives ~93% dedup
- Static binaries (ML models) → fixed-size chunking is fine
- Ephemeral temp files → no chunking saves CPU

### Design

Add a configurable `dedup_strategy` that can be set globally or per-frontend:

```toml
# Global default
dedup_strategy = "cdc"  # "none" | "fixed" | "cdc"

# Per-frontend override
[frontends.bazel-http]
dedup_strategy = "fixed"

[frontends.lfs]
dedup_strategy = "cdc"

[frontends.hub]
dedup_strategy = "none"
```

Env var: `SHARDLINE_DEDUP_STRATEGY=cdc`

### Strategies

| Strategy | Behavior | Chunk Size Meaning | Dedup Quality | CPU Cost |
|---|---|---|---|---|
| `none` | Store whole file as one blob | N/A | Only identical files | None |
| `fixed` | Split at every N bytes (current) | Hard boundary | Position-sensitive | None |
| `cdc` | Split at content-defined boundaries | Target/average size | Position-independent | Medium |

### Implementation Plan

1. Add `DedupStrategy` enum to `shardline-server-core`
2. Add `Chunker` trait with three implementations:
   - `NoChunker` (whole file)
   - `FixedChunker` (current `FileUploadIngestor` logic)
   - `CdcChunker` (buzhash-based, ~150 lines)
3. Modify `FileUploadIngestor` to accept a `Chunker` at construction
4. Add config support in `ServerConfig` and `shardline.toml`
5. No changes to backend, frontends, or storage layer

### Backward Compatibility

- Existing uploads stored with fixed-size chunks still reconstruct fine
- New CDC uploads produce different chunk boundaries → don't cross-dedup with old files
- Strategy is recorded per upload, reconstruction reads whatever was stored
- No migration needed

### Risks

1. **Cross-version dedup:** Files uploaded before migration won't dedup against CDC-chunked versions
2. **CPU overhead:** CDC hashes every byte in a sliding window (~1GB upload = ~1GB of windowed hashing)
3. **Chunk count variability:** CDC produces variable-sized chunks, existing `FileRecord.chunks` already handles this

### Related

- Issue: Xet client (xet-data 1.5.2) fails due to BG4 compression mismatch (see `fix/xet-core-bg4-compression` branch)
- Issue: HFT parquet dedup requires CDC for meaningful savings
- See: `docs/PERFORMANCE.md` for dedup benchmarks
