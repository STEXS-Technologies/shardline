# CDC Migration Plan: VRAM-256 + FastCDC Fallback

## Overview

Replace fixed-size chunking in `FileUploadIngestor` with content-defined chunking (CDC) for all non-Xet frontends (LFS, Bazel, OCI, Hub). Uses VRAM-256 (AVX2-accelerated RAM algorithm) as primary, FastCDC as fallback for non-AVX2 platforms.

## Architecture

```
FileUploadIngestor
  ├── DedupStrategy::None      → whole file (current)
  ├── DedupStrategy::Fixed     → 64KB fixed chunks (current)
  └── DedupStrategy::Cdc       → new CDC chunker
        ├── if AVX2 available   → VRAM-256 (RAM + SIMD)
        └── else                → FastCDC (Gear hash + NC)
```

## Algorithm: RAM (Rapid Asymmetric Maximum)

The RAM algorithm is hashless — no rolling hash computation. It finds chunk boundaries by scanning for local maximum bytes:

1. Maintain a sliding window of `window_size` bytes
2. Find the maximum byte value in the window
3. Scan forward from the window start until finding a byte ≥ that maximum
4. That position is the chunk boundary

The AVX2 acceleration uses `_mm256_max_epu8` to find the maximum in 32 bytes simultaneously, achieving ~19.5 GB/s throughput.

### Parameters

| Parameter | Value | Notes |
|---|---|---|
| target_chunk_size | 128 KiB | Power of 2 preferred |
| min_chunk_size | 16 KiB | Prevents tiny chunks |
| max_chunk_size | 1 MiB | Hard upper bound |
| window_size | 64 bytes | Scanning window |

## Implementation Files

### New files

- `crates/shardline-server/src/cdc/mod.rs` — CDC trait + dispatch
- `crates/shardline-server/src/cdc/ram.rs` — RAM algorithm (scalar + AVX2)
- `crates/shardline-server/src/cdc/fastcdc.rs` — FastCDC fallback

### Modified files

- `crates/shardline-server/src/upload_ingest/ingestor.rs` — use CDC chunker
- `crates/shardline-server-core/src/config/types/config.rs` — DedupStrategy enum
- `crates/shardline-server-core/src/config/env.rs` — SHARDLINE_DEDUP_STRATEGY env var

## CDC Trait

```rust
pub trait Chunker {
    /// Find the next chunk boundary in the data buffer.
    /// Returns the offset of the boundary, or None if no boundary found.
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize>;
    
    /// Reset the chunker state for a new file.
    fn reset(&mut self);
}
```

## VRAM-256 Implementation

```rust
#[cfg(target_arch = "x86_64")]
fn find_boundary_vram256(data: &[u8], window: usize) -> Option<usize> {
    use std::arch::x86_64::*;
    
    if data.len() < window { return None; }
    
    // Load window into SIMD register
    let window_vec = unsafe { _mm256_loadu_si256(data[..window].as_ptr() as *const __m256i) };
    
    // Find max byte in window using SIMD
    let mut max_vec = window_vec;
    for i in (32..window).step_by(32) {
        let chunk = unsafe { _mm256_loadu_si256(data[i..].as_ptr() as *const __m256i) };
        max_vec = unsafe { _mm256_max_epu8(max_vec, chunk) };
    }
    
    // Extract max value (broadcast to all lanes)
    // Scan forward for byte >= max
    // ...
}
```

## FastCDC Fallback

Uses the `fastcdc` crate with normalized chunking:

```rust
fn find_boundary_fastcdc(data: &[u8], target: usize) -> Option<usize> {
    let mask = (target - 1) as u32;
    // Gear hash boundary detection
    let mut hash: u64 = 0;
    for (i, &byte) in data.iter().enumerate() {
        hash = (hash << 1).wrapping_add(GEAR_TABLE[byte as usize] as u64);
        if (hash & mask as u64) == 0 && i >= MIN_CHUNK {
            return Some(i + 1);
        }
    }
    None
}
```

## Integration with FileUploadIngestor

The ingestor already accumulates bytes in a buffer and flushes at boundaries. The change is minimal:

```rust
// Before (fixed-size):
if self.pending.len() >= self.chunk_size {
    self.flush_chunk();
}

// After (CDC):
if let Some(boundary) = self.chunker.find_boundary(&self.pending) {
    self.flush_chunk_at(boundary);
}
```

## Configuration

```toml
# Global default
dedup_strategy = "cdc"  # "none" | "fixed" | "cdc"

# Per-frontend override
[frontends.bazel-http]
dedup_strategy = "cdc"
cdc_target_chunk_size = 131072  # 128 KiB
```

Env vars:
- `SHARDLINE_DEDUP_STRATEGY=cdc`
- `SHARDLINE_CDC_TARGET_CHUNK_SIZE=131072`

## Estimated Effort

| Task | LOC | Time |
|---|---|---|
| CDC trait + dispatch | ~50 | 1 day |
| RAM scalar + AVX2 | ~200 | 2 days |
| FastCDC fallback | ~100 | 1 day |
| Ingestor integration | ~50 | 0.5 day |
| Config support | ~30 | 0.5 day |
| Tests | ~300 | 1.5 days |
| **Total** | **~730** | **~6 days** |
