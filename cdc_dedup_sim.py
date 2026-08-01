"""Simulate content-defined chunking (CDC) on parquet files to measure dedup potential.

Uses a Rabin-like rolling hash to find chunk boundaries based on content,
then compares chunk sets between original and modified files.
This demonstrates what Xet-style CDC dedup would achieve for parquet data.
"""
import hashlib
import sys
import os
import time

# CDC parameters (matching shardline-xet-core defaults)
TARGET_CHUNK_SIZE = 128 * 1024   # 128 KiB target
MAX_CHUNK_SIZE = 16 * 1024 * 1024  # 16 MiB max
WINDOW_SIZE = 48  # rolling hash window

MASK = TARGET_CHUNK_SIZE - 1  # mask for boundary detection (power of 2)

def rabin_fingerprint(data: bytes, start: int, length: int) -> int:
    """Simple Rabin fingerprint over a window of bytes."""
    h = 0
    for i in range(start, start + length):
        h = ((h << 1) + data[i]) & 0xFFFFFFFFFFFFFFFF
    return h

def cdc_chunk_file(filepath: str) -> list[tuple[bytes, int, int]]:
    """Split a file into CDC chunks using content-defined boundaries.
    
    Returns list of (chunk_hash, chunk_start, chunk_size).
    """
    with open(filepath, 'rb') as f:
        data = f.read()
    
    chunks = []
    start = 0
    n = len(data)
    
    while start < n:
        # Find next chunk boundary using rolling hash
        end = min(start + TARGET_CHUNK_SIZE, n)
        
        # If we haven't hit max size, look for CDC boundary
        if end < n and (n - start) > TARGET_CHUNK_SIZE:
            # Scan for boundary within a window around target size
            scan_start = max(start + TARGET_CHUNK_SIZE // 2, start)
            scan_end = min(start + MAX_CHUNK_SIZE, n)
            
            found_boundary = False
            for i in range(scan_start, min(scan_end, n - WINDOW_SIZE)):
                # Simple rolling hash check
                h = rabin_fingerprint(data, i, WINDOW_SIZE)
                if (h & MASK) == 0:
                    end = i + WINDOW_SIZE
                    found_boundary = True
                    break
            
            if not found_boundary:
                end = min(start + MAX_CHUNK_SIZE, n)
        
        chunk_bytes = data[start:end]
        chunk_hash = hashlib.blake2b(chunk_bytes, digest_size=32).digest()
        chunks.append((chunk_hash, start, end - start))
        start = end
    
    return chunks

def main():
    if len(sys.argv) < 3:
        print("Usage: cdc_dedup_sim.py <original.parquet> <modified.parquet>")
        sys.exit(1)
    
    orig_path = sys.argv[1]
    mod_path = sys.argv[2]
    
    print(f"CDC chunking: target={TARGET_CHUNK_SIZE//1024}KiB, max={MAX_CHUNK_SIZE//1024//1024}MiB, window={WINDOW_SIZE}")
    print(f"Mask: 0x{MASK:x} (boundary when hash & mask == 0)")
    print()
    
    # Chunk original
    t0 = time.time()
    orig_chunks = cdc_chunk_file(orig_path)
    t1 = time.time()
    orig_size = os.path.getsize(orig_path)
    print(f"Original: {orig_path}")
    print(f"  File size: {orig_size:,} bytes ({orig_size/1e6:.1f} MB)")
    print(f"  Chunks: {len(orig_chunks)}")
    print(f"  Avg chunk size: {sum(c[2] for c in orig_chunks) // len(orig_chunks):,} bytes")
    print(f"  Chunking time: {t1-t0:.2f}s")
    
    # Build chunk hash set
    orig_hash_set = {}
    for h, start, size in orig_chunks:
        orig_hash_set[h] = (start, size)
    
    # Chunk modified
    t0 = time.time()
    mod_chunks = cdc_chunk_file(mod_path)
    t1 = time.time()
    mod_size = os.path.getsize(mod_path)
    print(f"\nModified: {mod_path}")
    print(f"  File size: {mod_size:,} bytes ({mod_size/1e6:.1f} MB)")
    print(f"  Chunks: {len(mod_chunks)}")
    print(f"  Avg chunk size: {sum(c[2] for c in mod_chunks) // len(mod_chunks):,} bytes")
    print(f"  Chunking time: {t1-t0:.2f}s")
    
    # Compare chunk sets
    mod_hash_set = {}
    for h, start, size in mod_chunks:
        mod_hash_set[h] = (start, size)
    
    shared = set(orig_hash_set.keys()) & set(mod_hash_set.keys())
    new_in_mod = set(mod_hash_set.keys()) - set(orig_hash_set.keys())
    only_in_orig = set(orig_hash_set.keys()) - set(mod_hash_set.keys())
    
    shared_bytes = sum(mod_hash_set[h][1] for h in shared)
    new_bytes = sum(mod_hash_set[h][1] for h in new_in_mod)
    
    print(f"\n=== CDC DEDUP RESULTS ===")
    print(f"Shared chunks:   {len(shared)} of {len(mod_chunks)} ({len(shared)*100//len(mod_chunks) if mod_chunks else 0}%)")
    print(f"New chunks:      {len(new_in_mod)}")
    print(f"Lost chunks:     {len(only_in_orig)} (from original, not in modified)")
    print(f"Shared bytes:    {shared_bytes:,} ({shared_bytes/1e6:.1f} MB)")
    print(f"New bytes:       {new_bytes:,} ({new_bytes/1e6:.1f} MB)")
    print(f"Total mod bytes: {mod_size:,}")
    print(f"Bytes saved:     {shared_bytes:,} ({shared_bytes*100//mod_size if mod_size else 0}% of modified file)")
    
    # Compare with fixed-size 64KiB chunking
    print(f"\n=== COMPARISON: Fixed 64KiB vs CDC ===")
    
    # Fixed-size chunking simulation
    CHUNK = 64 * 1024
    orig_fixed = {}
    with open(orig_path, 'rb') as f:
        data = f.read()
    for i in range(0, len(data), CHUNK):
        h = hashlib.blake2b(data[i:i+CHUNK], digest_size=32).digest()
        orig_fixed[h] = True
    
    mod_fixed = {}
    with open(mod_path, 'rb') as f:
        data = f.read()
    for i in range(0, len(data), CHUNK):
        h = hashlib.blake2b(data[i:i+CHUNK], digest_size=32).digest()
        mod_fixed[h] = True
    
    fixed_shared = len(set(orig_fixed.keys()) & set(mod_fixed.keys()))
    fixed_total = len(mod_fixed)
    
    print(f"Fixed-size (64KiB): {fixed_shared}/{fixed_total} chunks shared ({fixed_shared*100//fixed_total if fixed_total else 0}%)")
    print(f"CDC (128KiB target): {len(shared)}/{len(mod_chunks)} chunks shared ({len(shared)*100//len(mod_chunks) if mod_chunks else 0}%)")
    print(f"CDC improvement:     {len(shared)*100//max(fixed_shared,1)}x more chunks reused" if fixed_shared > 0 else "CDC: infinite improvement (fixed shared 0)")

if __name__ == "__main__":
    main()
