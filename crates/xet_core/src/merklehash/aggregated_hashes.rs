use super::{MerkleHash, compute_internal_node_hash};

const AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR: u64 = 4;
const MAX_GROUP_SIZE: usize = 2 * AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR as usize + 1;

#[inline]
fn next_merge_cut(hashes: &[(MerkleHash, u64)]) -> usize {
    if hashes.len() <= 2 {
        return hashes.len();
    }

    let end = MAX_GROUP_SIZE.min(hashes.len());

    for (i, (h, _)) in hashes.iter().enumerate().take(end).skip(2) {
        if *h % AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR == 0 {
            return i + 1;
        }
    }

    end
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

#[inline]
fn write_hex_u64(buf: &mut [u8], pos: &mut usize, val: u64) {
    let p = *pos;
    buf[p] = HEX_DIGITS[((val >> 60) & 0xF) as usize];
    buf[p + 1] = HEX_DIGITS[((val >> 56) & 0xF) as usize];
    buf[p + 2] = HEX_DIGITS[((val >> 52) & 0xF) as usize];
    buf[p + 3] = HEX_DIGITS[((val >> 48) & 0xF) as usize];
    buf[p + 4] = HEX_DIGITS[((val >> 44) & 0xF) as usize];
    buf[p + 5] = HEX_DIGITS[((val >> 40) & 0xF) as usize];
    buf[p + 6] = HEX_DIGITS[((val >> 36) & 0xF) as usize];
    buf[p + 7] = HEX_DIGITS[((val >> 32) & 0xF) as usize];
    buf[p + 8] = HEX_DIGITS[((val >> 28) & 0xF) as usize];
    buf[p + 9] = HEX_DIGITS[((val >> 24) & 0xF) as usize];
    buf[p + 10] = HEX_DIGITS[((val >> 20) & 0xF) as usize];
    buf[p + 11] = HEX_DIGITS[((val >> 16) & 0xF) as usize];
    buf[p + 12] = HEX_DIGITS[((val >> 12) & 0xF) as usize];
    buf[p + 13] = HEX_DIGITS[((val >> 8) & 0xF) as usize];
    buf[p + 14] = HEX_DIGITS[((val >> 4) & 0xF) as usize];
    buf[p + 15] = HEX_DIGITS[(val & 0xF) as usize];
    *pos = p + 16;
}

#[inline]
fn write_decimal_u64(buf: &mut [u8], pos: &mut usize, val: u64) {
    if val == 0 {
        buf[*pos] = b'0';
        *pos += 1;
        return;
    }
    let mut digits = [0u8; 20];
    let mut dpos = 20;
    let mut v = val;
    while v > 0 {
        dpos -= 1;
        digits[dpos] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    let len = 20 - dpos;
    buf[*pos..*pos + len].copy_from_slice(&digits[dpos..]);
    *pos += len;
}

const MAX_MERGE_BUF_SIZE: usize =
    (2 * AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR as usize + 1) * 88;

#[inline]
fn write_hash_entry(buf: &mut [u8], pos: &mut usize, total_len: &mut u64, h: &MerkleHash, s: u64) {
    write_hex_u64(buf, pos, h[0].to_le());
    write_hex_u64(buf, pos, h[1].to_le());
    write_hex_u64(buf, pos, h[2].to_le());
    write_hex_u64(buf, pos, h[3].to_le());
    buf[*pos] = b' ';
    buf[*pos + 1] = b':';
    buf[*pos + 2] = b' ';
    *pos += 3;
    write_decimal_u64(buf, pos, s);
    buf[*pos] = b'\n';
    *pos += 1;
    *total_len += s;
}

#[inline]
fn merged_hash_of_sequence(hash: &[(MerkleHash, u64)]) -> (MerkleHash, u64) {
    let mut buf = [0u8; MAX_MERGE_BUF_SIZE];
    let mut pos = 0usize;
    let mut total_len = 0u64;
    for &(ref h, s) in hash.iter() {
        write_hash_entry(&mut buf, &mut pos, &mut total_len, h, s);
    }
    (compute_internal_node_hash(&buf[..pos]), total_len)
}

#[inline]
fn aggregated_node_hash(chunks: &[(MerkleHash, u64)]) -> MerkleHash {
    if chunks.is_empty() {
        return MerkleHash::default();
    }

    let mut hv = chunks.to_vec();

    while hv.len() > 1 {
        let mut write_idx = 0;
        let mut read_idx = 0;

        while read_idx != hv.len() {
            let next_cut = read_idx + next_merge_cut(&hv[read_idx..]);
            hv[write_idx] = merged_hash_of_sequence(&hv[read_idx..next_cut]);
            write_idx += 1;
            read_idx = next_cut;
        }

        hv.resize(write_idx, Default::default());
    }

    hv[0].0
}

#[inline]
pub fn xorb_hash(chunks: &[(MerkleHash, u64)]) -> MerkleHash {
    if chunks.is_empty() {
        return MerkleHash::default();
    }
    aggregated_node_hash(chunks)
}

#[inline]
pub fn file_hash_with_salt(chunks: &[(MerkleHash, u64)], salt: &[u8; 32]) -> MerkleHash {
    if chunks.is_empty() {
        return MerkleHash::default();
    }
    let key_bytes: [u8; 32] = *salt;
    let aggregated = aggregated_node_hash(chunks);
    let agg_bytes: [u8; 32] = aggregated.into();
    let digest = blake3::keyed_hash(&key_bytes, &agg_bytes);
    MerkleHash::from(*digest.as_bytes())
}

#[inline]
pub fn file_hash(chunks: &[(MerkleHash, u64)]) -> MerkleHash {
    file_hash_with_salt(chunks, &[0; 32])
}
