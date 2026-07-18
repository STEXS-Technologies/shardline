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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merklehash::compute_data_hash;

    #[test]
    fn xorb_hash_empty() {
        assert_eq!(xorb_hash(&[]), MerkleHash::default());
    }

    #[test]
    fn xorb_hash_single() {
        let h = xorb_hash(&[(compute_data_hash(b"a"), 1)]);
        assert_ne!(h, MerkleHash::default());
    }

    #[test]
    fn xorb_hash_deterministic() {
        let chunks = vec![(compute_data_hash(b"a"), 1), (compute_data_hash(b"b"), 2)];
        assert_eq!(xorb_hash(&chunks), xorb_hash(&chunks));
    }

    #[test]
    fn xorb_hash_order_matters() {
        let a = (compute_data_hash(b"a"), 1);
        let b = (compute_data_hash(b"b"), 2);
        assert_ne!(xorb_hash(&[a, b]), xorb_hash(&[b, a]));
    }

    #[test]
    fn xorb_hash_two_chunks_triggers_next_merge_cut_len_eq_2() {
        let h1 = MerkleHash::from([0u64, 0, 0, 1]);
        let h2 = MerkleHash::from([0u64, 0, 0, 2]);
        assert_ne!(xorb_hash(&[(h1, 10), (h2, 20)]), MerkleHash::default());
    }

    #[test]
    fn xorb_hash_three_chunks_no_mod_fallback() {
        let h1 = MerkleHash::from([0u64, 0, 0, 1]);
        let h2 = MerkleHash::from([0u64, 0, 0, 2]);
        let h3 = MerkleHash::from([0u64, 0, 0, 3]);
        assert_ne!(
            xorb_hash(&[(h1, 1), (h2, 2), (h3, 3)]),
            MerkleHash::default()
        );
    }

    #[test]
    fn xorb_hash_three_chunks_with_mod_triggers_early_return() {
        let h1 = MerkleHash::from([0u64, 0, 0, 1]);
        let h2 = MerkleHash::from([0u64, 0, 0, 2]);
        let h3 = MerkleHash::from([0u64, 0, 0, 4]); // 4 % 4 == 0
        assert_ne!(
            xorb_hash(&[(h1, 1), (h2, 2), (h3, 3)]),
            MerkleHash::default()
        );
    }

    #[test]
    fn xorb_hash_many_chunks_exercises_multiple_merge_rounds() {
        let mut chunks = Vec::new();
        for i in 0u64..10 {
            chunks.push((MerkleHash::from([i, i * 2, i * 3, i * 4]), i + 1));
        }
        assert_ne!(xorb_hash(&chunks), MerkleHash::default());
    }

    #[test]
    fn xorb_hash_with_zero_size_triggers_write_decimal_zero_branch() {
        let chunks = vec![(MerkleHash::from([1u64, 0, 0, 0]), 0u64)];
        assert_ne!(xorb_hash(&chunks), MerkleHash::default());
    }

    #[test]
    fn xorb_hash_mod_at_various_positions() {
        for mod_pos in 2..=5 {
            let mut chunks: Vec<(MerkleHash, u64)> = (0..6)
                .map(|i| {
                    let h = if i == mod_pos {
                        MerkleHash::from([0u64, 0, 0, 4]) // 4 % 4 == 0
                    } else {
                        MerkleHash::from([0u64, 0, 0, (i + 1) as u64])
                    };
                    (h, (i + 1) as u64)
                })
                .collect();
            assert_ne!(
                xorb_hash(&chunks),
                MerkleHash::default(),
                "failed at mod_pos={mod_pos}"
            );
        }
    }

    #[test]
    fn file_hash_empty() {
        assert_eq!(file_hash(&[]), MerkleHash::default());
    }

    #[test]
    fn file_hash_deterministic() {
        let chunks = vec![(compute_data_hash(b"a"), 1)];
        assert_eq!(file_hash(&chunks), file_hash(&chunks));
    }

    #[test]
    fn file_hash_with_salt_deterministic() {
        let salt = [1u8; 32];
        let chunks = vec![(compute_data_hash(b"a"), 1)];
        assert_eq!(
            file_hash_with_salt(&chunks, &salt),
            file_hash_with_salt(&chunks, &salt)
        );
    }

    #[test]
    fn file_hash_vs_xorb_hash() {
        let chunks = vec![
            (compute_data_hash(b"a"), 100),
            (compute_data_hash(b"b"), 200),
        ];
        assert_ne!(file_hash(&chunks), xorb_hash(&chunks));
    }

    #[test]
    fn file_hash_with_salt_empty() {
        assert_eq!(
            file_hash_with_salt(&[], &[0xABu8; 32]),
            MerkleHash::default()
        );
    }

    #[test]
    fn file_hash_with_salt_different_salts_produce_different_hashes() {
        let chunks = vec![(MerkleHash::from([1u64, 2, 3, 4]), 100)];
        let h1 = file_hash_with_salt(&chunks, &[1u8; 32]);
        let h2 = file_hash_with_salt(&chunks, &[2u8; 32]);
        assert_ne!(h1, h2);
        assert_ne!(h1, MerkleHash::default());
    }

    #[test]
    fn next_merge_cut_len_1_or_2() {
        let h = MerkleHash::default();
        assert_eq!(next_merge_cut(&[(h, 1)]), 1);
        assert_eq!(next_merge_cut(&[(h, 1), (h, 2)]), 2);
    }

    #[test]
    fn write_hex_u64_all_zero() {
        let mut buf = [0u8; 20];
        let mut pos = 0;
        write_hex_u64(&mut buf, &mut pos, 0);
        assert_eq!(&buf[..16], b"0000000000000000");
        assert_eq!(pos, 16);
    }

    #[test]
    fn write_hex_u64_all_ones() {
        let mut buf = [0u8; 20];
        let mut pos = 0;
        write_hex_u64(&mut buf, &mut pos, u64::MAX);
        assert_eq!(&buf[..16], b"ffffffffffffffff");
        assert_eq!(pos, 16);
    }

    #[test]
    fn write_hex_u64_pattern() {
        let mut buf = [0u8; 20];
        let mut pos = 0;
        write_hex_u64(&mut buf, &mut pos, 0xDEADBEEFCAFEBABE);
        assert_eq!(pos, 16);
    }

    #[test]
    fn write_decimal_u64_zero() {
        let mut buf = [0u8; 20];
        let mut pos = 0;
        write_decimal_u64(&mut buf, &mut pos, 0);
        assert_eq!(&buf[..1], b"0");
        assert_eq!(pos, 1);
    }

    #[test]
    fn write_decimal_u64_nonzero() {
        let mut buf = [0u8; 20];
        let mut pos = 0;
        write_decimal_u64(&mut buf, &mut pos, 12345);
        assert_eq!(&buf[..5], b"12345");
        assert_eq!(pos, 5);
    }

    #[test]
    fn write_decimal_u64_large() {
        let mut buf = [0u8; 40];
        let mut pos = 0;
        write_decimal_u64(&mut buf, &mut pos, u64::MAX);
        assert_eq!(&buf[..20], b"18446744073709551615");
        assert_eq!(pos, 20);
    }

    #[test]
    fn write_hash_entry_regular() {
        let mut buf = [0u8; 200];
        let mut pos = 0;
        let mut total = 0u64;
        let h = MerkleHash::from([1u64, 2, 3, 4]);
        write_hash_entry(&mut buf, &mut pos, &mut total, &h, 42);
        // 64 hex chars + " : " + decimal + "\n"
        assert!(pos > 64 + 3 + 1);
        assert_eq!(total, 42);
    }

    #[test]
    fn merged_hash_of_sequence_basic() {
        let h1 = (MerkleHash::from([1u64, 0, 0, 0]), 10u64);
        let h2 = (MerkleHash::from([2u64, 0, 0, 0]), 20u64);
        let (result_hash, total_len) = merged_hash_of_sequence(&[h1, h2]);
        assert_ne!(result_hash, MerkleHash::default());
        assert_eq!(total_len, 30);
    }

    #[test]
    fn aggregated_node_hash_empty() {
        assert_eq!(aggregated_node_hash(&[]), MerkleHash::default());
    }
}
