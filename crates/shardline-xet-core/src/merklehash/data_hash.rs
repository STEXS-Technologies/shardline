use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::num::ParseIntError;
use std::ops::{Deref, DerefMut};
use std::{fmt, str};

use serde::{Deserialize, Serialize};

/// The DataHash is a 256-bit value stored as `[u64; 4]`.
#[derive(Clone, Copy, Default, Serialize, Deserialize)]
pub struct DataHash([u64; 4]);

impl Deref for DataHash {
    type Target = [u64; 4];
    #[inline(always)]
    fn deref(&self) -> &[u64; 4] {
        &self.0
    }
}

impl DerefMut for DataHash {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u64; 4] {
        &mut (self.0)
    }
}

impl From<[u64; 4]> for DataHash {
    fn from(value: [u64; 4]) -> Self {
        DataHash(value)
    }
}

impl From<[u8; 32]> for DataHash {
    fn from(value: [u8; 32]) -> Self {
        let mut inner = [0u64; 4];
        for (i, chunk) in value.chunks_exact(8).enumerate() {
            inner[i] = u64::from_le_bytes(chunk.try_into().expect("slice len is 8"));
        }
        DataHash(inner)
    }
}

impl From<&[u8; 32]> for DataHash {
    fn from(value: &[u8; 32]) -> Self {
        Self::from(*value)
    }
}

impl From<DataHash> for [u8; 32] {
    fn from(val: DataHash) -> Self {
        let mut out = [0u8; 32];
        for i in 0..4 {
            let bytes = val.0[i].to_le_bytes();
            out[i * 8..(i + 1) * 8].copy_from_slice(&bytes);
        }
        out
    }
}

impl AsRef<DataHash> for DataHash {
    fn as_ref(&self) -> &DataHash {
        self
    }
}

impl PartialEq for DataHash {
    fn eq(&self, other: &Self) -> bool {
        self.0[0] == other.0[0]
            && self.0[1] == other.0[1]
            && self.0[2] == other.0[2]
            && self.0[3] == other.0[3]
    }
}

impl Eq for DataHash {}

impl Ord for DataHash {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for DataHash {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl core::ops::Rem<u64> for DataHash {
    type Output = u64;

    fn rem(self, rhs: u64) -> Self::Output {
        self[3].to_le() % rhs
    }
}

#[derive(Debug, Clone)]
pub struct DataHashHexParseError;

impl Error for DataHashHexParseError {}

impl fmt::Display for DataHashHexParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid hex input for DataHash")
    }
}

impl From<ParseIntError> for DataHashHexParseError {
    fn from(_err: ParseIntError) -> Self {
        DataHashHexParseError {}
    }
}

impl DataHash {
    pub fn hex(&self) -> String {
        format!(
            "{:016x}{:016x}{:016x}{:016x}",
            self.0[0].to_le(),
            self.0[1].to_le(),
            self.0[2].to_le(),
            self.0[3].to_le()
        )
    }

    pub fn from_hex(h: &str) -> Result<DataHash, DataHashHexParseError> {
        if h.len() != 64 {
            return Err(DataHashHexParseError {});
        }
        let good = h.as_bytes().iter().all(|c| c.is_ascii_hexdigit());
        if !good {
            return Err(DataHashHexParseError {});
        }
        let mut ret: DataHash = Default::default();
        ret.0[0] = u64::from_str_radix(&h[..16], 16)?.to_le();
        ret.0[1] = u64::from_str_radix(&h[16..32], 16)?.to_le();
        ret.0[2] = u64::from_str_radix(&h[32..48], 16)?.to_le();
        ret.0[3] = u64::from_str_radix(&h[48..64], 16)?.to_le();
        Ok(ret)
    }

    /// Converts to a stack-allocated byte array. Prefer the infallible
    /// `From<DataHash> for [u8; 32]` conversion when a fixed-size buffer is
    /// sufficient.
    pub fn to_bytes(&self) -> [u8; 32] {
        (*self).into()
    }

    pub fn from_slice(value: &[u8]) -> Result<Self, DataHashBytesParseError> {
        if value.len() != 32 {
            return Err(DataHashBytesParseError);
        }
        let mut hash: DataHash = DataHash::default();
        for i in 0..4 {
            let start = i * 8;
            hash.0[i] =
                u64::from_le_bytes(value[start..start + 8].try_into().expect("slice len is 8"));
        }
        Ok(hash)
    }
}

impl fmt::LowerHex for DataHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}

impl fmt::Display for DataHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}

impl fmt::Debug for DataHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}

#[derive(Debug, Clone)]
pub struct DataHashBytesParseError;

impl Error for DataHashBytesParseError {}

impl fmt::Display for DataHashBytesParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid bytes input for DataHash")
    }
}

impl TryFrom<&[u8]> for DataHash {
    type Error = DataHashBytesParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::from_slice(value)
    }
}

impl From<DataHash> for Vec<u8> {
    fn from(val: DataHash) -> Self {
        let bytes: [u8; 32] = val.into();
        bytes.to_vec()
    }
}

impl Hash for DataHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0[0]);
    }
}

const DATA_KEY: [u8; 32] = [
    102, 151, 245, 119, 91, 149, 80, 222, 49, 53, 203, 172, 165, 151, 24, 28, 157, 228, 33, 16,
    155, 235, 43, 88, 180, 208, 176, 75, 147, 173, 242, 41,
];

const INTERNAL_NODE_HASH: [u8; 32] = [
    1, 126, 197, 199, 165, 71, 41, 150, 253, 148, 102, 102, 180, 138, 2, 230, 93, 221, 83, 111, 55,
    199, 109, 210, 248, 99, 82, 230, 74, 83, 113, 63,
];

pub fn compute_data_hash(slice: &[u8]) -> DataHash {
    let digest = blake3::keyed_hash(&DATA_KEY, slice);
    DataHash::from(*digest.as_bytes())
}

pub fn compute_internal_node_hash(slice: &[u8]) -> DataHash {
    let digest = blake3::keyed_hash(&INTERNAL_NODE_HASH, slice);
    DataHash::from(*digest.as_bytes())
}

pub struct HashedWrite<W: std::io::Write> {
    hasher: blake3::Hasher,
    writer: W,
}

impl<W: std::io::Write> HashedWrite<W> {
    pub fn new(writer: W) -> Self {
        Self {
            hasher: blake3::Hasher::new_keyed(&DATA_KEY),
            writer,
        }
    }

    pub fn hash(&self) -> DataHash {
        let digest = self.hasher.finalize();
        DataHash::from(*digest.as_bytes())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: std::io::Write> std::io::Write for HashedWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hash, Hasher};
    use std::io::Write;

    use super::*;

    #[test]
    fn default_is_all_zeros() {
        let h = DataHash::default();
        assert_eq!(*h, [0u64; 4]);
    }

    #[test]
    fn from_u64_array() {
        let arr = [1u64, 2, 3, 4];
        let h = DataHash::from(arr);
        assert_eq!(*h, arr);
    }

    #[test]
    fn from_u8_32_and_back() {
        let arr = [0xABu8; 32];
        let h = DataHash::from(arr);
        let back: [u8; 32] = h.into();
        assert_eq!(arr, back);
    }

    #[test]
    fn from_ref_u8_32() {
        let arr = [0xCDu8; 32];
        let h = DataHash::from(&arr);
        let back: [u8; 32] = h.into();
        assert_eq!(arr, back);
    }

    #[test]
    fn from_slice_valid() {
        let bytes = vec![42u8; 32];
        let h = DataHash::from_slice(&bytes).unwrap();
        let back: [u8; 32] = h.into();
        assert_eq!(&bytes, &back[..]);
    }

    #[test]
    fn from_slice_wrong_length() {
        assert!(DataHash::from_slice(&[0u8; 31]).is_err());
        assert!(DataHash::from_slice(&[0u8; 33]).is_err());
    }

    #[test]
    fn try_from_slice() {
        let bytes = [7u8; 32];
        let h = DataHash::try_from(bytes.as_slice()).unwrap();
        let back: [u8; 32] = h.into();
        assert_eq!(bytes, back);
        assert!(DataHash::try_from(&[0u8; 16][..]).is_err());
    }

    #[test]
    fn hex_roundtrip() {
        let arr = [0xDEADBEEFu64, 0xCAFEBABE, 0x12345678, 0x9ABCDEF0];
        let h = DataHash::from(arr);
        let hex = h.hex();
        assert_eq!(hex.len(), 64);
        let h2 = DataHash::from_hex(&hex).unwrap();
        assert_eq!(h, h2);
    }

    #[test]
    fn from_hex_invalid_length() {
        assert!(DataHash::from_hex("abc").is_err());
        assert!(DataHash::from_hex(&"a".repeat(63)).is_err());
        assert!(DataHash::from_hex(&"a".repeat(65)).is_err());
    }

    #[test]
    fn from_hex_invalid_chars() {
        assert!(DataHash::from_hex(&"z".repeat(64)).is_err());
        assert!(DataHash::from_hex(&"xyz".repeat(22)[..64]).is_err());
    }

    #[test]
    fn from_hex_error_has_proper_display_and_source() {
        use std::error::Error;
        let err = DataHashHexParseError;
        assert_eq!(err.to_string(), "Invalid hex input for DataHash");
        assert!(err.source().is_none());

        let from_parse: DataHashHexParseError = "garbage".parse::<u64>().unwrap_err().into();
        assert_eq!(from_parse.to_string(), "Invalid hex input for DataHash");
    }

    #[test]
    fn bytes_parse_error_display() {
        let err = DataHashBytesParseError;
        assert_eq!(err.to_string(), "Invalid bytes input for DataHash");
        assert!(std::error::Error::source(&err).is_none());
    }

    #[test]
    fn display_and_lower_hex() {
        let h = DataHash::from([1u64, 2, 3, 4]);
        assert_eq!(format!("{h}"), format!("{h:x}"));
        assert_eq!(format!("{h}").len(), 64);
    }

    #[test]
    fn debug_output_is_hex() {
        let h = DataHash::from([0u64; 4]);
        assert_eq!(
            format!("{h:?}"),
            "0000000000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn ord_and_eq() {
        let a = DataHash::from([1u64, 0, 0, 0]);
        let b = DataHash::from([2u64, 0, 0, 0]);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, a);
        assert_ne!(a, b);
    }

    #[test]
    fn rem_operator() {
        let h = DataHash::from([0, 0, 0, 10]);
        assert_eq!(h % 3, 1);
        assert_eq!(h % 5, 0);
        assert_eq!(h % 10, 0);
    }

    #[test]
    fn into_bytes() {
        let h = DataHash::from([1u64, 2, 3, 4]);
        let bytes: [u8; 32] = h.into();
        assert_eq!(bytes.len(), 32);
    }

    #[test]
    fn as_ref_data_hash() {
        let h = DataHash::from([1u64, 2, 3, 4]);
        let r: &DataHash = h.as_ref();
        assert_eq!(*r, h);
    }

    #[test]
    fn into_vec_u8() {
        let h = DataHash::from([0xDEADBEEFu64, 0xCAFEBABE, 0x12345678, 0x9ABCDEF0]);
        let vec: Vec<u8> = h.into();
        assert_eq!(vec.len(), 32);
        let back = DataHash::from_slice(&vec).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn hash_impl_consistent() {
        let a = DataHash::from([1u64, 2, 3, 4]);
        let b = DataHash::from([1u64, 2, 3, 4]);
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    #[test]
    fn deref_mut() {
        let mut h = DataHash::from([0u64; 4]);
        h[0] = 42;
        assert_eq!(h[0], 42);
    }

    #[test]
    fn as_bytes() {
        let h = DataHash::from([0x0102030405060708u64, 0, 0, 0]);
        let bytes: [u8; 32] = h.into();
        assert_eq!(bytes.len(), 32);
        let first = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        assert_eq!(first, 0x0102030405060708);
    }

    #[test]
    fn compute_data_hash_consistent() {
        assert_eq!(compute_data_hash(b"hello"), compute_data_hash(b"hello"));
        assert_ne!(compute_data_hash(b"hello"), compute_data_hash(b"world"));
        assert_ne!(compute_data_hash(b""), DataHash::default());
    }

    #[test]
    fn test_compute_internal_node_hash() {
        assert_eq!(
            compute_internal_node_hash(b"test"),
            compute_internal_node_hash(b"test")
        );
        assert_ne!(
            compute_internal_node_hash(b"test"),
            compute_data_hash(b"test")
        );
    }

    #[test]
    fn hashed_write_basic() {
        let data = b"hashed write works";
        let mut w = HashedWrite::new(Vec::new());
        w.write_all(data).unwrap();
        assert_eq!(w.hash(), compute_data_hash(data));
        assert_eq!(w.into_inner(), data);
    }

    #[test]
    fn hashed_write_empty() {
        let w = HashedWrite::new(Vec::new());
        assert_eq!(w.hash(), compute_data_hash(b""));
    }

    #[test]
    fn hashed_write_multiple_writes() {
        let mut w = HashedWrite::new(Vec::new());
        w.write_all(b"part1").unwrap();
        w.write_all(b"part2").unwrap();
        assert_eq!(w.hash(), compute_data_hash(b"part1part2"));
        w.flush().unwrap();
    }

    #[test]
    fn hashed_write_flush() {
        let mut w = HashedWrite::new(Vec::new());
        w.write_all(b"flush").unwrap();
        w.flush().unwrap();
    }
}
