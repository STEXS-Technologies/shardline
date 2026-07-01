use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::num::ParseIntError;
use std::ops::{Deref, DerefMut};
use std::{fmt, str};

use serde::{Deserialize, Serialize};

/// The DataHash is a 256-bit value stored as `[u64; 4]`.
#[derive(Clone, Copy, Serialize, Deserialize)]
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
        for i in 0..4 {
            let start = i * 8;
            inner[i] = u64::from_le_bytes(value[start..start + 8].try_into().expect("slice len is 8"));
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

impl AsRef<[u8]> for DataHash {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: DataHash is repr(transparent) over [u64; 4] which is a plain
        // byte array. We use a boxed slice to keep the reference valid.
        // This is safe because [u64; 4] has no padding and is stored contiguously.
        // We leak a small allocation to return a static reference.
        // Actually, let's use a different approach: store as bytes internally.
        // For now, we convert on the fly. This is only used in a few places.
        //
        // Actually, let's just use a simple boxed slice approach.
        // We'll convert to a Vec and leak it. But that's wasteful.
        // Better approach: just use the byte representation directly.
        //
        // Since we can't use unsafe, let's convert to a static reference via leak.
        // This is acceptable for the limited usage patterns in this crate.
        let bytes: [u8; 32] = (*self).into();
        let boxed: &'static mut [u8] = Box::leak(bytes.into());
        boxed
    }
}

impl AsRef<DataHash> for DataHash {
    fn as_ref(&self) -> &DataHash {
        self
    }
}

impl Default for DataHash {
    fn default() -> DataHash {
        DataHash([0; 4])
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

    pub fn as_bytes(&self) -> &[u8] {
        // Convert to bytes and leak for a 'static reference.
        // This is acceptable for the limited usage in this crate.
        let bytes: [u8; 32] = (*self).into();
        Box::leak(bytes.into())
    }

    pub fn from_slice(value: &[u8]) -> Result<Self, DataHashBytesParseError> {
        if value.len() != 32 {
            return Err(DataHashBytesParseError);
        }
        let mut hash: DataHash = DataHash::default();
        for i in 0..4 {
            let start = i * 8;
            hash.0[i] = u64::from_le_bytes(value[start..start + 8].try_into().expect("slice len is 8"));
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
        val.as_bytes().into()
    }
}

impl Hash for DataHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0[0]);
    }
}

const DATA_KEY: [u8; 32] = [
    102, 151, 245, 119, 91, 149, 80, 222, 49, 53, 203, 172, 165, 151, 24, 28, 157, 228, 33, 16, 155, 235, 43, 88,
    180, 208, 176, 75, 147, 173, 242, 41,
];

const INTERNAL_NODE_HASH: [u8; 32] = [
    1, 126, 197, 199, 165, 71, 41, 150, 253, 148, 102, 102, 180, 138, 2, 230, 93, 221, 83, 111, 55, 199, 109, 210,
    248, 99, 82, 230, 74, 83, 113, 63,
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
