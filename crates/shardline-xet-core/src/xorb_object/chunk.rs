use std::borrow::Cow;

use serde::Serialize;

use crate::merklehash::MerkleHash;

#[derive(Clone, Debug, Serialize)]
pub struct Chunk {
    pub hash: MerkleHash,
    pub data: Cow<'static, [u8]>,
}
