//! Compile-time choice of staging evidence, independent of content validation.

use shardline_protocol::ShardlineHash;

pub(super) trait StagingDigest: Default {
    type Hash;

    fn update(&mut self, bytes: &[u8]);
    fn finish(self) -> Self::Hash;
}

impl StagingDigest for blake3::Hasher {
    type Hash = ShardlineHash;

    fn update(&mut self, bytes: &[u8]) {
        blake3::Hasher::update(self, bytes);
    }

    fn finish(self) -> Self::Hash {
        ShardlineHash::from_bytes(*self.finalize().as_bytes())
    }
}

// Native Xorb identity is established by subsequent member/root validation,
// not a hash of the unnormalized wire representation. Do not fabricate a hash
// for that path: the staged type deliberately carries no digest.
impl StagingDigest for () {
    type Hash = ();

    fn update(&mut self, _bytes: &[u8]) {}

    fn finish(self) -> Self::Hash {}
}
