//! Canonical object-key path prefixes for CAS content-addressed storage.
//!
//! All protocol frontends and operator tools must construct object keys using
//! these constants and helpers. Inline path strings are forbidden.

use shardline_storage::ObjectKey;

/// Prefix for Xet xorb objects: `xorbs/default/`.
pub const XORB_PREFIX: &str = "xorbs/default/";

/// Prefix for Xet shard objects: `shards/`.
pub const SHARD_PREFIX: &str = "shards/";

/// Prefix for chunk objects: `chunks/`.
pub const CHUNK_PREFIX: &str = "chunks/";

/// Prefix for SHA-256 content-addressed objects: `sha256/`.
pub const SHA256_PREFIX: &str = "sha256/";

/// Prefix for protocol-shared objects: `protocols/shared/sha256/`.
pub const PROTOCOL_SHARED_SHA256_PREFIX: &str = "protocols/shared/sha256/";

/// Fuzz test namespace for object keys: `fuzz/`.
pub const FUZZ_NAMESPACE_PREFIX: &str = "fuzz/";

/// Builds an object key under the xorb namespace: `xorbs/default/{prefix}/{name}.xorb`.
///
/// `prefix` is typically the first two hex characters of the object hash.
/// `name` is the full hex hash identifying the xorb.
///
/// # Panics
///
/// Panics if the constructed key contains invalid characters or is too long
/// for an object key. In practice this never happens because the format
    #[allow(clippy::expect_used)]
/// `{prefix}/{name}` with hex characters always produces a valid object key.
#[must_use]
pub fn xorb_key(prefix: &str, name: &str) -> ObjectKey {
    let key_str = format!("{XORB_PREFIX}{prefix}/{name}.xorb");
    ObjectKey::parse(&key_str).expect("xorb key format is valid")
}

/// Builds an object key under the shard namespace: `shards/{prefix}/{name}.shard`.
///
/// `prefix` is typically the first two hex characters of the shard hash.
/// `name` is the full hex hash or logical shard identifier.
///
/// # Panics
///
    #[allow(clippy::expect_used)]
/// Panics if the constructed key is invalid. This never happens in practice
/// since the format produces valid keys from hex strings.
#[must_use]
pub fn shard_key(prefix: &str, name: &str) -> ObjectKey {
    let key_str = format!("{SHARD_PREFIX}{prefix}/{name}.shard");
    ObjectKey::parse(&key_str).expect("shard key format is valid")
}

/// Builds an object key under the chunk namespace: `chunks/{prefix}/{name}`.
///
/// `prefix` is typically the first two hex characters of the chunk hash.
/// `name` is the full hex hash or logical chunk identifier.
///
/// # Panics
    #[allow(clippy::expect_used)]
///
/// Panics if the constructed key is invalid. This never happens in practice
/// since the format produces valid keys from hex strings.
#[must_use]
pub fn chunk_key(prefix: &str, name: &str) -> ObjectKey {
    let key_str = format!("{CHUNK_PREFIX}{prefix}/{name}");
    ObjectKey::parse(&key_str).expect("chunk key format is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xorb_key_format() {
        let key = xorb_key("ab", "abcdef123456");
        assert_eq!(key.as_str(), "xorbs/default/ab/abcdef123456.xorb");
    }

    #[test]
    fn shard_key_format() {
        let key = shard_key("cd", "test-shard");
        assert_eq!(key.as_str(), "shards/cd/test-shard.shard");
    }

    #[test]
    fn chunk_key_format() {
        let key = chunk_key("ef", "chunk-hash");
        assert_eq!(key.as_str(), "chunks/ef/chunk-hash");
    }
}
