use shardline_protocol::RepositoryScope;
use shardline_storage::ObjectKey;

use crate::{ProtocolError, object_key, scope_namespace, validate_content_hash};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BazelCacheKind {
    Ac,
    Cas,
}

impl BazelCacheKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ac => "ac",
            Self::Cas => "cas",
        }
    }
}

pub fn bazel_cache_object_key(
    kind: BazelCacheKind,
    hash_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, ProtocolError> {
    validate_content_hash(hash_hex)?;
    object_key(&format!(
        "protocols/bazel/{}/{}/{}",
        scope_namespace(repository_scope),
        kind.as_str(),
        hash_hex
    ))
}
