use shardline_protocol::{RepositoryProvider, RepositoryScope};

use crate::RepositoryRecordScope;

pub(crate) fn record_key(
    kind: &str,
    scope_key: &str,
    file_id: &str,
    content_hash: Option<&str>,
) -> String {
    let mut key = String::new();
    push_length_prefixed(&mut key, kind);
    push_length_prefixed(&mut key, scope_key);
    push_length_prefixed(&mut key, file_id);
    if let Some(value) = content_hash {
        push_length_prefixed(&mut key, value);
    }
    key
}

pub(crate) fn repository_scope_key(repository_scope: Option<&RepositoryScope>) -> String {
    let mut key = String::new();
    match repository_scope {
        Some(scope) => append_repository_scope_key(
            &mut key,
            scope.provider(),
            scope.owner(),
            scope.name(),
            scope.revision(),
        ),
        None => push_length_prefixed(&mut key, "global"),
    }
    key
}

pub(crate) fn repository_record_scope_key(repository_scope: &RepositoryRecordScope) -> String {
    let mut key = String::new();
    append_repository_scope_key(
        &mut key,
        repository_scope.provider(),
        repository_scope.owner(),
        repository_scope.name(),
        None,
    );
    key
}

fn append_repository_scope_key(
    key: &mut String,
    provider: RepositoryProvider,
    owner: &str,
    name: &str,
    revision: Option<&str>,
) {
    push_length_prefixed(key, provider.as_str());
    push_length_prefixed(key, owner);
    push_length_prefixed(key, name);
    if let Some(revision) = revision {
        push_length_prefixed(key, revision);
    }
}

fn push_length_prefixed(target: &mut String, value: &str) {
    target.push_str(&value.len().to_string());
    target.push(':');
    target.push_str(value);
}

#[cfg(test)]
mod tests {
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic, clippy::unwrap_in_result, clippy::arithmetic_side_effects, clippy::option_if_let_else, clippy::unreachable, clippy::shadow_unrelated, clippy::let_underscore_must_use, clippy::unwrap_err_used)]
    use super::*;

    // -----------------------------------------------------------------------
    // push_length_prefixed (private – exercised through public helpers)
    // -----------------------------------------------------------------------

    #[test]
    fn push_length_prefixed_basic() {
        let mut buf = String::new();
        push_length_prefixed(&mut buf, "hello");
        assert_eq!(buf, "5:hello");
    }

    #[test]
    fn push_length_prefixed_empty() {
        let mut buf = String::new();
        push_length_prefixed(&mut buf, "");
        assert_eq!(buf, "0:");
    }

    // -----------------------------------------------------------------------
    // record_key
    // -----------------------------------------------------------------------

    #[test]
    fn record_key_without_hash() {
        let key = record_key("latest", "global", "file.bin", None);
        assert_eq!(key, "6:latest6:global8:file.bin");
    }

    #[test]
    fn record_key_with_hash() {
        let key = record_key("version", "scope", "file.bin", Some("hash"));
        assert_eq!(key, "7:version5:scope8:file.bin4:hash");
    }

    #[test]
    fn record_key_without_hash_omits_hash_part() {
        let key_with = record_key("latest", "global", "file.bin", Some("abc"));
        let key_without = record_key("latest", "global", "file.bin", None);
        assert!(key_with.len() > key_without.len());
        assert!(key_with.starts_with(&key_without));
    }

    // -----------------------------------------------------------------------
    // repository_scope_key
    // -----------------------------------------------------------------------

    #[test]
    fn repository_scope_key_none_is_global() {
        let key = repository_scope_key(None);
        assert_eq!(key, "6:global");
    }

    #[test]
    fn repository_scope_key_with_scope() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "owner", "name", None).unwrap();
        let key = repository_scope_key(Some(&scope));
        // "github" = 6 chars, "owner" = 5 chars, "name" = 4 chars
        assert_eq!(key, "6:github5:owner4:name");
    }

    #[test]
    fn repository_scope_key_with_revision() {
        let scope = RepositoryScope::new(
            RepositoryProvider::GitLab,
            "alice",
            "myrepo",
            Some("abc123"),
        )
        .unwrap();
        let key = repository_scope_key(Some(&scope));
        assert_eq!(key, "6:gitlab5:alice6:myrepo6:abc123");
    }

    // -----------------------------------------------------------------------
    // repository_record_scope_key
    // -----------------------------------------------------------------------

    #[test]
    fn repository_record_scope_key_omits_revision() {
        let scope = RepositoryRecordScope::new(RepositoryProvider::GitHub, "owner", "name");
        let key = repository_record_scope_key(&scope);
        // Should contain provider, owner, name but NOT the revision
        assert_eq!(key, "6:github5:owner4:name");
    }
}
