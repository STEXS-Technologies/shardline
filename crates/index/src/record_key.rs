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
