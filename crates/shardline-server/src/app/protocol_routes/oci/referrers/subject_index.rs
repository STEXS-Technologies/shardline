//! Derived lookup metadata; immutable manifests and visibility remain authoritative.
use std::{collections::BTreeSet, sync::Arc};

use serde_json::Value;
use sha2::{Digest, Sha256};
use shardline_index::OciManifestSubject;
use shardline_server_core::AuthorizedRepository;

use super::super::helpers::{oci_manifest_index_key, oci_manifest_key, oci_manifest_prefix};
use crate::{
    ServerBackend, ServerError,
    app::AppState,
    protocol_support::{parse_sha256_digest, scope_namespace},
};

const PAGE: usize = 1000;
#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
pub(super) enum Lookup {
    Indexed,
    ManifestInventory,
}

pub(super) struct Candidate {
    pub(super) digest_hex: String,
    pub(super) mapping: Option<OciManifestSubject>,
}

pub(super) async fn lookup(state: &Arc<AppState>) -> Result<Lookup, ServerError> {
    match &state.backend {
        ServerBackend::Postgres(backend) => Ok(
            if backend.index_store().oci_referrer_index_writable().await? {
                Lookup::Indexed
            } else {
                Lookup::ManifestInventory
            },
        ),
        ServerBackend::Local(_) => Ok(Lookup::Indexed),
    }
}

pub(super) async fn candidates(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    subject: &str,
    after: Option<&str>,
    lookup: Lookup,
) -> Result<Vec<Candidate>, ServerError> {
    match lookup {
        Lookup::Indexed => Ok(page(state, repository, auth, Some(subject), after)
            .await?
            .into_iter()
            .map(|mapping| Candidate {
                digest_hex: mapping.digest_hex.clone(),
                mapping: Some(mapping),
            })
            .collect()),
        Lookup::ManifestInventory => {
            let prefix = oci_manifest_prefix(repository, auth)?;
            let start = after
                .map(|digest| oci_manifest_key(repository, digest, auth))
                .transpose()?;
            let backend = state.backend.clone();
            crate::admission::blocking_work(&state.pools.blocking_io, move || {
                backend
                    .list_object_flat_namespace_page(&prefix, start.as_ref(), PAGE)?
                    .into_iter()
                    .map(|metadata| {
                        let digest_hex = metadata
                            .key()
                            .as_str()
                            .rsplit('/')
                            .next()
                            .ok_or(ServerError::InvalidManifestReference)?
                            .to_owned();
                        Ok(Candidate {
                            digest_hex,
                            mapping: None,
                        })
                    })
                    .collect()
            })
            .await
        }
    }
}

pub(super) fn mapping(
    repository: &str,
    auth: &AuthorizedRepository,
    digest: &str,
    document: &Value,
) -> Result<OciManifestSubject, ServerError> {
    let subject_digest_hex = document
        .get("subject")
        .and_then(|subject| subject.get("digest"))
        .and_then(Value::as_str)
        .map(parse_sha256_digest)
        .transpose()?;
    Ok(OciManifestSubject {
        scope_namespace: scope_namespace(auth.namespace()),
        repository: repository.to_owned(),
        digest_hex: digest.to_owned(),
        subject_digest_hex,
    })
}

pub(super) async fn retain(
    state: &Arc<AppState>,
    entry: OciManifestSubject,
) -> Result<(), ServerError> {
    retain_index(state, entry, false).await
}

async fn retain_index(
    state: &Arc<AppState>,
    entry: OciManifestSubject,
    reconciliation: bool,
) -> Result<(), ServerError> {
    match &state.backend {
        ServerBackend::Postgres(backend) => {
            if reconciliation {
                backend
                    .index_store()
                    .reconcile_oci_manifest_subject(&entry)
                    .await?;
            } else {
                backend
                    .index_store()
                    .retain_oci_manifest_subject(&entry)
                    .await?;
            }
            Ok(())
        }
        ServerBackend::Local(_) => {
            let root = state.config.root_dir().to_owned();
            crate::admission::blocking_work(&state.pools.blocking_io, move || {
                let index = shardline_index::LocalIndexStore::open(root);
                if reconciliation {
                    index.reconcile_oci_manifest_subject(&entry)?;
                } else {
                    index.retain_oci_manifest_subject(&entry)?;
                }
                Ok(())
            })
            .await
        }
    }
}

pub(super) async fn page(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    subject: Option<&str>,
    after: Option<&str>,
) -> Result<Vec<OciManifestSubject>, ServerError> {
    let scope = scope_namespace(auth.namespace());
    match &state.backend {
        ServerBackend::Postgres(backend) => Ok(backend
            .index_store()
            .list_oci_manifest_subjects(&scope, repository, subject, after, PAGE)
            .await?),
        ServerBackend::Local(_) => {
            let root = state.config.root_dir().to_owned();
            let repository = repository.to_owned();
            let subject = subject.map(str::to_owned);
            let after = after.map(str::to_owned);
            crate::admission::blocking_work(&state.pools.blocking_io, move || {
                Ok(
                    shardline_index::LocalIndexStore::open(root).list_oci_manifest_subjects(
                        &scope,
                        &repository,
                        subject.as_deref(),
                        after.as_deref(),
                        PAGE,
                    )?,
                )
            })
            .await
        }
    }
}

/// Reconcile the immutable inventory on every discovery, including mixed-version
/// writers that cannot populate this optional index. Memory stays page bounded.
/// Warm reconciliation reads metadata, not every unrelated manifest payload.
/// A missing mapping is never interpreted as proof that no referrer exists.
pub(super) async fn reconcile(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
) -> Result<(), ServerError> {
    let prefix = oci_manifest_prefix(repository, auth)?;
    let mut after = None;
    loop {
        let backend = state.backend.clone();
        let prefix = prefix.clone();
        let start = after.clone();
        let inventory = crate::admission::blocking_work(&state.pools.blocking_io, move || {
            backend.list_object_flat_namespace_page(&prefix, start.as_ref(), PAGE)
        })
        .await?;
        let Some(last) = inventory.last() else {
            return Ok(());
        };
        let end = last
            .key()
            .as_str()
            .rsplit('/')
            .next()
            .ok_or(ServerError::InvalidManifestReference)?;
        let mut mapping_after = after
            .as_ref()
            .and_then(|key: &shardline_storage::ObjectKey| key.as_str().rsplit('/').next())
            .map(str::to_owned);
        let mut missing: BTreeSet<_> = inventory
            .iter()
            .map(|metadata| {
                metadata
                    .key()
                    .as_str()
                    .rsplit('/')
                    .next()
                    .ok_or(ServerError::InvalidManifestReference)
                    .map(str::to_owned)
            })
            .collect::<Result<_, _>>()?;
        loop {
            let mappings = page(state, repository, auth, None, mapping_after.as_deref()).await?;
            if mappings.is_empty() {
                break;
            }
            let mut reached_end = false;
            for entry in mappings {
                if entry.digest_hex.as_str() > end {
                    reached_end = true;
                    break;
                }
                missing.remove(&entry.digest_hex);
                mapping_after = Some(entry.digest_hex);
            }
            if reached_end || mapping_after.as_deref() == Some(end) {
                break;
            }
        }
        for metadata in &inventory {
            let key = metadata.key();
            let digest = key
                .as_str()
                .rsplit('/')
                .next()
                .ok_or(ServerError::InvalidManifestReference)?;
            if !missing.contains(digest) {
                continue;
            }
            reconcile_manifest(state, repository, auth, digest, key).await?;
        }
        after = Some(last.key().clone());
    }
}

async fn reconcile_manifest(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    digest: &str,
    key: &shardline_storage::ObjectKey,
) -> Result<(), ServerError> {
    if state
        .backend
        .oci_object_is_deleted(&oci_manifest_index_key(repository, digest, auth))
        .await?
    {
        return Ok(());
    }
    let bytes = match state.backend.read_object(key).await {
        Ok(bytes) => bytes,
        // Inventory can precede concurrent physical reclamation. An absent
        // candidate is not a failure of the entire subject's discovery. Keep
        // corruption and all other backend failures visible to the caller.
        Err(ServerError::NotFound) => return Ok(()),
        Err(error) => return Err(error),
    };
    if hex::encode(Sha256::digest(&bytes)) != digest {
        return Err(ServerError::InvalidManifestReference);
    }
    let document: Value = serde_json::from_slice(&bytes)?;
    retain_index(state, mapping(repository, auth, digest, &document)?, true).await
}
