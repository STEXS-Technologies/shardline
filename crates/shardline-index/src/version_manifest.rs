//! Bounded, versioned recovery envelopes over authoritative reconstruction records.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use shardline_protocol::RepositoryScope;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
use thiserror::Error;

use crate::{
    FileRecord,
    hub::{HubFileEntry, HubRevision},
};

/// Maximum canonical metadata object accepted by a v1 manifest decoder.
pub const VERSION_MANIFEST_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Maximum number of reconstruction terms or Hub tree entries in a manifest.
pub const VERSION_MANIFEST_MAX_TERMS: usize = 100_000;
const FORMAT_VERSION: u16 = 1;

/// Immutable archive envelope for one repository-scoped file version.
///
/// Reconstruction fields remain in [`FileRecord`]. The envelope supplies only
/// deployment/resource isolation and identities that the record cannot derive
/// without its publication scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileVersionManifestV1 {
    pub format_version: u16,
    pub deployment_id: String,
    pub resource_domain: String,
    pub repository_stable_id: String,
    pub record: FileRecord,
}

/// Immutable archive envelope for a Hub revision and materialized tree.
/// The exact Git commit/tree object closure remains in Hub's existing identity
/// archive; `public_commit_id` pins this snapshot to that identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubTreeVersionManifestV1 {
    pub format_version: u16,
    pub deployment_id: String,
    pub resource_domain: String,
    pub revision: HubRevision,
    pub public_commit_id: String,
    /// Existing ordered tree entries; order is preserved in the canonical bytes.
    pub files: Vec<HubFileEntry>,
}

/// Stable locator for a canonical recovery manifest object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestLocator {
    pub digest: String,
    pub key: ObjectKey,
    pub outcome: PutOutcome,
}

/// Invalid, unsupported, oversized, or unwritable immutable manifest data.
#[derive(Debug, Error)]
pub enum VersionManifestError {
    #[error("manifest identity field is invalid: {0}")]
    InvalidIdentity(&'static str),
    #[error("unsupported manifest format version")]
    UnsupportedVersion,
    #[error("manifest exceeds its byte or entry limit")]
    LimitExceeded,
    #[error("file reconstruction record is invalid: {0}")]
    InvalidFileRecord(#[from] crate::FileRecordInvariantError),
    #[error("manifest serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("manifest storage key is invalid: {0}")]
    ObjectKey(#[from] shardline_storage::ObjectKeyError),
    #[error("manifest identity or digest is invalid")]
    DigestMismatch,
    #[error("manifest contains duplicate or invalid Hub paths")]
    InvalidTree,
    #[error("manifest storage failed: {0}")]
    Storage(String),
}

impl FileVersionManifestV1 {
    /// Creates a v1 envelope using the publication's authoritative repository scope.
    ///
    /// # Errors
    /// Returns an error if identity fields, scope, or the reconstruction record is invalid.
    pub fn new(
        deployment_id: impl Into<String>,
        resource_domain: impl Into<String>,
        publication_scope: &RepositoryScope,
        record: FileRecord,
    ) -> Result<Self, VersionManifestError> {
        let deployment_id = deployment_id.into();
        let resource_domain = resource_domain.into();
        validate_namespace("deployment_id", &deployment_id)?;
        validate_namespace("resource_domain", &resource_domain)?;
        record.validate_reconstruction_plan()?;
        validate_record_bounds(&record)?;
        if let Some(record_scope) = record.repository_scope.as_ref()
            && !same_repository(record_scope, publication_scope)
        {
            return Err(VersionManifestError::InvalidIdentity("repository_scope"));
        }
        let repository_stable_id =
            repository_stable_id(&deployment_id, &resource_domain, publication_scope)?;
        let manifest = Self {
            format_version: FORMAT_VERSION,
            deployment_id,
            resource_domain,
            repository_stable_id,
            record,
        };
        manifest.validate(publication_scope)?;
        Ok(manifest)
    }

    /// Revalidates version and reconstruction identity against the caller's scope.
    ///
    /// # Errors
    /// Returns an error if the format, scope, derived identity, or reconstruction plan is invalid.
    pub fn validate(
        &self,
        publication_scope: &RepositoryScope,
    ) -> Result<(), VersionManifestError> {
        if self.format_version != FORMAT_VERSION {
            return Err(VersionManifestError::UnsupportedVersion);
        }
        validate_namespace("deployment_id", &self.deployment_id)?;
        validate_namespace("resource_domain", &self.resource_domain)?;
        self.record.validate_reconstruction_plan()?;
        validate_record_bounds(&self.record)?;
        if self
            .record
            .repository_scope
            .as_ref()
            .is_some_and(|record_scope| !same_repository(record_scope, publication_scope))
        {
            return Err(VersionManifestError::InvalidIdentity("repository_scope"));
        }
        let repository = repository_stable_id(
            &self.deployment_id,
            &self.resource_domain,
            publication_scope,
        )?;
        if self.repository_stable_id != repository {
            return Err(VersionManifestError::DigestMismatch);
        }
        Ok(())
    }

    /// Stable version identity derived from the scoped existing file version.
    ///
    /// # Errors
    /// Returns an error if serializing the identity tuple fails.
    pub fn immutable_version_id(&self) -> Result<String, VersionManifestError> {
        file_version_id(
            &self.deployment_id,
            &self.resource_domain,
            &self.repository_stable_id,
            &self.record,
        )
    }

    /// Hashes the exact existing FileRecord serialization for provenance/inventory references.
    ///
    /// # Errors
    /// Returns an error if the record cannot be serialized.
    pub fn canonical_metadata_digest(&self) -> Result<String, VersionManifestError> {
        digest_serialized(&self.record)
    }

    /// Encodes deterministic struct/vector-order JSON under the v1 byte cap.
    ///
    /// # Errors
    /// Returns an error if validation fails, serialization fails, or the byte cap is exceeded.
    pub fn canonical_bytes(
        &self,
        publication_scope: &RepositoryScope,
    ) -> Result<Vec<u8>, VersionManifestError> {
        self.validate(publication_scope)?;
        encode_bounded(self)
    }

    /// Returns the immutable object digest, key and integrity for canonical bytes.
    ///
    /// # Errors
    /// Returns an error if validation, encoding, or object-key construction fails.
    pub fn storage_identity(
        &self,
        publication_scope: &RepositoryScope,
    ) -> Result<(String, ObjectKey, ObjectIntegrity), VersionManifestError> {
        storage_identity(
            &self.canonical_bytes(publication_scope)?,
            &self.deployment_id,
            &self.repository_stable_id,
        )
    }

    /// Decodes bounded v1 JSON and validates the full reconstruction plan and scope.
    ///
    /// # Errors
    /// Returns an error for oversized, malformed, unsupported, or inconsistent manifests.
    pub fn decode(
        bytes: &[u8],
        publication_scope: &RepositoryScope,
    ) -> Result<Self, VersionManifestError> {
        let manifest: Self = decode_bounded(bytes)?;
        manifest.validate(publication_scope)?;
        Ok(manifest)
    }

    /// Stores canonical bytes in the existing verified immutable object store.
    ///
    /// # Errors
    /// Returns an error if validation fails or the object store rejects the immutable write.
    pub fn store<S: ObjectStore>(
        &self,
        publication_scope: &RepositoryScope,
        store: &S,
    ) -> Result<ManifestLocator, VersionManifestError>
    where
        S::Error: std::fmt::Display,
    {
        let bytes = self.canonical_bytes(publication_scope)?;
        store_manifest(
            &bytes,
            &self.deployment_id,
            &self.repository_stable_id,
            store,
        )
    }
}

impl HubTreeVersionManifestV1 {
    /// Creates a Hub snapshot using the repository identity from authorized publication context.
    ///
    /// # Errors
    /// Returns an error if the deployment identity, revision, tree, or byte bounds are invalid.
    pub fn new(
        deployment_id: impl Into<String>,
        resource_domain: impl Into<String>,
        revision: HubRevision,
        public_commit_id: impl Into<String>,
        mut files: Vec<HubFileEntry>,
    ) -> Result<Self, VersionManifestError> {
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let manifest = Self {
            format_version: FORMAT_VERSION,
            deployment_id: deployment_id.into(),
            resource_domain: resource_domain.into(),
            revision,
            public_commit_id: public_commit_id.into(),
            files,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Stable immutable identity derived from the scoped existing Hub revision.
    ///
    /// # Errors
    /// Returns an error if serializing the identity tuple fails.
    pub fn immutable_version_id(&self) -> Result<String, VersionManifestError> {
        digest_serialized(&(
            "shardline-hub-revision-v1",
            &self.deployment_id,
            &self.resource_domain,
            &self.revision.repo_id,
            &self.revision.sha,
        ))
    }

    /// Stable repository identity derived from deployment/domain and HubStore's repository key.
    ///
    /// # Errors
    /// Returns an error if serializing the identity tuple fails.
    pub fn repository_stable_id(&self) -> Result<String, VersionManifestError> {
        digest_serialized(&(
            "shardline-hub-repository-v1",
            &self.deployment_id,
            &self.resource_domain,
            &self.revision.repo_id,
        ))
    }

    /// Revalidates bounded tree entries and scoped revision identity.
    ///
    /// # Errors
    /// Returns an error if the format, identity, path set, or tree bounds are invalid.
    pub fn validate(&self) -> Result<(), VersionManifestError> {
        if self.format_version != FORMAT_VERSION {
            return Err(VersionManifestError::UnsupportedVersion);
        }
        validate_namespace("deployment_id", &self.deployment_id)?;
        validate_namespace("resource_domain", &self.resource_domain)?;
        validate_namespace("public_commit_id", &self.public_commit_id)?;
        validate_identifier_text("hub_repo_id", &self.revision.repo_id, 4096)?;
        validate_namespace("hub_revision_sha", &self.revision.sha)?;
        validate_identifier_text("hub_ref_name", &self.revision.ref_name, 4096)?;
        if self
            .revision
            .message
            .as_ref()
            .is_some_and(|message| message.len() > 4096)
        {
            return Err(VersionManifestError::LimitExceeded);
        }
        if self.files.len() > VERSION_MANIFEST_MAX_TERMS {
            return Err(VersionManifestError::LimitExceeded);
        }
        let mut previous_path: Option<&str> = None;
        let mut estimated_bytes = 1024_usize;
        for file in &self.files {
            if file.path.is_empty()
                || file.path.len() > 4096
                || file.sha.is_empty()
                || file.sha.len() > 128
                || previous_path.is_some_and(|previous| previous >= file.path.as_str())
            {
                return Err(VersionManifestError::InvalidTree);
            }
            previous_path = Some(file.path.as_str());
            estimated_bytes = add_bounded_size(estimated_bytes, file.path.len())?;
            estimated_bytes = add_bounded_size(estimated_bytes, file.sha.len())?;
            estimated_bytes = add_bounded_size(estimated_bytes, 64)?;
            if estimated_bytes > VERSION_MANIFEST_MAX_BYTES {
                return Err(VersionManifestError::LimitExceeded);
            }
        }
        Ok(())
    }

    /// Encodes deterministic struct/vector-order JSON under the v1 byte cap.
    ///
    /// # Errors
    /// Returns an error if validation fails, serialization fails, or the byte cap is exceeded.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, VersionManifestError> {
        self.validate()?;
        encode_bounded(self)
    }

    /// Returns the immutable object digest, key and integrity for canonical bytes.
    ///
    /// # Errors
    /// Returns an error if validation, encoding, or object-key construction fails.
    pub fn storage_identity(
        &self,
    ) -> Result<(String, ObjectKey, ObjectIntegrity), VersionManifestError> {
        storage_identity(
            &self.canonical_bytes()?,
            &self.deployment_id,
            &self.repository_stable_id()?,
        )
    }

    /// Decodes bounded v1 JSON and validates all tree entries.
    ///
    /// # Errors
    /// Returns an error for oversized, malformed, unsupported, or inconsistent manifests.
    pub fn decode(bytes: &[u8]) -> Result<Self, VersionManifestError> {
        let manifest: Self = decode_bounded(bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Stores canonical bytes in the existing verified immutable object store.
    ///
    /// # Errors
    /// Returns an error if validation fails or the object store rejects the immutable write.
    pub fn store<S: ObjectStore>(&self, store: &S) -> Result<ManifestLocator, VersionManifestError>
    where
        S::Error: std::fmt::Display,
    {
        self.validate()?;
        let bytes = self.canonical_bytes()?;
        let repository_stable_id = self.repository_stable_id()?;
        store_manifest(&bytes, &self.deployment_id, &repository_stable_id, store)
    }
}

fn store_manifest<S: ObjectStore>(
    bytes: &[u8],
    deployment_id: &str,
    repository_stable_id: &str,
    store: &S,
) -> Result<ManifestLocator, VersionManifestError>
where
    S::Error: std::fmt::Display,
{
    let (digest, key, integrity) = storage_identity(bytes, deployment_id, repository_stable_id)?;
    let outcome = store
        .put_if_absent(&key, ObjectBody::from_slice(bytes), &integrity)
        .map_err(|error| VersionManifestError::Storage(error.to_string()))?;
    Ok(ManifestLocator {
        digest,
        key,
        outcome,
    })
}

fn storage_identity(
    bytes: &[u8],
    deployment_id: &str,
    repository_stable_id: &str,
) -> Result<(String, ObjectKey, ObjectIntegrity), VersionManifestError> {
    let integrity = ObjectIntegrity::for_bytes(bytes);
    let digest = integrity.hash().hex_string();
    let prefix = digest
        .get(..2)
        .ok_or(VersionManifestError::DigestMismatch)?;
    let deployment_key = ObjectIntegrity::for_bytes(deployment_id.as_bytes())
        .hash()
        .hex_string();
    let repository_key = ObjectIntegrity::for_bytes(repository_stable_id.as_bytes())
        .hash()
        .hex_string();
    let key = ObjectKey::parse(&format!(
        "recovery/v1/{deployment_key}/{repository_key}/versions/{prefix}/{digest}"
    ))?;
    Ok((digest, key, integrity))
}

fn encode_bounded<T: Serialize>(value: &T) -> Result<Vec<u8>, VersionManifestError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > VERSION_MANIFEST_MAX_BYTES {
        return Err(VersionManifestError::LimitExceeded);
    }
    Ok(bytes)
}

fn decode_bounded<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, VersionManifestError> {
    if bytes.len() > VERSION_MANIFEST_MAX_BYTES {
        return Err(VersionManifestError::LimitExceeded);
    }
    Ok(serde_json::from_slice(bytes)?)
}

fn repository_stable_id(
    deployment_id: &str,
    resource_domain: &str,
    scope: &RepositoryScope,
) -> Result<String, VersionManifestError> {
    digest_serialized(&(
        "shardline-repository-v1",
        deployment_id,
        resource_domain,
        scope.provider().as_str(),
        scope.owner(),
        scope.name(),
    ))
}

fn file_version_id(
    deployment_id: &str,
    resource_domain: &str,
    repository_stable_id: &str,
    record: &FileRecord,
) -> Result<String, VersionManifestError> {
    digest_serialized(&(
        "shardline-file-version-v1",
        deployment_id,
        resource_domain,
        repository_stable_id,
        &record.file_id,
        &record.content_hash,
    ))
}

fn digest_serialized(value: &impl Serialize) -> Result<String, VersionManifestError> {
    let bytes = serde_json::to_vec(value)?;
    Ok(hex::encode(
        ObjectIntegrity::for_bytes(&bytes).hash().as_bytes(),
    ))
}

fn same_repository(left: &RepositoryScope, right: &RepositoryScope) -> bool {
    left.provider() == right.provider()
        && left.owner() == right.owner()
        && left.name() == right.name()
}

fn validate_record_bounds(record: &FileRecord) -> Result<(), VersionManifestError> {
    if record.chunks.len() > VERSION_MANIFEST_MAX_TERMS
        || record.file_id.is_empty()
        || record.file_id.len() > 4096
        || record.content_hash.is_empty()
        || record.content_hash.len() > 256
    {
        return Err(VersionManifestError::LimitExceeded);
    }
    let mut estimated_bytes = 1024_usize;
    estimated_bytes = add_bounded_size(estimated_bytes, record.file_id.len())?;
    estimated_bytes = add_bounded_size(estimated_bytes, record.content_hash.len())?;
    for chunk in &record.chunks {
        estimated_bytes = add_bounded_size(estimated_bytes, chunk.hash.len())?;
        estimated_bytes = add_bounded_size(estimated_bytes, 192)?;
        if estimated_bytes > VERSION_MANIFEST_MAX_BYTES {
            return Err(VersionManifestError::LimitExceeded);
        }
    }
    Ok(())
}

fn add_bounded_size(current: usize, additional: usize) -> Result<usize, VersionManifestError> {
    current
        .checked_add(additional)
        .ok_or(VersionManifestError::LimitExceeded)
}

fn validate_namespace(field: &'static str, value: &str) -> Result<(), VersionManifestError> {
    if value.is_empty()
        || value.len() > 256
        || value.chars().any(|character| {
            !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':'))
        })
    {
        return Err(VersionManifestError::InvalidIdentity(field));
    }
    Ok(())
}

fn validate_identifier_text(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), VersionManifestError> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(VersionManifestError::InvalidIdentity(field));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use shardline_storage::{LocalObjectStore, PutOutcome};

    use super::*;
    use crate::{StorageRepresentation, hub::HubRevision};

    fn record(scope: Option<RepositoryScope>) -> FileRecord {
        FileRecord {
            file_id: "assets/model.bin".to_owned(),
            content_hash: "a1".repeat(32),
            total_bytes: 0,
            chunk_size: 1024,
            storage_repr: StorageRepresentation::XorbCdcV1,
            repository_scope: scope,
            deduplication_domain: None,
            chunks: Vec::new(),
        }
    }

    fn scope(owner: &str) -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, owner, "models", Some("main")).unwrap()
    }

    #[test]
    fn file_manifest_roundtrips_zero_byte_record_and_is_repository_scoped() {
        let publication_scope = scope("alice");
        let manifest = FileVersionManifestV1::new(
            "deployment-a",
            "files",
            &publication_scope,
            record(Some(publication_scope.clone())),
        )
        .unwrap();
        let bytes = manifest.canonical_bytes(&publication_scope).unwrap();
        let decoded = FileVersionManifestV1::decode(&bytes, &publication_scope).unwrap();
        assert_eq!(decoded, manifest);
        assert!(manifest.record.chunks.is_empty());
        assert!(FileVersionManifestV1::decode(&bytes, &scope("bob")).is_err());
        assert_ne!(
            manifest.repository_stable_id,
            repository_stable_id("deployment-a", "files", &scope("bob")).unwrap()
        );
        let same_repo_other_ref = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "alice",
            "models",
            Some("feature"),
        )
        .unwrap();
        assert_eq!(
            manifest.repository_stable_id,
            repository_stable_id("deployment-a", "files", &same_repo_other_ref).unwrap()
        );
        let other =
            FileVersionManifestV1::new("deployment-a", "files", &scope("bob"), record(None))
                .unwrap();
        assert_ne!(
            manifest.immutable_version_id().unwrap(),
            other.immutable_version_id().unwrap()
        );
    }

    #[test]
    fn file_manifest_rejects_tampering_unknown_fields_and_oversized_input() {
        let scope = scope("alice");
        let manifest =
            FileVersionManifestV1::new("deployment-a", "files", &scope, record(None)).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&manifest.canonical_bytes(&scope).unwrap()).unwrap();
        value["record"]["total_bytes"] = serde_json::json!(1);
        assert!(
            FileVersionManifestV1::decode(&serde_json::to_vec(&value).unwrap(), &scope).is_err()
        );
        value["record"]["total_bytes"] = serde_json::json!(0);
        value["unknown"] = serde_json::json!(true);
        assert!(
            FileVersionManifestV1::decode(&serde_json::to_vec(&value).unwrap(), &scope).is_err()
        );
        assert!(matches!(
            FileVersionManifestV1::decode(&vec![0; VERSION_MANIFEST_MAX_BYTES + 1], &scope),
            Err(VersionManifestError::LimitExceeded)
        ));
    }

    #[test]
    fn file_manifest_store_is_content_addressed_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(root.path().join("objects")).unwrap();
        let scope = scope("alice");
        let manifest =
            FileVersionManifestV1::new("deployment-a", "files", &scope, record(None)).unwrap();
        let first = manifest.store(&scope, &store).unwrap();
        assert_eq!(first.outcome, PutOutcome::Inserted);
        let second = manifest.store(&scope, &store).unwrap();
        assert_eq!(second.outcome, PutOutcome::AlreadyExists);
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.key, second.key);
        assert!(store.contains(&first.key).unwrap());
    }

    #[test]
    fn hub_tree_manifest_preserves_revision_and_ordered_paths() {
        let revision = HubRevision {
            repo_id: "alice/models".to_owned(),
            ref_name: "main".to_owned(),
            sha: "a".repeat(64),
            parent_sha: Some("b".repeat(64)),
            message: Some("version 1".to_owned()),
            created_at_unix_seconds: 1_700_000_000,
        };
        let files = vec![
            HubFileEntry {
                path: "folder/b.bin".to_owned(),
                size: 4,
                sha: "d".repeat(64),
                is_lfs: true,
            },
            HubFileEntry {
                path: "a.bin".to_owned(),
                size: 0,
                sha: "c".repeat(64),
                is_lfs: false,
            },
        ];
        let manifest = HubTreeVersionManifestV1::new(
            "deployment-a",
            "hub-repositories",
            revision,
            "e".repeat(40),
            files,
        )
        .unwrap();
        let bytes = manifest.canonical_bytes().unwrap();
        let decoded = HubTreeVersionManifestV1::decode(&bytes).unwrap();
        assert_eq!(decoded, manifest);
        assert_eq!(decoded.files[0].path, "a.bin");
        assert_eq!(decoded.files[1].path, "folder/b.bin");
        assert_eq!(
            decoded.immutable_version_id().unwrap(),
            manifest.immutable_version_id().unwrap()
        );
        assert_ne!(decoded.repository_stable_id().unwrap(), "");
    }

    #[test]
    fn hub_tree_manifest_rejects_duplicate_paths() {
        let revision = HubRevision {
            repo_id: "alice/models".to_owned(),
            ref_name: "main".to_owned(),
            sha: "a".repeat(64),
            parent_sha: None,
            message: None,
            created_at_unix_seconds: 1,
        };
        let file = HubFileEntry {
            path: "file.bin".to_owned(),
            size: 1,
            sha: "b".repeat(64),
            is_lfs: false,
        };
        assert!(matches!(
            HubTreeVersionManifestV1::new(
                "deployment-a",
                "hub-repositories",
                revision,
                "c".repeat(40),
                vec![file.clone(), file]
            ),
            Err(VersionManifestError::InvalidTree)
        ));
    }
}
