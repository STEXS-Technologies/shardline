//! Derived subject lookup metadata, never OCI publication authority.

/// The subject extracted from one digest-verified immutable manifest.
///
/// `None` records that the manifest was inspected and has no subject. Missing
/// rows instead require reconciliation with the immutable manifest inventory,
/// including content uploaded by older servers. A row neither publishes bytes
/// nor clears a deletion marker. Digests detect corruption, not forgery by a
/// privileged database writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciManifestSubject {
    /// Repository-scope storage namespace.
    pub scope_namespace: String,
    /// OCI repository name.
    pub repository: String,
    /// Lowercase SHA-256 manifest digest without its algorithm prefix.
    pub digest_hex: String,
    /// Lowercase SHA-256 subject digest, without its algorithm prefix.
    pub subject_digest_hex: Option<String>,
}

impl OciManifestSubject {
    pub(crate) fn valid(&self) -> bool {
        fn digest(value: &str) -> bool {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }
        digest(&self.digest_hex) && self.subject_digest_hex.as_deref().is_none_or(digest)
    }

    pub(crate) fn commitment(&self) -> Result<String, shardline_reliability::ReliabilityError> {
        Ok(shardline_reliability::canonical_state_digest(&(
            "shardline.oci.manifest-subject.v1",
            &self.scope_namespace,
            &self.repository,
            &self.digest_hex,
            &self.subject_digest_hex,
        ))?
        .as_str()
        .to_owned())
    }
}
