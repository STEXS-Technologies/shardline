//! Integration tests for OCI key construction with repository scopes.
//!
//! These tests exercise the public key-construction functions (`oci_blob_key`,
//! `oci_manifest_key`, `oci_tag_key`, `oci_blob_location`, etc.) from an
//! external consumer's perspective, covering different `RepositoryProvider`
//! variants and scope combinations.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string,
    clippy::panic,
    clippy::match_wild_err_arm,
    clippy::ignored_unit_patterns
)]

use shardline_oci_adapter::{
    OciAdapterError, OciReference, oci_blob_key, oci_blob_key_from_namespace, oci_blob_location,
    oci_manifest_key, oci_manifest_key_from_namespace, oci_manifest_location,
    oci_manifest_media_type_key, oci_manifest_media_type_key_from_namespace, oci_manifest_prefix,
    oci_tag_key, oci_tag_prefix, oci_tag_target_key, oci_tag_target_prefix, parse_reference,
    upload_session_location, validate_repository,
};
use shardline_protocol::{RepositoryProvider, RepositoryScope};

// ── Helpers ────────────────────────────────────────────────────────────────

const VALID_DIGEST: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const VALID_DIGEST_HEX: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

const fn global_scope() -> Option<&'static RepositoryScope> {
    None
}

fn make_scope(provider: RepositoryProvider, owner: &str, name: &str) -> RepositoryScope {
    RepositoryScope::new(provider, owner, name, None).expect("valid scope params should succeed")
}

// ── oci_blob_key ───────────────────────────────────────────────────────────

#[test]
fn blob_key_global_scope() {
    let key = oci_blob_key("team/assets", VALID_DIGEST, global_scope()).unwrap();
    let s = key.as_str();
    assert!(s.contains("protocols/oci/global/repos/"));
    assert!(s.contains("/blobs/"));
    assert!(s.contains(VALID_DIGEST));
}

#[test]
fn tombstone_blob_key_rebuild_matches_live_key() {
    let live = oci_blob_key("team/assets", VALID_DIGEST_HEX, global_scope()).unwrap();
    let rebuilt = oci_blob_key_from_namespace("team/assets", VALID_DIGEST_HEX, "global").unwrap();
    assert_eq!(rebuilt, live);
}

#[test]
fn tombstone_keys_rebuild_scoped_live_keys() {
    let scope = make_scope(RepositoryProvider::GitHub, "github-user", "project");
    let live_blob = oci_blob_key("github-user/project", VALID_DIGEST_HEX, Some(&scope)).unwrap();
    let scope_namespace = live_blob
        .as_str()
        .split('/')
        .nth(2)
        .expect("OCI keys include a scope namespace");
    assert_eq!(
        oci_blob_key_from_namespace("github-user/project", VALID_DIGEST_HEX, scope_namespace)
            .unwrap(),
        live_blob
    );
    assert_eq!(
        oci_manifest_key_from_namespace("github-user/project", VALID_DIGEST_HEX, scope_namespace)
            .unwrap(),
        oci_manifest_key("github-user/project", VALID_DIGEST_HEX, Some(&scope)).unwrap()
    );
    assert_eq!(
        oci_manifest_media_type_key_from_namespace(
            "github-user/project",
            VALID_DIGEST_HEX,
            scope_namespace
        )
        .unwrap(),
        oci_manifest_media_type_key("github-user/project", VALID_DIGEST_HEX, Some(&scope)).unwrap()
    );
}

#[test]
fn tombstone_key_rebuild_rejects_corrupt_namespace_and_digest() {
    assert!(matches!(
        oci_blob_key_from_namespace("team/assets", VALID_DIGEST_HEX, "../escape"),
        Err(OciAdapterError::InvalidContentHash)
    ));
    assert!(oci_blob_key_from_namespace("team/assets", "bad", "global").is_err());
}

#[test]
fn blob_key_with_github_scope() {
    let scope = make_scope(RepositoryProvider::GitHub, "my-org", "my-repo");
    let key = oci_blob_key("my-org/my-repo", VALID_DIGEST, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(
        !s.contains("/global/"),
        "scoped key should not contain global"
    );
    assert!(s.contains("/blobs/"), "key should contain blobs path");
}

#[test]
fn blob_key_with_gitea_scope() {
    let scope = make_scope(RepositoryProvider::Gitea, "gitea-user", "project");
    let key = oci_blob_key("gitea-user/project", VALID_DIGEST, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/blobs/"));
}

#[test]
fn blob_key_with_gitlab_scope() {
    let scope = make_scope(RepositoryProvider::GitLab, "group", "subgroup/project");
    let key = oci_blob_key("group/subgroup/project", VALID_DIGEST, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/blobs/"));
}

#[test]
fn blob_key_with_codeberg_scope() {
    let scope = make_scope(RepositoryProvider::Codeberg, "user", "repo");
    let key = oci_blob_key("user/repo", VALID_DIGEST, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/blobs/"));
}

#[test]
fn blob_key_with_generic_scope() {
    let scope = make_scope(RepositoryProvider::Generic, "forge", "project");
    let key = oci_blob_key("forge/project", VALID_DIGEST, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/blobs/"));
}

#[test]
fn blob_key_rejects_empty_repo() {
    assert!(matches!(
        oci_blob_key("", VALID_DIGEST, global_scope()),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn blob_key_rejects_invalid_repo() {
    assert!(matches!(
        oci_blob_key("../escape", VALID_DIGEST, global_scope()),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn blob_key_with_nested_repository() {
    let key = oci_blob_key("org/team/project/cache", VALID_DIGEST, global_scope()).unwrap();
    let s = key.as_str();
    // The repository is hashed, but the key should contain the digest
    assert!(s.contains(VALID_DIGEST));
}

// ── oci_manifest_key ───────────────────────────────────────────────────────

#[test]
fn manifest_key_global_scope() {
    let key = oci_manifest_key("team/assets", VALID_DIGEST, global_scope()).unwrap();
    let s = key.as_str();
    assert!(s.contains("protocols/oci/global/repos/"));
    assert!(s.contains("/manifests/"));
}

#[test]
fn manifest_key_with_scope() {
    let scope = make_scope(RepositoryProvider::GitHub, "org", "repo");
    let key = oci_manifest_key("org/repo", VALID_DIGEST, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/manifests/"));
}

#[test]
fn manifest_key_rejects_empty_repo() {
    assert!(matches!(
        oci_manifest_key("", VALID_DIGEST, global_scope()),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

// ── oci_tag_key ────────────────────────────────────────────────────────────

#[test]
fn tag_key_global_scope() {
    let key = oci_tag_key("team/assets", "latest", global_scope()).unwrap();
    let s = key.as_str();
    assert!(s.contains("protocols/oci/global/repos/"));
    assert!(s.contains("/tags/latest"));
}

#[test]
fn tag_key_with_scope() {
    let scope = make_scope(RepositoryProvider::GitLab, "group", "project");
    let key = oci_tag_key("group/project", "v1.2.3", Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/tags/v1.2.3"));
}

#[test]
fn tag_key_rejects_empty_tag() {
    assert!(matches!(
        oci_tag_key("team/assets", "", global_scope()),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

#[test]
fn tag_key_rejects_invalid_tag() {
    assert!(matches!(
        oci_tag_key("team/assets", "-starts-with-hyphen", global_scope()),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

// ── oci_manifest_media_type_key ─────────────────────────────────────────────

#[test]
fn manifest_media_type_key_contains_correct_path() {
    let key = oci_manifest_media_type_key("team/assets", VALID_DIGEST, global_scope()).unwrap();
    let s = key.as_str();
    assert!(s.contains("/manifest-media-types/"));
}

#[test]
fn manifest_media_type_key_with_scope() {
    let scope = make_scope(RepositoryProvider::GitHub, "org", "repo");
    let key = oci_manifest_media_type_key("org/repo", VALID_DIGEST, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/manifest-media-types/"));
}

// ── oci_manifest_prefix / oci_tag_prefix ───────────────────────────────────

#[test]
fn manifest_prefix_contains_manifests() {
    let prefix = oci_manifest_prefix("team/assets", global_scope()).unwrap();
    assert!(prefix.as_str().contains("/manifests/"));
}

#[test]
fn manifest_prefix_with_scope() {
    let scope = make_scope(RepositoryProvider::GitHub, "org", "repo");
    let prefix = oci_manifest_prefix("org/repo", Some(&scope)).unwrap();
    assert!(!prefix.as_str().contains("/global/"));
    assert!(prefix.as_str().contains("/manifests/"));
}

#[test]
fn tag_prefix_contains_tags() {
    let prefix = oci_tag_prefix("team/assets", global_scope()).unwrap();
    assert!(prefix.as_str().contains("/tags/"));
}

#[test]
fn tag_prefix_with_scope() {
    let scope = make_scope(RepositoryProvider::Gitea, "user", "repo");
    let prefix = oci_tag_prefix("user/repo", Some(&scope)).unwrap();
    assert!(!prefix.as_str().contains("/global/"));
    assert!(prefix.as_str().contains("/tags/"));
}

// ── oci_tag_target_key / oci_tag_target_prefix ──────────────────────────────

#[test]
fn tag_target_key_global() {
    let key =
        oci_tag_target_key("team/assets", VALID_DIGEST_HEX, "stable", global_scope()).unwrap();
    let s = key.as_str();
    assert!(s.contains("/tag-targets/"));
    assert!(s.contains(VALID_DIGEST_HEX));
    assert!(s.ends_with("/stable"));
}

#[test]
fn tag_target_key_with_scope() {
    let scope = make_scope(RepositoryProvider::Generic, "forge", "project");
    let key = oci_tag_target_key("forge/project", VALID_DIGEST_HEX, "rc1", Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/tag-targets/"));
}

#[test]
fn tag_target_prefix_global() {
    let prefix = oci_tag_target_prefix("team/assets", VALID_DIGEST_HEX, global_scope()).unwrap();
    let s = prefix.as_str();
    assert!(s.contains("/tag-targets/"));
    assert!(s.ends_with('/'));
}

#[test]
fn tag_target_prefix_with_scope() {
    let scope = make_scope(RepositoryProvider::GitLab, "group", "project");
    let prefix = oci_tag_target_prefix("group/project", VALID_DIGEST_HEX, Some(&scope)).unwrap();
    assert!(!prefix.as_str().contains("/global/"));
    assert!(prefix.as_str().contains("/tag-targets/"));
}

// ── oci_blob_location / oci_manifest_location / upload_session_location ─────

#[test]
fn blob_location_format() {
    let loc = oci_blob_location("myrepo", "abcdef");
    assert_eq!(loc, "/v2/myrepo/blobs/sha256:abcdef");
}

#[test]
fn blob_location_with_nested_repo() {
    let loc = oci_blob_location("org/team/project", "deadbeef");
    assert_eq!(loc, "/v2/org/team/project/blobs/sha256:deadbeef");
}

#[test]
fn manifest_location_with_tag() {
    let loc = oci_manifest_location("myrepo", "latest");
    assert_eq!(loc, "/v2/myrepo/manifests/latest");
}

#[test]
fn manifest_location_with_digest() {
    let loc = oci_manifest_location("myrepo", "sha256:abcdef");
    assert_eq!(loc, "/v2/myrepo/manifests/sha256:abcdef");
}

#[test]
fn upload_session_location_format() {
    let loc = upload_session_location("myrepo", "session-123");
    assert_eq!(loc, "/v2/myrepo/blobs/uploads/session-123");
}

// ── parse_reference ────────────────────────────────────────────────────────

#[test]
fn parse_reference_digest() {
    let r = parse_reference(VALID_DIGEST).unwrap();
    assert!(matches!(r, OciReference::Digest(ref d) if d == VALID_DIGEST_HEX));
}

#[test]
fn parse_reference_tag() {
    let r = parse_reference("latest").unwrap();
    assert!(matches!(r, OciReference::Tag(ref t) if t == "latest"));
}

#[test]
fn parse_reference_tag_with_dot_and_hyphen() {
    let r = parse_reference("v1.0.0-rc1").unwrap();
    assert!(matches!(r, OciReference::Tag(ref t) if t == "v1.0.0-rc1"));
}

#[test]
fn parse_reference_empty_errors() {
    assert!(matches!(
        parse_reference(""),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

#[test]
fn parse_reference_invalid_digest_errors() {
    assert!(matches!(
        parse_reference("sha256:nothex"),
        Err(OciAdapterError::InvalidDigest)
    ));
}

// ── validate_repository ────────────────────────────────────────────────────

#[test]
fn validate_repository_accepts_valid() {
    assert!(validate_repository("team/assets").is_ok());
}

#[test]
fn validate_repository_accepts_deeply_nested() {
    assert!(validate_repository("a/b/c/d/e").is_ok());
}

#[test]
fn validate_repository_rejects_empty() {
    assert!(matches!(
        validate_repository(""),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn validate_repository_rejects_traversal() {
    assert!(matches!(
        validate_repository("../assets"),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn validate_repository_rejects_uppercase() {
    assert!(matches!(
        validate_repository("Team/assets"),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

// ── Different repository providers produce different keys ──────────────────

#[test]
fn different_providers_produce_different_blob_keys() {
    let github = make_scope(RepositoryProvider::GitHub, "org", "repo");
    let gitlab = make_scope(RepositoryProvider::GitLab, "org", "repo");
    let key_gh = oci_blob_key("org/repo", VALID_DIGEST, Some(&github)).unwrap();
    let key_gl = oci_blob_key("org/repo", VALID_DIGEST, Some(&gitlab)).unwrap();
    assert_ne!(
        key_gh.as_str(),
        key_gl.as_str(),
        "different providers should produce different keys"
    );
}

#[test]
fn different_owners_produce_different_manifest_keys() {
    let scope_a = make_scope(RepositoryProvider::GitHub, "team-a", "repo");
    let scope_b = make_scope(RepositoryProvider::GitHub, "team-b", "repo");
    // Repo names must match the scope prefix; use a common sub-path
    let key_a = oci_manifest_key("team-a/repo", VALID_DIGEST, Some(&scope_a)).unwrap();
    let key_b = oci_manifest_key("team-b/repo", VALID_DIGEST, Some(&scope_b)).unwrap();
    assert_ne!(
        key_a.as_str(),
        key_b.as_str(),
        "different owners should produce different keys"
    );
}

#[test]
fn different_repo_names_produce_different_tag_keys() {
    let scope_a = make_scope(RepositoryProvider::GitHub, "org", "repo-a");
    let scope_b = make_scope(RepositoryProvider::GitHub, "org", "repo-b");
    let key_a = oci_tag_key("org/repo-a", "latest", Some(&scope_a)).unwrap();
    let key_b = oci_tag_key("org/repo-b", "latest", Some(&scope_b)).unwrap();
    assert_ne!(
        key_a.as_str(),
        key_b.as_str(),
        "different repo names should produce different keys"
    );
}
