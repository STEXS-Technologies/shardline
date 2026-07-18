mod bazel;
mod helpers;
mod lfs;
pub(super) mod oci;

pub(crate) use bazel::{
    bazel_get, bazel_get_ac, bazel_get_cas, bazel_head, bazel_head_ac, bazel_head_cas, bazel_put,
    bazel_put_ac, bazel_put_cas,
};
pub(crate) use helpers::{
    direct_object_response, ensure_upload_growth_within_limit, parse_query_map, parse_query_values,
    parse_upload_content_range,
};
pub(crate) use lfs::{
    lfs_batch, lfs_delete_object, lfs_get_object, lfs_head_object, lfs_patch_object,
    lfs_put_object, lfs_verify_object,
};
#[cfg(feature = "fuzzing")]
pub(crate) use oci::parse_oci_path;
pub(crate) use oci::{
    oci_api_dispatch, oci_dispatch, oci_registry_token, oci_transfer_dispatch, oci_v2_root,
};

use super::MAX_PROTOCOL_QUERY_BYTES;

// Re-export parent items needed by sibling sub-modules (bazel.rs, lfs.rs, oci/).
use super::reconstruction_helpers::{byte_range_stream_response, full_byte_stream_response};
use super::{
    AppState, MAX_LFS_BATCH_OBJECTS, MAX_OCI_MANIFEST_TAGS, MAX_OCI_TAG_LIST_PAGE_SIZE, authorize,
    scope_from_auth,
};

#[cfg(test)]
mod tests;
