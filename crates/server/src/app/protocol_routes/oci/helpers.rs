use axum::http::Method;

use super::path::OciPath;

pub(crate) const fn oci_route_served_by_api(method: &Method, path: &OciPath) -> bool {
    matches!(
        (method, path),
        (
            &Method::GET,
            OciPath::Manifest { .. } | OciPath::TagsList { .. }
        ) | (&Method::HEAD, OciPath::Manifest { .. })
            | (&Method::PUT, OciPath::Manifest { .. })
            | (&Method::DELETE, OciPath::Manifest { .. })
            | (&Method::DELETE, OciPath::Blob { .. })
    )
}

pub(crate) const fn oci_route_served_by_transfer(method: &Method, path: &OciPath) -> bool {
    matches!(
        (method, path),
        (
            &Method::GET,
            OciPath::Blob { .. } | OciPath::BlobUploadSession { .. }
        ) | (&Method::HEAD, OciPath::Blob { .. })
            | (&Method::POST, OciPath::BlobUploads { .. })
            | (&Method::PATCH, OciPath::BlobUploadSession { .. })
            | (&Method::PUT, OciPath::BlobUploadSession { .. })
            | (&Method::DELETE, OciPath::BlobUploadSession { .. })
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::protocol_routes::oci::path::OciPath;

    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const SESSION: &str = "0000000000000001";
    const REPO: &str = "team/assets";

    fn blob_path() -> OciPath {
        OciPath::Blob {
            repository: REPO.to_owned(),
            digest_hex: DIGEST.to_owned(),
        }
    }

    fn manifest_path() -> OciPath {
        OciPath::Manifest {
            repository: REPO.to_owned(),
            reference: "v1".to_owned(),
        }
    }

    fn blob_uploads_path() -> OciPath {
        OciPath::BlobUploads {
            repository: REPO.to_owned(),
        }
    }

    fn blob_upload_session_path() -> OciPath {
        OciPath::BlobUploadSession {
            repository: REPO.to_owned(),
            session_id: SESSION.to_owned(),
        }
    }

    fn tags_list_path() -> OciPath {
        OciPath::TagsList {
            repository: REPO.to_owned(),
        }
    }

    // ── oci_route_served_by_api ────────────────────────────────────────

    #[test]
    fn api_serves_get_manifest() {
        assert!(oci_route_served_by_api(&Method::GET, &manifest_path()));
    }

    #[test]
    fn api_serves_head_manifest() {
        assert!(oci_route_served_by_api(&Method::HEAD, &manifest_path()));
    }

    #[test]
    fn api_serves_put_manifest() {
        assert!(oci_route_served_by_api(&Method::PUT, &manifest_path()));
    }

    #[test]
    fn api_serves_delete_manifest() {
        assert!(oci_route_served_by_api(&Method::DELETE, &manifest_path()));
    }

    #[test]
    fn api_serves_get_tags_list() {
        assert!(oci_route_served_by_api(&Method::GET, &tags_list_path()));
    }

    #[test]
    fn api_serves_delete_blob() {
        assert!(oci_route_served_by_api(&Method::DELETE, &blob_path()));
    }

    #[test]
    fn api_rejects_get_blob() {
        assert!(!oci_route_served_by_api(&Method::GET, &blob_path()));
    }

    #[test]
    fn api_rejects_post_blob_uploads() {
        assert!(!oci_route_served_by_api(
            &Method::POST,
            &blob_uploads_path()
        ));
    }

    #[test]
    fn api_rejects_patch_blob_upload_session() {
        assert!(!oci_route_served_by_api(
            &Method::PATCH,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn api_rejects_put_blob_upload_session() {
        assert!(!oci_route_served_by_api(
            &Method::PUT,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn api_rejects_get_blob_upload_session() {
        assert!(!oci_route_served_by_api(
            &Method::GET,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn api_rejects_delete_blob_upload_session() {
        assert!(!oci_route_served_by_api(
            &Method::DELETE,
            &blob_upload_session_path()
        ));
    }

    // ── oci_route_served_by_transfer ───────────────────────────────────

    #[test]
    fn transfer_serves_get_blob() {
        assert!(oci_route_served_by_transfer(&Method::GET, &blob_path()));
    }

    #[test]
    fn transfer_serves_head_blob() {
        assert!(oci_route_served_by_transfer(&Method::HEAD, &blob_path()));
    }

    #[test]
    fn transfer_serves_post_blob_uploads() {
        assert!(oci_route_served_by_transfer(
            &Method::POST,
            &blob_uploads_path()
        ));
    }

    #[test]
    fn transfer_serves_patch_blob_upload_session() {
        assert!(oci_route_served_by_transfer(
            &Method::PATCH,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn transfer_serves_put_blob_upload_session() {
        assert!(oci_route_served_by_transfer(
            &Method::PUT,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn transfer_serves_get_blob_upload_session() {
        assert!(oci_route_served_by_transfer(
            &Method::GET,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn transfer_serves_delete_blob_upload_session() {
        assert!(oci_route_served_by_transfer(
            &Method::DELETE,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn transfer_rejects_get_manifest() {
        assert!(!oci_route_served_by_transfer(
            &Method::GET,
            &manifest_path()
        ));
    }

    #[test]
    fn transfer_rejects_head_manifest() {
        assert!(!oci_route_served_by_transfer(
            &Method::HEAD,
            &manifest_path()
        ));
    }

    #[test]
    fn transfer_rejects_put_manifest() {
        assert!(!oci_route_served_by_transfer(
            &Method::PUT,
            &manifest_path()
        ));
    }

    #[test]
    fn transfer_rejects_delete_manifest() {
        assert!(!oci_route_served_by_transfer(
            &Method::DELETE,
            &manifest_path()
        ));
    }

    #[test]
    fn transfer_rejects_get_tags_list() {
        assert!(!oci_route_served_by_transfer(
            &Method::GET,
            &tags_list_path()
        ));
    }

    #[test]
    fn transfer_rejects_delete_blob() {
        assert!(!oci_route_served_by_transfer(&Method::DELETE, &blob_path()));
    }

    // ── api ∩ transfer = ∅ for all method+path combinations ───────────

    #[test]
    fn api_and_transfer_are_complementary() {
        let methods = [
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ];
        let paths = [
            blob_path(),
            manifest_path(),
            blob_uploads_path(),
            blob_upload_session_path(),
            tags_list_path(),
        ];
        for method in &methods {
            for path in &paths {
                let served_by_api = oci_route_served_by_api(method, path);
                let served_by_transfer = oci_route_served_by_transfer(method, path);
                assert!(
                    !(served_by_api && served_by_transfer),
                    "method={method} path={path:?} served by BOTH api and transfer"
                );
            }
        }
    }
}
