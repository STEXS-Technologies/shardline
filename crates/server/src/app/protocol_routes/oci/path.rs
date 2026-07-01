use crate::{ServerError, oci_adapter::validate_repository, protocol_support::parse_sha256_digest};

pub(crate) enum OciPath {
    Blob {
        repository: String,
        digest_hex: String,
    },
    BlobUploads {
        repository: String,
    },
    BlobUploadSession {
        repository: String,
        session_id: String,
    },
    Manifest {
        repository: String,
        reference: String,
    },
    TagsList {
        repository: String,
    },
}

pub(crate) fn parse_oci_path(path: &str) -> Result<OciPath, ServerError> {
    let path = path.trim_end_matches('/');
    if let Some(repository) = path.strip_suffix("/blobs/uploads") {
        validate_repository(repository)?;
        return Ok(OciPath::BlobUploads {
            repository: repository.to_owned(),
        });
    }
    if let Some((repository, session_id)) = path.split_once("/blobs/uploads/") {
        validate_repository(repository)?;
        return Ok(OciPath::BlobUploadSession {
            repository: repository.to_owned(),
            session_id: session_id.to_owned(),
        });
    }
    if let Some((repository, digest)) = path.split_once("/blobs/") {
        validate_repository(repository)?;
        return Ok(OciPath::Blob {
            repository: repository.to_owned(),
            digest_hex: parse_sha256_digest(digest)?,
        });
    }
    if let Some((repository, reference)) = path.split_once("/manifests/") {
        validate_repository(repository)?;
        return Ok(OciPath::Manifest {
            repository: repository.to_owned(),
            reference: reference.to_owned(),
        });
    }
    if let Some(repository) = path.strip_suffix("/tags/list") {
        validate_repository(repository)?;
        return Ok(OciPath::TagsList {
            repository: repository.to_owned(),
        });
    }

    Err(ServerError::NotFound)
}
