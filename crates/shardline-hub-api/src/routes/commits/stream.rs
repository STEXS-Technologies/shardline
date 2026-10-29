//! Bounded NDJSON transport, verified content preparation and scoped LFS copies.

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use axum::body::Body;
use futures_util::StreamExt;
use shardline_index::hub::HubFileEntry;
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

use crate::{
    commit::{CommitInstruction, ParsedCommit, parse_ndjson_commit_stream},
    error::HubApiError,
    routes::HubState,
};

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

#[derive(Clone)]
pub(super) enum PreparedInstruction {
    File(HubFileEntry),
    Delete(String),
}

struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) fn prepare(
    state: &HubState,
    auth: &AuthorizedRepository,
    instruction: &CommitInstruction,
) -> Result<PreparedInstruction, HubApiError> {
    Ok(match instruction {
        CommitInstruction::InlineFile { path, content } => {
            let hash = blake3::hash(content);
            let sha = hash.to_hex().to_string();
            let size = content.len() as u64;
            let key = super::lfs_object_key(&sha, auth)?;
            let integrity = ObjectIntegrity::new(
                shardline_protocol::ShardlineHash::from_bytes(*hash.as_bytes()),
                size,
            );
            state
                .object_store
                .put_if_absent(&key, ObjectBody::from_slice(content), &integrity)
                .map_err(|error| {
                    crate::routes::object_io::storage_failure("Hub inline content write", &error)
                })?;
            crate::object_locations::remember(
                state,
                auth,
                shardline_index::hub::HubObjectKind::File,
                &sha,
            )?;
            PreparedInstruction::File(HubFileEntry {
                path: path.clone(),
                size,
                sha,
                is_lfs: false,
            })
        }
        CommitInstruction::LfsPointer { path, oid, size } => {
            crate::commit::validate_lfs_oid(oid)?;
            PreparedInstruction::File(HubFileEntry {
                path: path.clone(),
                size: *size,
                sha: oid.clone(),
                is_lfs: true,
            })
        }
        CommitInstruction::Delete { path } => PreparedInstruction::Delete(path.clone()),
    })
}

pub(super) async fn parse(
    body: Body,
    state: HubState,
    auth: AuthorizedRepository,
) -> Result<(ParsedCommit, Vec<PreparedInstruction>), HubApiError> {
    let permit = crate::git::smart_http::IDENTITY_ADMISSION
        .acquire()
        .await
        .map_err(failure)?;
    // One queued line, one active line and one producer line bound payload
    // memory independently of total commit size. Admission covers both reading
    // the body and the blocking worker, even if the request is cancelled.
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelOnDrop(cancelled.clone());
    let mut worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut sizes = HashMap::new();
        let mut instructions = Vec::new();
        let parsed = parse_ndjson_commit_stream(
            std::iter::from_fn(|| receiver.blocking_recv()),
            |oid| {
                if let Some(size) = sizes.get(oid) {
                    return Ok(*size);
                }
                let size = stored_size(&state, &auth, oid)?;
                sizes.insert(oid.to_owned(), size);
                Ok(size)
            },
            |instruction| {
                if cancelled.load(Ordering::Acquire) {
                    return Err(failure("commit request cancelled"));
                }
                instructions.push(prepare(&state, &auth, &instruction)?);
                Ok(())
            },
        )?;
        Ok((parsed, instructions))
    });
    let producer = async move {
        if let Err(error) = read_lines(body, &sender).await {
            drop(sender.send(Err(error)).await);
        }
        drop(sender);
    };
    tokio::pin!(producer);
    let result = tokio::select! {
        result = &mut worker => result,
        () = &mut producer => worker.await,
    };
    result.map_err(failure)?.map_err(|error| {
        if let HubApiError::Json(ref source) = error {
            HubApiError::BadRequest(format!("invalid commit JSON: {source}"))
        } else {
            error
        }
    })
}

async fn read_lines(
    body: Body,
    sender: &tokio::sync::mpsc::Sender<Result<String, HubApiError>>,
) -> Result<(), HubApiError> {
    const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;
    const MAX_REQUEST_BYTES: usize = 1024 * 1024 * 1024;
    let mut stream = body.into_data_stream();
    let mut line = Vec::new();
    let mut total = 0_usize;
    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(|error| {
            HubApiError::PathValidation(format!("commit body read failed: {error}"))
        })?;
        total = total
            .checked_add(frame.len())
            .ok_or_else(|| failure("commit length overflow"))?;
        if total > MAX_REQUEST_BYTES {
            return Err(HubApiError::PathValidation(
                "commit exceeds 1 GiB request limit".to_owned(),
            ));
        }
        for part in frame.split_inclusive(|byte| *byte == b'\n') {
            if line.len().saturating_add(part.len()) > MAX_LINE_BYTES {
                return Err(HubApiError::PathValidation(
                    "commit line exceeds 16 MiB limit; use LFS for large files".to_owned(),
                ));
            }
            line.extend_from_slice(part);
            if part.last() == Some(&b'\n') {
                send_line(sender, std::mem::take(&mut line)).await?;
            }
        }
    }
    if !line.is_empty() {
        send_line(sender, line).await?;
    }
    Ok(())
}

async fn send_line(
    sender: &tokio::sync::mpsc::Sender<Result<String, HubApiError>>,
    line: Vec<u8>,
) -> Result<(), HubApiError> {
    let line = String::from_utf8(line).map_err(|error| {
        HubApiError::PathValidation(format!("commit body is not UTF-8: {error}"))
    })?;
    sender
        .send(Ok(line))
        .await
        .map_err(|_closed| failure("commit parser stopped"))
}

fn stored_size(
    state: &HubState,
    auth: &AuthorizedRepository,
    oid: &str,
) -> Result<Option<u64>, HubApiError> {
    if let Some(scope) = auth.repository()
        && let Some(identity) = state
            .store
            .lookup_xet_file_identity(scope, oid)
            .map_err(failure)?
    {
        if identity.sha256 != oid
            || identity.repository_key()
                != shardline_index::XetFileIdentity::repository_key_for_scope(scope)
            || hex::decode(&identity.file_id).map_or(true, |bytes| bytes.len() != 32)
        {
            return Err(failure("Hub Xet identity does not match LFS copy"));
        }
        return Ok(Some(identity.total_bytes));
    }
    let key = crate::object_locations::file_key(state, oid, auth)?;
    Ok(state
        .object_store
        .metadata(&key)
        .map_err(|error| {
            crate::routes::object_io::storage_failure("Hub LFS metadata read", &error)
        })?
        .map(|metadata| metadata.length()))
}
