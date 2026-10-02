//! Command handlers for the `sdx` file-management CLI lane.

use std::io::Read;
use std::path::{Path, PathBuf};

use sdx::client::XetClientBuilder;
use sdx::{INGESTION_BLOCK_SIZE, SdxConfig, XetClient, XetUrl};

use super::cli::{
    BranchArgs, CatArgs, CpArgs, GlobalArgs, InfoArgs, LsArgs, RmArgs, SyncArgs, TransferFlags,
};
use super::error::XetError;
use super::resolve::{
    Session, resolve_auth, resolve_remote, session_for, session_for_repo, session_for_revision,
};

/// Memory bound for streaming downloads (cat) and downloads.
const DOWNLOAD_BUFFER_CAP: u64 = 256 * 1024 * 1024;

/// Classifies an operand as remote or local.
pub(crate) fn is_remote_operand(input: &str, config: Option<&SdxConfig>) -> bool {
    if input.starts_with("xet://") {
        return true;
    }
    // Shorthand form: requires a config-provided endpoint, a non-existent
    // local path, and at least `owner/repo/revision` segments.
    config.and_then(|config| config.endpoint()).is_some()
        && !Path::new(input).exists()
        && input.trim_matches('/').split('/').count() >= 3
}

/// `sdx cp <src> <dst>`.
///
/// # Errors
///
/// Returns an error when the operand shape is unsupported or a transfer fails.
pub(crate) async fn cp(
    global: &GlobalArgs,
    args: &CpArgs,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let src_remote = is_remote_operand(&args.src, config);
    let dst_remote = is_remote_operand(&args.dst, config);
    match (src_remote, dst_remote) {
        (true, true) => Err(XetError::RemoteToRemote),
        (false, false) => Err(XetError::LocalToLocal),
        (true, false) => {
            download_cp(
                global,
                &args.src,
                &args.dst,
                args.transfer.recursive,
                config,
            )
            .await
        }
        (false, true) => upload_cp(global, &args.src, &args.dst, &args.transfer, config).await,
    }
}

/// `sdx sync <src> <dst>` — push-only directory synchronization.
///
/// # Errors
///
/// Returns an error for non-push shapes or a transfer failure.
pub(crate) async fn sync(
    global: &GlobalArgs,
    args: &SyncArgs,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let src_remote = is_remote_operand(&args.src, config);
    let dst_remote = is_remote_operand(&args.dst, config);
    match (src_remote, dst_remote) {
        (false, true) => sync_push(global, &args.src, &args.dst, &args.transfer, config).await,
        (true, false) => Err(XetError::Message(
            "pull sync is not supported; sync is push-only (local -> remote)".to_owned(),
        )),
        _ => Err(XetError::Message(
            "sync requires a local source directory and a xet:// remote destination".to_owned(),
        )),
    }
}

/// `sdx ls <url>`.
///
/// # Errors
///
/// Returns an error when listing fails.
pub(crate) async fn ls(
    global: &GlobalArgs,
    args: &LsArgs,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let session = session_for(&args.url, global, config, None)?;
    if args.branches {
        let revisions = session.client.list_revisions().await?;
        for revision in revisions {
            println!("{}", revision.name);
        }
        return Ok(());
    }
    let entries = session.client.list_dir_all(&session.url.path).await?;
    for entry in entries {
        if args.long {
            let size = entry
                .size
                .map_or_else(|| "-".to_owned(), |size| size.to_string());
            println!("{size:>12} {}", entry.path);
        } else {
            println!("{}", entry.path);
        }
    }
    Ok(())
}

/// `sdx rm <url>`.
///
/// # Errors
///
/// Returns an error when deregistration fails.
pub(crate) async fn rm(
    global: &GlobalArgs,
    args: &RmArgs,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let session = session_for(&args.url, global, config, None)?;
    let deleted = session
        .client
        .delete_path(&session.url.path, args.recursive)
        .await?;
    println!("deleted {deleted} path(s)");
    Ok(())
}

/// `sdx cat <url>` — stream a remote file to standard output.
///
/// # Errors
///
/// Returns an error when the file cannot be resolved or streamed.
pub(crate) async fn cat(
    global: &GlobalArgs,
    args: &CatArgs,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let url = resolve_remote(&args.url, config)?;
    let auth = resolve_auth(global, config, &url)?;
    let client = build_download_client(&url, auth)?;
    let entry = client.resolve_path(&url.path).await?;
    let stdout = std::io::stdout();
    client.download_to_writer(&entry.file_id, stdout).await?;
    Ok(())
}

/// `sdx info <url>`.
///
/// # Errors
///
/// Returns an error when metadata resolution fails.
pub(crate) async fn info(
    global: &GlobalArgs,
    args: &InfoArgs,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let session = session_for(&args.url, global, config, None)?;
    let url = &session.url;
    if url.path.is_empty() || url.path.ends_with('/') {
        let entries = session.client.list_dir_all(&url.path).await?;
        println!(
            "path: {}",
            if url.path.is_empty() {
                "/"
            } else {
                url.path.as_str()
            }
        );
        let files = entries.iter().filter(|entry| !entry.is_dir).count();
        let total = entries
            .iter()
            .filter(|entry| !entry.is_dir)
            .fold(0_u64, |acc, entry| {
                acc.saturating_add(entry.size.unwrap_or(0))
            });
        println!("files: {files}");
        println!("total_bytes: {total}");
    } else {
        let entry = session.client.resolve_path(&url.path).await?;
        println!("path: {}", entry.path);
        println!("file_id: {}", entry.file_id);
        println!("size: {}", entry.size);
        println!("updated_at: {}", entry.updated_at);
    }
    Ok(())
}

/// `sdx branch <url> [--create NAME | --delete NAME]`.
///
/// # Errors
///
/// Returns an error when a revision operation fails.
pub(crate) async fn branch(
    global: &GlobalArgs,
    args: &BranchArgs,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let session = session_for_repo(&args.url, global, config)?;
    if let Some(name) = &args.create {
        // The server enforces a strict scope match, so scope the token to the
        // revision being created.
        let scoped = session_for_revision(&args.url, name, global, config)?;
        let revision = scoped.client.create_revision(name).await?;
        println!("created revision {}", revision.name);
        return Ok(());
    }
    if let Some(name) = &args.delete {
        let scoped = session_for_revision(&args.url, name, global, config)?;
        scoped.client.delete_revision(name).await?;
        println!("deleted revision {name}");
        return Ok(());
    }
    let revisions = session.client.list_revisions().await?;
    for revision in revisions {
        println!("{}", revision.name);
    }
    Ok(())
}

/// Uploads a local file (or directory tree) to a remote.
async fn upload_cp(
    global: &GlobalArgs,
    src: &str,
    dst: &str,
    transfer: &TransferFlags,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let path = Path::new(src);
    if !path.exists() {
        return Err(XetError::message(format!("source does not exist: {src}")));
    }
    let session = session_for(dst, global, config, transfer.chunk_size)?;
    if path.is_dir() {
        if !transfer.recursive {
            return Err(XetError::DirectoryRequiresRecursive);
        }
        upload_dir(&session, path, transfer).await
    } else {
        let remote = derive_upload_path(&session.url, path)?;
        upload_one(&session.client, path, &remote, transfer).await
    }
}

/// Recursively uploads a local directory tree, preserving relative structure.
async fn upload_dir(
    session: &Session,
    root: &Path,
    transfer: &TransferFlags,
) -> Result<(), XetError> {
    let base = session.url.path.trim_matches('/').to_owned();
    let mut files = Vec::new();
    collect_files(root, &mut files)?;
    for file in files {
        let rel = file.strip_prefix(root).map_err(|error| {
            XetError::message(format!("failed to compute relative path: {error}"))
        })?;
        let rel_str = rel.to_string_lossy();
        let remote = if base.is_empty() {
            rel_str.into_owned()
        } else {
            format!("{base}/{rel_str}")
        };
        upload_one(&session.client, &file, &remote, transfer).await?;
    }
    Ok(())
}

/// Derives the remote path for a single-file upload.
fn derive_upload_path(url: &XetUrl, file: &Path) -> Result<String, XetError> {
    let base = url.path.trim_matches('/');
    if base.is_empty() || url.path.ends_with('/') {
        let name = file.file_name().ok_or_else(|| {
            XetError::message(format!("source has no file name: {}", file.display()))
        })?;
        let name = name.to_string_lossy();
        if base.is_empty() {
            Ok(name.into_owned())
        } else {
            Ok(format!("{base}/{name}"))
        }
    } else {
        Ok(base.to_owned())
    }
}

/// Uploads a single file (registering metadata unless `--no-register`).
async fn upload_one(
    client: &XetClient,
    path: &Path,
    remote: &str,
    transfer: &TransferFlags,
) -> Result<(), XetError> {
    if transfer.no_register {
        let info = upload_no_register(client, path).await?;
        println!(
            "{} -> {} ({} bytes, {} chunks) [not registered]",
            path.display(),
            remote,
            info.total_bytes,
            info.chunk_count
        );
    } else {
        let info = client.upload_file(path, remote).await?;
        println!(
            "{} -> {} ({} bytes, {} chunks)",
            path.display(),
            remote,
            info.total_bytes,
            info.chunk_count
        );
    }
    Ok(())
}

/// Uploads a file's chunks into the session shard without registering its
/// path metadata. The shard is still uploaded so the `file_id` is fetchable.
async fn upload_no_register(
    client: &XetClient,
    path: &Path,
) -> Result<sdx::UploadFileInfo, XetError> {
    let session = client.upload_session()?;
    let handle = session.upload_stream_handle();
    let mut file = std::fs::File::open(path)?;
    let reader = &mut file;
    let mut buf = Vec::with_capacity(INGESTION_BLOCK_SIZE);
    loop {
        buf.resize(INGESTION_BLOCK_SIZE, 0);
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        buf.truncate(n);
        handle.write(std::mem::take(&mut buf)).await?;
    }
    let info = handle.finish().await?;
    session.finalize().await?;
    Ok(info)
}

/// Downloads a remote file (or directory tree) to a local path.
async fn download_cp(
    global: &GlobalArgs,
    src: &str,
    dst: &str,
    recursive: bool,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let url = resolve_remote(src, config)?;
    let auth = resolve_auth(global, config, &url)?;
    let client = build_download_client(&url, auth)?;
    if url.path.is_empty() || url.path.ends_with('/') {
        if !recursive {
            return Err(XetError::DirectoryRequiresRecursive);
        }
        download_dir(&client, &url, dst).await
    } else {
        let entry = client.resolve_path(&url.path).await?;
        let local = if Path::new(dst).is_dir() {
            Path::new(dst).join(basename(&url.path))
        } else {
            PathBuf::from(dst)
        };
        download_one(&client, &entry.file_id, &local).await
    }
}

/// Recursively downloads a remote directory tree.
async fn download_dir(client: &XetClient, url: &XetUrl, dst: &str) -> Result<(), XetError> {
    let base = url.path.trim_matches('/');
    let entries = client.list_dir_all(&url.path).await?;
    let mut downloads = Vec::new();
    // Validate the complete remote listing before publishing any files.
    for entry in entries {
        if entry.is_dir {
            continue;
        }
        let file_id = entry
            .file_id
            .as_deref()
            .ok_or_else(|| XetError::message(format!("missing file_id for {}", entry.path)))?;
        let rel = relative_download_path(base, &entry.path)?;
        let local = Path::new(dst).join(rel);
        downloads.push((file_id.to_owned(), local));
    }
    for (file_id, local) in downloads {
        download_one(client, &file_id, &local).await?;
    }
    Ok(())
}

fn relative_download_path(base: &str, remote: &str) -> Result<PathBuf, XetError> {
    let invalid = || XetError::message(format!("unsafe remote download path: {remote:?}"));
    if remote.contains(['\\', '\0'])
        || remote.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || (part.len() == 2 && part.ends_with(':'))
        })
    {
        return Err(invalid());
    }
    let relative = if base.is_empty() {
        remote
    } else {
        remote
            .strip_prefix(base)
            .and_then(|rest| rest.strip_prefix('/'))
            .ok_or_else(invalid)?
    };
    if relative.is_empty() {
        return Err(invalid());
    }
    Ok(PathBuf::from(relative))
}

/// Downloads one remote file to a local path.
async fn download_one(client: &XetClient, file_id: &str, local: &Path) -> Result<(), XetError> {
    #[cfg(unix)]
    let output = crate::local_output::AtomicOutputFile::create(local, true)?;
    #[cfg(unix)]
    let file = output.try_clone_file()?;
    #[cfg(not(unix))]
    let output = {
        if let Ok(metadata) = std::fs::symlink_metadata(local)
            && metadata.file_type().is_symlink()
        {
            return Err(XetError::message(
                "download destination must not be a symlink".to_owned(),
            ));
        }
        if let Some(parent) = local.parent().filter(|path| !path.as_os_str().is_empty()) {
            crate::local_output::ensure_output_directory(parent)?;
        }
        tempfile::NamedTempFile::new_in(
            local
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?
    };
    #[cfg(not(unix))]
    let file = output.as_file().try_clone()?;
    let n = client.download_to_writer(file_id, file).await?;
    #[cfg(unix)]
    tokio::task::spawn_blocking(move || output.commit())
        .await
        .map_err(|error| XetError::message(error.to_string()))??;
    #[cfg(not(unix))]
    {
        output.as_file().sync_all()?;
        output
            .persist(local)
            .map_err(|error| XetError::Io(error.error))?;
    }
    println!("{} <- {} ({} bytes)", local.display(), file_id, n);
    Ok(())
}

/// Push-only sync: uploads changed files from a local directory to a remote.
async fn sync_push(
    global: &GlobalArgs,
    src: &str,
    dst: &str,
    transfer: &TransferFlags,
    config: Option<&SdxConfig>,
) -> Result<(), XetError> {
    let src_path = Path::new(src);
    if !src_path.is_dir() {
        return Err(XetError::message(format!(
            "sync source is not a directory: {src}"
        )));
    }
    let session = session_for(dst, global, config, transfer.chunk_size)?;
    let base = session.url.path.trim_matches('/').to_owned();
    let mut files = Vec::new();
    collect_files(src_path, &mut files)?;
    for file in files {
        let rel = file.strip_prefix(src_path).map_err(|error| {
            XetError::message(format!("failed to compute relative path: {error}"))
        })?;
        let rel_str = rel.to_string_lossy();
        let remote = if base.is_empty() {
            rel_str.into_owned()
        } else {
            format!("{base}/{rel_str}")
        };
        // Size equality cannot establish content equality. Let the upload
        // pipeline hash the bytes and deduplicate unchanged CAS objects.
        upload_one(&session.client, &file, &remote, transfer).await?;
    }
    Ok(())
}

/// Recursively collects regular files under `dir` into `out`.
fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), XetError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err(XetError::message(format!(
                "refusing symlink in upload tree: {}",
                path.display()
            )));
        }
        if kind.is_dir() {
            collect_files(&path, out)?;
        } else if kind.is_file() {
            out.push(path);
        } else {
            return Err(XetError::message(format!(
                "upload tree contains a non-regular file: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

/// Returns the file-name portion of a slash-delimited remote path.
fn basename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or_default().to_owned()
}

/// Builds a download-focused client with a bounded buffer semaphore.
fn build_download_client(url: &XetUrl, auth: sdx::Auth) -> Result<XetClient, XetError> {
    XetClientBuilder::new()
        .from_url(url)
        .auth(auth)
        .with_buffer_semaphore(DOWNLOAD_BUFFER_CAP)
        .build()
        .map_err(XetError::Sdx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_paths_stay_in_selected_remote_directory() {
        assert_eq!(
            relative_download_path("models", "models/nested/file").unwrap(),
            PathBuf::from("nested/file")
        );
        for (base, path) in [
            ("", "../valuable"),
            ("", "/absolute"),
            ("", "C:/valuable"),
            ("", "nested\\valuable"),
            ("", "nested/./file"),
            ("", "nested//file"),
            ("models", "models2/file"),
            ("models", "other/file"),
        ] {
            assert!(
                relative_download_path(base, path).is_err(),
                "accepted {path}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn upload_tree_rejects_symlink_cycle() {
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(dir.path(), dir.path().join("loop")).unwrap();
        assert!(collect_files(dir.path(), &mut Vec::new()).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelled_download_preserves_destination_and_removes_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        std::fs::write(&dest, b"original").unwrap();
        // Keep a listener alive without serving requests: token issuance stays
        // pending after the temporary writer is created.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url =
            XetUrl::parse(&format!("xet://127.0.0.1:{port}/github/team/assets/main")).unwrap();
        let auth = sdx::Auth::new(&format!("http://127.0.0.1:{port}"), url.repository_id())
            .unwrap()
            .with_api_key("test-bootstrap-key".to_owned())
            .with_subject("test-subject".to_owned());
        let client = build_download_client(&url, auth).unwrap();
        let task_dest = dest.clone();
        let task =
            tokio::spawn(async move { download_one(&client, &"0".repeat(64), &task_dest).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while std::fs::read_dir(dir.path()).unwrap().count() == 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(std::fs::read(&dest).unwrap(), b"original");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn recursive_download_cannot_escape_destination_with_server_path() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("downloads");
        let valuable = dir.path().join("valuable");
        std::fs::create_dir(&dest).unwrap();
        std::fs::write(&valuable, b"original outside directory").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let base = format!("http://127.0.0.1:{port}");
        let response_base = base.clone();
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
            let response_base = response_base.clone();
            async move {
                if uri.path().contains("xet-read-token") {
                    let exp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 3600;
                    axum::Json(serde_json::json!({"casUrl": response_base, "exp": exp, "accessToken": "test"}))
                } else {
                    axum::Json(serde_json::json!({"entries": [{"path": "../valuable", "isDir": false, "fileId": "invalid"}], "nextCursor": null}))
                }
            }
        });
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let url =
            XetUrl::parse(&format!("xet://127.0.0.1:{port}/github/team/assets/main/")).unwrap();
        let auth = sdx::Auth::new(&base, url.repository_id())
            .unwrap()
            .with_api_key("test-bootstrap-key".to_owned())
            .with_subject("test-subject".to_owned());
        let client = build_download_client(&url, auth).unwrap();
        assert!(
            download_dir(&client, &url, dest.to_str().unwrap())
                .await
                .is_err()
        );
        server.abort();
        assert_eq!(
            std::fs::read(&valuable).unwrap(),
            b"original outside directory"
        );
    }

    #[tokio::test]
    async fn failed_download_preserves_existing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        std::fs::write(&dest, b"valuable original").unwrap();
        let url = XetUrl::parse("xet://127.0.0.1:9/github/team/assets/main").unwrap();
        let auth = sdx::Auth::new("http://127.0.0.1:9", url.repository_id()).unwrap();
        let client = build_download_client(&url, auth).unwrap();
        assert!(
            download_one(&client, "invalid-file-id", &dest)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"valuable original");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
