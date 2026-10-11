use super::{Error, Result};
use ed25519_dalek::{SigningKey, VerifyingKey};
use penelope_core::task::{Digest, TaskGeneration, TaskScope};
use shardline_reliability::{
    MaintenanceArtifact, MaintenanceArtifactKind, MaintenanceArtifactProof,
    sign_maintenance_artifact, verify_maintenance_artifact,
};
use statechronicle_domain::intent::KeyId;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub(super) fn private_directory(path: &Path) -> Result<()> {
    crate::local_path::ensure_directory_path_components_are_not_symlinked(path)?;
    if !path.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err("maintenance state directory must be owned by this user and mode 0700".into());
    }
    Ok(())
}

pub(super) fn open_private(path: &Path, max: u64) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > max
    {
        return Err("invalid private maintenance file".into());
    }
    Ok(file)
}

pub(super) fn read_private(path: &Path, max: u64) -> Result<Vec<u8>> {
    let file = open_private(path, max)?;
    let mut bytes = Vec::new();
    file.take(max.saturating_add(1)).read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len())? > max {
        return Err("maintenance file exceeds quota".into());
    }
    Ok(bytes)
}

pub(super) fn signer(directory: &Path, create: bool) -> Result<SigningKey> {
    let path = directory.join("signing-key");
    if create && !path.exists() {
        let mut seed = zeroize::Zeroizing::new([0_u8; 32]);
        getrandom::fill(seed.as_mut()).map_err(|error| -> Error { error.to_string().into() })?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)?;
        file.write_all(seed.as_ref())?;
        file.sync_all()?;
        File::open(directory)?.sync_all()?;
    }
    let bytes = zeroize::Zeroizing::new(read_private(&path, 32)?);
    let seed: &[u8; 32] = bytes.as_slice().try_into()?;
    Ok(SigningKey::from_bytes(seed))
}

pub(super) fn key_id(key: &VerifyingKey) -> KeyId {
    KeyId(format!(
        "shardline-maintenance:{}",
        hex::encode(key.as_bytes())
    ))
}

fn artifact_path(directory: &Path, digest: Digest) -> PathBuf {
    directory.join("artifacts").join(hex::encode(digest.0))
}

pub(super) fn publish(
    directory: &Path,
    scope: TaskScope,
    generation: TaskGeneration,
    kind: MaintenanceArtifactKind,
    step: u32,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    let artifacts = directory.join("artifacts");
    private_directory(&artifacts)?;
    let digest = Digest::of(bytes);
    let path = artifact_path(directory, digest);
    if path.exists() {
        let artifact = MaintenanceArtifact {
            schema_version: 1,
            scope,
            generation,
            step,
            kind,
            digest,
            bytes: u64::try_from(bytes.len())?,
        };
        artifact.verify_materialized(open_private(&path, 64 * 1024 * 1024)?)?;
    } else {
        // Atomic publication; content identity makes exact re-publication harmless.
        crate::write_output_bytes(&path, bytes, false)?;
    }
    let key = signer(directory, false)?;
    Ok(sign_maintenance_artifact(
        MaintenanceArtifact {
            schema_version: 1,
            scope,
            generation,
            step,
            kind,
            digest,
            bytes: u64::try_from(bytes.len())?,
        },
        chrono::Utc::now(),
        &key,
        key_id(&key.verifying_key()),
    )?
    .to_json()?)
}

pub(super) fn verify(
    directory: &Path,
    bytes: &[u8],
    scope: TaskScope,
    generation: TaskGeneration,
    kind: MaintenanceArtifactKind,
    key: &VerifyingKey,
) -> Result<MaintenanceArtifactProof> {
    let proof = MaintenanceArtifactProof::from_json(bytes)?;
    let artifact = verify_maintenance_artifact(&proof, scope, generation, key, &key_id(key))?;
    if artifact.kind != kind {
        return Err("maintenance artifact role does not match task".into());
    }
    let file = open_private(&artifact_path(directory, artifact.digest), 64 * 1024 * 1024)?;
    artifact.verify_materialized(file)?;
    Ok(proof)
}

pub(super) fn publish_file(
    directory: &Path,
    scope: TaskScope,
    generation: TaskGeneration,
    kind: MaintenanceArtifactKind,
    step: u32,
    file: &File,
) -> Result<Vec<u8>> {
    use sha2::{Digest as _, Sha256};
    use std::io::{Seek, SeekFrom};
    file.sync_all()?;
    let mut reader = file.try_clone()?;
    reader.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 16384];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(buffer.get(..count).ok_or("invalid report read")?);
    }
    let artifact = MaintenanceArtifact {
        schema_version: 1,
        scope,
        generation,
        step,
        kind,
        digest: Digest(hash.finalize().into()),
        bytes: file.metadata()?.len(),
    };
    let artifacts = directory.join("artifacts");
    private_directory(&artifacts)?;
    let destination = artifact_path(directory, artifact.digest);
    if !destination.exists() {
        let mut temporary = tempfile::NamedTempFile::new_in(&artifacts)?;
        reader.seek(SeekFrom::Start(0))?;
        std::io::copy(&mut reader, temporary.as_file_mut())?;
        temporary.as_file().sync_all()?;
        temporary.persist_noclobber(&destination)?;
        File::open(&artifacts)?.sync_all()?;
    }
    artifact.verify_materialized(open_private(&destination, 64 * 1024 * 1024)?)?;
    let key = signer(directory, false)?;
    Ok(sign_maintenance_artifact(
        artifact,
        chrono::Utc::now(),
        &key,
        key_id(&key.verifying_key()),
    )?
    .to_json()?)
}

pub(super) fn success(directory: &Path, digest: Digest) -> Result<bool> {
    use std::io::{BufRead, BufReader};
    let file = open_private(&artifact_path(directory, digest), 64 * 1024 * 1024)?;
    let mut header = String::new();
    BufReader::new(file.take(64)).read_line(&mut header)?;
    match header.as_str() {
        "maintenance.success: true\n" => Ok(true),
        "maintenance.success: false\n" => Ok(false),
        _ => Err("maintenance report has an invalid outcome header".into()),
    }
}

/// Bound disk growth while streaming reports; proof blobs remain at most 64 KiB.
pub(super) struct ReportWriter<'file> {
    file: &'file mut File,
    remaining: usize,
}
impl<'file> ReportWriter<'file> {
    pub const fn new(file: &'file mut File) -> Self {
        Self {
            file,
            remaining: 64 * 1024 * 1024,
        }
    }
}
impl Write for ReportWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other(
                "maintenance report exceeds 64 MiB quota",
            ));
        }
        let count = self.file.write(bytes)?;
        self.remaining = self.remaining.saturating_sub(count);
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
