//! Bounded, fair scanning of retained evidence. The cursor never authorizes work.
use super::{Result, artifact};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions, TryLockError},
    io::ErrorKind,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

const BATCH_SIZE: usize = 64;
const CURSOR: &str = ".export-cursor";

pub(super) struct Batch {
    // Lock the existing directory inode, without an extra persistent lock file.
    _lease: File,
    pub jobs: Vec<PathBuf>,
    cursor: PathBuf,
    total: usize,
}

impl Batch {
    pub fn open(state: &Path) -> Result<Option<Self>> {
        artifact::private_directory(state)?;
        let lease = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(state)?;
        match lease.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                tracing::debug!(
                    "another invocation owns evidence export; existing command continues"
                );
                return Ok(None);
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        let cursor = state.join(CURSOR);
        let after = match artifact::read_private(&cursor, 64) {
            Ok(bytes) => String::from_utf8(bytes).ok().filter(|name| task_name(name)),
            Err(error) if !cursor.exists() => {
                let _missing = error;
                None
            }
            Err(error) => {
                tracing::warn!("invalid export scan cursor; restarting evidence scan: {error}");
                None
            }
        };
        let mut first = BTreeMap::new();
        let mut next = BTreeMap::new();
        let mut total = 0_usize;
        for entry in std::fs::read_dir(state)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !task_name(&name) || !entry.file_type()?.is_dir() {
                continue;
            }
            total = total.saturating_add(1);
            retain_first(&mut first, name.clone(), entry.path());
            if after.as_ref().is_none_or(|after| &name > after) {
                retain_first(&mut next, name, entry.path());
            }
        }
        // Wrap only after reaching the end. At most 64 journals are opened;
        // directory enumeration uses bounded memory regardless of backlog size.
        let selected = if next.is_empty() { first } else { next };
        Ok(Some(Self {
            _lease: lease,
            jobs: selected.into_values().collect(),
            cursor,
            total,
        }))
    }

    pub fn advance(&self, directory: &Path) -> Result<()> {
        let name = directory
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| task_name(name))
            .ok_or("invalid retained task directory")?;
        // Persist before inspecting a journal. A corrupt journal or interrupted
        // export must not permanently block the jobs that follow it.
        crate::write_output_bytes(&self.cursor, name.as_bytes(), false)?;
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        if self.total <= BATCH_SIZE {
            match std::fs::remove_file(&self.cursor) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

fn task_name(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn retain_first(entries: &mut BTreeMap<String, PathBuf>, name: String, path: PathBuf) {
    entries.insert(name, path);
    if entries.len() > BATCH_SIZE {
        entries.pop_last();
    }
}
