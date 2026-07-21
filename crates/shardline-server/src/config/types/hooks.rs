use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};

#[cfg(test)]
pub(crate) type SecretFileReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
pub(crate) struct SecretFileReadHookRegistration {
    pub(crate) path: PathBuf,
    pub(crate) hook: SecretFileReadHook,
}

#[cfg(test)]
pub(crate) type SecretFileReadHookSlot = Vec<SecretFileReadHookRegistration>;

#[cfg(test)]
pub(crate) static BEFORE_SECRET_FILE_READ_HOOK: LazyLock<Mutex<SecretFileReadHookSlot>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

#[cfg(test)]
pub fn run_before_secret_file_read_hook_for_tests(path: &Path) {
    let hook = match BEFORE_SECRET_FILE_READ_HOOK.lock() {
        Ok(mut guard) => take_secret_file_read_hook_for_path(&mut guard, path),
        Err(poisoned) => take_secret_file_read_hook_for_path(&mut poisoned.into_inner(), path),
    };

    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
pub(crate) fn take_secret_file_read_hook_for_path(
    slot: &mut SecretFileReadHookSlot,
    path: &Path,
) -> Option<SecretFileReadHook> {
    let index = slot
        .iter()
        .position(|registration| registration.path == path)?;
    Some(slot.remove(index).hook)
}

#[cfg(not(test))]
pub const fn run_before_secret_file_read_hook_for_tests(_path: &Path) {}
