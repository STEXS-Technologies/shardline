#[cfg(target_os = "linux")]
use std::collections::BTreeMap;
#[cfg(target_os = "linux")]
use std::env::current_exe;
#[cfg(any(target_os = "linux", test))]
use std::fs;
#[cfg(any(target_os = "linux", test))]
use std::fs::{File, OpenOptions};
#[cfg(any(target_os = "linux", test))]
use std::io::Read;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    io,
    path::{Path, PathBuf},
};

use thiserror::Error;

#[cfg(any(target_os = "linux", test))]
use shardline_storage::local_path::resolve_platform_symlinks;

#[cfg(not(target_os = "linux"))]
use crate::local_output::remove_output_file_if_present;
#[cfg(target_os = "linux")]
use crate::local_output::{
    ensure_output_directory, remove_output_file_if_present, write_output_bytes,
};

const DEFAULT_OUTPUT_DIR: &str = "/etc/systemd/system";
const DEFAULT_UNIT_PREFIX: &str = "shardline-gc";
const DEFAULT_CALENDAR: &str = "*-*-* 03:17:00";
const DEFAULT_BINARY_PATH: &str = "/usr/local/bin/shardline";
const DEFAULT_ENV_FILE: &str = "/etc/shardline/shardline.env";
const DEFAULT_WORKING_DIRECTORY: &str = "/var/lib/shardline";
const DEFAULT_SERVICE_USER: &str = "shardline";
const DEFAULT_SERVICE_GROUP: &str = "shardline";
#[cfg(any(target_os = "linux", test))]
const MAX_SYSTEMD_ENV_FILE_BYTES: u64 = 65_536;
#[cfg(any(target_os = "linux", test))]
#[cfg(target_os = "linux")]
const MAX_SYSTEM_ACCOUNT_FILE_BYTES: u64 = 4_194_304;

#[cfg(test)]
type LocalTextFileReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct LocalTextFileReadHookRegistration {
    path: PathBuf,
    hook: LocalTextFileReadHook,
}

#[cfg(test)]
type LocalTextFileReadHookSlot = Option<LocalTextFileReadHookRegistration>;

#[cfg(test)]
static BEFORE_LOCAL_TEXT_FILE_READ_HOOK: LazyLock<Mutex<LocalTextFileReadHookSlot>> =
    LazyLock::new(|| Mutex::new(None));

/// One scheduled-GC systemd installation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcScheduleInstallOptions {
    /// Directory that receives the unit files.
    pub output_dir: PathBuf,
    /// Unit basename without the `.service` or `.timer` suffix.
    pub unit_prefix: String,
    /// `systemd.timer` calendar expression.
    pub calendar: String,
    /// Retention window passed to `shardline gc`.
    pub retention_seconds: u64,
    /// Absolute path to the `shardline` binary.
    pub binary_path: PathBuf,
    /// Environment file referenced by the generated service.
    pub env_file: PathBuf,
    /// Working directory and writable state path for the generated service.
    pub working_directory: PathBuf,
    /// Service user.
    pub user: String,
    /// Service group.
    pub group: String,
    /// When true, print generated unit files to stdout instead of writing to disk.
    pub dry_run: bool,
}

impl Default for GcScheduleInstallOptions {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from(DEFAULT_OUTPUT_DIR),
            unit_prefix: DEFAULT_UNIT_PREFIX.to_owned(),
            calendar: DEFAULT_CALENDAR.to_owned(),
            retention_seconds: 86_400,
            binary_path: PathBuf::from(DEFAULT_BINARY_PATH),
            env_file: PathBuf::from(DEFAULT_ENV_FILE),
            working_directory: PathBuf::from(DEFAULT_WORKING_DIRECTORY),
            user: DEFAULT_SERVICE_USER.to_owned(),
            group: DEFAULT_SERVICE_GROUP.to_owned(),
            dry_run: false,
        }
    }
}

/// Summary for a scheduled-GC installation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcScheduleInstallReport {
    /// Generated service unit path.
    pub service_path: PathBuf,
    /// Generated timer unit path.
    pub timer_path: PathBuf,
    /// Resolved binary path embedded in the service unit.
    pub binary_path: PathBuf,
    /// Resolved environment file referenced by the service unit.
    pub env_file: PathBuf,
    /// Resolved working directory embedded in the service unit.
    pub working_directory: PathBuf,
    /// `systemd.timer` calendar expression embedded in the timer unit.
    pub calendar: String,
    /// Retention window passed to the scheduled collector.
    pub retention_seconds: u64,
}

impl GcScheduleInstallReport {
    pub fn print_summary(&self) {
        println!("service_path: {}", self.service_path.display());
        println!("timer_path: {}", self.timer_path.display());
        println!("binary_path: {}", self.binary_path.display());
        println!("env_file: {}", self.env_file.display());
        println!("working_directory: {}", self.working_directory.display());
        println!("calendar: {}", self.calendar);
        println!("retention_seconds: {}", self.retention_seconds);
    }
}

/// Summary for a scheduled-GC uninstall run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcScheduleUninstallReport {
    /// Service unit path targeted by the uninstall run.
    pub service_path: PathBuf,
    /// Timer unit path targeted by the uninstall run.
    pub timer_path: PathBuf,
    /// Whether the service file existed and was removed.
    pub removed_service: bool,
    /// Whether the timer file existed and was removed.
    pub removed_timer: bool,
}

impl GcScheduleUninstallReport {
    pub fn print_summary(&self) {
        println!("service_path: {}", self.service_path.display());
        println!("timer_path: {}", self.timer_path.display());
        println!("removed_service: {}", self.removed_service);
        println!("removed_timer: {}", self.removed_timer);
    }
}

/// Scheduled-GC helper failure.
#[derive(Debug, Error)]
pub enum GcScheduleError {
    /// One text field was empty after trimming.
    #[error("invalid {field}: value must not be empty")]
    EmptyValue {
        /// Invalid field name.
        field: &'static str,
    },
    /// One text field contained control characters.
    #[error("invalid {field}: control characters are not allowed")]
    ControlCharacters {
        /// Invalid field name.
        field: &'static str,
    },
    /// The output directory was not a directory after creation.
    #[error("invalid output directory: {0}")]
    InvalidOutputDirectory(PathBuf),
    /// One path that must be absolute was not absolute.
    #[error("{field} must be an absolute path: {path}")]
    NonAbsolutePath {
        /// Field name.
        field: &'static str,
        /// Invalid path.
        path: PathBuf,
    },
    /// The selected binary path did not exist as a regular file.
    #[error("shardline binary was not found at {0}")]
    MissingBinary(PathBuf),
    /// The selected environment file did not exist as a regular file.
    #[error("environment file was not found at {0}")]
    MissingEnvFile(PathBuf),
    /// A local control file exceeded the supported parser limit.
    #[error(
        "{field} exceeded supported size at {path}: {observed_bytes} bytes > {maximum_bytes} bytes"
    )]
    LocalFileTooLarge {
        /// File role.
        field: &'static str,
        /// File path.
        path: PathBuf,
        /// Observed file size.
        observed_bytes: u64,
        /// Maximum accepted file size.
        maximum_bytes: u64,
    },
    /// A local control file changed after validation and was rejected.
    #[error(
        "{field} changed during bounded read at {path}: expected {expected_bytes} bytes, observed {observed_bytes} bytes"
    )]
    LocalFileLengthMismatch {
        /// File role.
        field: &'static str,
        /// File path.
        path: PathBuf,
        /// Validated file size.
        expected_bytes: u64,
        /// Observed file size after bounded read.
        observed_bytes: u64,
    },
    /// The environment file contains an invalid line.
    #[error("invalid environment file line {line}: {detail}")]
    InvalidEnvFileLine {
        /// One-based line number.
        line: usize,
        /// Human-readable failure detail.
        detail: &'static str,
    },
    /// The configured environment file and explicit working directory disagreed.
    #[error(
        "working directory {working_directory} disagrees with SHARDLINE_ROOT_DIR={configured_root}"
    )]
    RootDirectoryConflict {
        /// Working directory requested for the generated service.
        working_directory: String,
        /// Root directory loaded from the environment file.
        configured_root: String,
    },
    /// A path referenced by the environment file did not exist.
    #[error("{field} points to a missing path: {path}")]
    MissingReferencedPath {
        /// Environment field name.
        field: &'static str,
        /// Referenced path.
        path: PathBuf,
    },
    /// The selected service user did not exist on the host.
    #[error("service user was not found on the host: {0}")]
    MissingUser(String),
    /// The selected service group did not exist on the host.
    #[error("service group was not found on the host: {0}")]
    MissingGroup(String),
    /// Filesystem access failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Installs systemd units for scheduled Shardline garbage collection.
///
/// # Errors
///
/// Returns [`GcScheduleError`] when one option is invalid or the unit files cannot be
/// written. On non-Linux platforms, always returns an error since systemd is not available.
pub fn install_gc_schedule(
    options: &GcScheduleInstallOptions,
) -> Result<GcScheduleInstallReport, GcScheduleError> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = options;
        Err(GcScheduleError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "scheduled garbage collection via systemd is only supported on Linux",
        )))
    }
    #[cfg(target_os = "linux")]
    {
        let resolved = resolve_install_options(options)?;

        let service_path = resolved
            .output_dir
            .join(format!("{}.service", resolved.unit_prefix));
        let timer_path = resolved
            .output_dir
            .join(format!("{}.timer", resolved.unit_prefix));

        let service_unit = render_service_unit(&resolved);
        let timer_unit = render_timer_unit(&resolved);

        if resolved.dry_run {
            println!(
                "Would write {} ({} bytes):\n{}\n",
                service_path.display(),
                service_unit.len(),
                service_unit
            );
            println!(
                "Would write {} ({} bytes):\n{}\n",
                timer_path.display(),
                timer_unit.len(),
                timer_unit
            );
        } else {
            ensure_output_directory(&resolved.output_dir)?;
            if !resolved.output_dir.is_dir() {
                return Err(GcScheduleError::InvalidOutputDirectory(resolved.output_dir));
            }
            ensure_output_directory(&resolved.working_directory)?;
            write_output_bytes(&service_path, service_unit.as_bytes(), true)?;
            write_output_bytes(&timer_path, timer_unit.as_bytes(), true)?;
        }

        Ok(GcScheduleInstallReport {
            service_path,
            timer_path,
            binary_path: resolved.binary_path,
            env_file: resolved.env_file,
            working_directory: resolved.working_directory,
            calendar: resolved.calendar,
            retention_seconds: resolved.retention_seconds,
        })
    }
}

/// Removes systemd units for scheduled Shardline garbage collection.
///
/// # Errors
///
/// Returns [`GcScheduleError`] when the output directory or unit prefix is invalid or a
/// filesystem operation fails.
pub fn uninstall_gc_schedule(
    output_dir: &Path,
    unit_prefix: &str,
) -> Result<GcScheduleUninstallReport, GcScheduleError> {
    validate_text_field("unit-prefix", unit_prefix)?;

    let service_path = output_dir.join(format!("{unit_prefix}.service"));
    let timer_path = output_dir.join(format!("{unit_prefix}.timer"));
    let removed_service = remove_if_present(&service_path)?;
    let removed_timer = remove_if_present(&timer_path)?;

    Ok(GcScheduleUninstallReport {
        service_path,
        timer_path,
        removed_service,
        removed_timer,
    })
}

fn remove_if_present(path: &Path) -> io::Result<bool> {
    remove_output_file_if_present(path)
}

#[cfg(target_os = "linux")]
fn resolve_install_options(
    options: &GcScheduleInstallOptions,
) -> Result<GcScheduleInstallOptions, GcScheduleError> {
    validate_text_field("unit-prefix", &options.unit_prefix)?;
    validate_text_field("calendar", &options.calendar)?;
    validate_absolute_path_field("env-file", &options.env_file)?;
    validate_absolute_path_field("working-directory", &options.working_directory)?;
    validate_text_field("user", &options.user)?;
    validate_text_field("group", &options.group)?;

    let binary_path = resolve_binary_path(&options.binary_path);
    match ensure_regular_local_file_path(&binary_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(GcScheduleError::MissingBinary(binary_path));
        }
        Err(error) => return Err(GcScheduleError::Io(error)),
    }

    match ensure_regular_local_file_path(&options.env_file) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(GcScheduleError::MissingEnvFile(options.env_file.clone()));
        }
        Err(error) => return Err(GcScheduleError::Io(error)),
    }
    let env = parse_env_file(&options.env_file)?;
    let configured_root = env.get("SHARDLINE_ROOT_DIR").cloned();
    let working_directory = match configured_root {
        Some(root) if options.working_directory == Path::new(DEFAULT_WORKING_DIRECTORY) => {
            PathBuf::from(root)
        }
        Some(root) if options.working_directory != Path::new(root.as_str()) => {
            return Err(GcScheduleError::RootDirectoryConflict {
                working_directory: options.working_directory.display().to_string(),
                configured_root: root,
            });
        }
        _none_or_matching => options.working_directory.clone(),
    };

    validate_absolute_path_field("binary-path", &binary_path)?;
    validate_absolute_path_field("working-directory", &working_directory)?;
    validate_user_exists(&options.user)?;
    validate_group_exists(&options.group)?;
    validate_env_referenced_path(&env, "SHARDLINE_TOKEN_SIGNING_KEY_FILE")?;
    validate_env_referenced_path(&env, "SHARDLINE_METRICS_TOKEN_FILE")?;
    validate_env_referenced_path(&env, "SHARDLINE_PROVIDER_CONFIG_FILE")?;
    validate_env_referenced_path(&env, "SHARDLINE_PROVIDER_API_KEY_FILE")?;

    Ok(GcScheduleInstallOptions {
        output_dir: options.output_dir.clone(),
        unit_prefix: options.unit_prefix.clone(),
        calendar: options.calendar.clone(),
        retention_seconds: options.retention_seconds,
        binary_path,
        env_file: options.env_file.clone(),
        working_directory,
        user: options.user.clone(),
        group: options.group.clone(),
        dry_run: options.dry_run,
    })
}

#[cfg(target_os = "linux")]
fn validate_path_field(field: &'static str, path: &Path) -> Result<(), GcScheduleError> {
    let rendered = path.to_string_lossy();
    validate_text_field(field, rendered.as_ref())
}

#[cfg(target_os = "linux")]
fn validate_absolute_path_field(field: &'static str, path: &Path) -> Result<(), GcScheduleError> {
    validate_path_field(field, path)?;
    if path.is_absolute() {
        return Ok(());
    }
    Err(GcScheduleError::NonAbsolutePath {
        field,
        path: path.to_path_buf(),
    })
}

fn validate_text_field(field: &'static str, value: &str) -> Result<(), GcScheduleError> {
    if value.trim().is_empty() {
        return Err(GcScheduleError::EmptyValue { field });
    }
    if value.chars().any(char::is_control) {
        return Err(GcScheduleError::ControlCharacters { field });
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn resolve_binary_path(configured_binary_path: &Path) -> PathBuf {
    if configured_binary_path != Path::new(DEFAULT_BINARY_PATH) {
        return configured_binary_path.to_path_buf();
    }
    if let Ok(current_exe) = current_exe()
        && current_exe.is_file()
    {
        return current_exe;
    }
    configured_binary_path.to_path_buf()
}

#[cfg(target_os = "linux")]
fn parse_env_file(path: &Path) -> Result<BTreeMap<String, String>, GcScheduleError> {
    let contents = read_text_file_with_limit("environment file", path, MAX_SYSTEMD_ENV_FILE_BYTES)?;
    let mut env = BTreeMap::new();
    for (index, line) in contents.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let without_export = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, raw_value)) = without_export.split_once('=') else {
            return Err(GcScheduleError::InvalidEnvFileLine {
                line: index.saturating_add(1),
                detail: "expected KEY=VALUE",
            });
        };
        let key = key.trim();
        let value = raw_value.trim();
        validate_text_field("env-key", key)?;
        validate_text_field("env-value", value)?;
        env.insert(key.to_owned(), strip_wrapping_quotes(value).to_owned());
    }
    Ok(env)
}

#[cfg(target_os = "linux")]
fn strip_wrapping_quotes(value: &str) -> &str {
    let Some(first) = value.chars().next() else {
        return value;
    };
    let Some(last) = value.chars().last() else {
        return value;
    };
    if value.len() >= 2 && ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
        return &value[1..value.len().saturating_sub(1)];
    }
    value
}

#[cfg(target_os = "linux")]
fn validate_env_referenced_path(
    env: &BTreeMap<String, String>,
    field: &'static str,
) -> Result<(), GcScheduleError> {
    let Some(value) = env.get(field) else {
        return Ok(());
    };
    let path = PathBuf::from(value);
    validate_absolute_path_field(field, &path)?;
    match ensure_regular_local_file_path(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(GcScheduleError::MissingReferencedPath { field, path })
        }
        Err(error) => Err(GcScheduleError::Io(error)),
    }
}

#[cfg(target_os = "linux")]
fn validate_user_exists(user: &str) -> Result<(), GcScheduleError> {
    let passwd = read_text_file_with_limit(
        "passwd database",
        Path::new("/etc/passwd"),
        MAX_SYSTEM_ACCOUNT_FILE_BYTES,
    )?;
    if passwd
        .lines()
        .any(|line| line.split(':').next() == Some(user))
    {
        return Ok(());
    }
    Err(GcScheduleError::MissingUser(user.to_owned()))
}

#[cfg(target_os = "linux")]
fn validate_group_exists(group: &str) -> Result<(), GcScheduleError> {
    let groups = read_text_file_with_limit(
        "group database",
        Path::new("/etc/group"),
        MAX_SYSTEM_ACCOUNT_FILE_BYTES,
    )?;
    if groups
        .lines()
        .any(|line| line.split(':').next() == Some(group))
    {
        return Ok(());
    }
    Err(GcScheduleError::MissingGroup(group.to_owned()))
}

#[cfg(any(target_os = "linux", test))]
fn read_text_file_with_limit(
    field: &'static str,
    path: &Path,
    maximum_bytes: u64,
) -> Result<String, GcScheduleError> {
    let mut file = open_local_text_file(path)?;
    let metadata = file.metadata()?;
    ensure_local_file_size_within_limit(field, path, metadata.len(), maximum_bytes)?;

    run_before_local_text_file_read_hook_for_tests(path);

    let bytes = read_bounded_local_text_file(field, path, &mut file, metadata.len())?;
    ensure_local_file_size_within_limit(
        field,
        path,
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        maximum_bytes,
    )?;
    let contents = String::from_utf8(bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} is not valid UTF-8: {error}"),
        )
    })?;

    Ok(contents)
}

#[cfg(any(target_os = "linux", test))]
#[cfg(unix)]
fn open_local_text_file(path: &Path) -> Result<File, GcScheduleError> {
    use std::os::unix::fs::OpenOptionsExt;

    ensure_regular_local_file_path(path)?;
    let resolved = resolve_platform_symlinks(path);
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&resolved)?)
}

#[cfg(any(target_os = "linux", test))]
#[cfg(not(unix))]
fn open_local_text_file(path: &Path) -> Result<File, GcScheduleError> {
    ensure_regular_local_file_path(path)?;
    Ok(File::open(path)?)
}

#[cfg(any(target_os = "linux", test))]
fn ensure_regular_local_file_path(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local control file path must be a regular file and must not be a symlink",
        ));
    }

    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn read_bounded_local_text_file(
    field: &'static str,
    path: &Path,
    file: &mut fs::File,
    expected_length: u64,
) -> Result<Vec<u8>, GcScheduleError> {
    let capacity = usize::try_from(expected_length).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity);
    let mut limited = file.by_ref().take(expected_length);
    limited.read_to_end(&mut bytes)?;

    let read_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_length != expected_length {
        return Err(GcScheduleError::LocalFileLengthMismatch {
            field,
            path: path.to_path_buf(),
            expected_bytes: expected_length,
            observed_bytes: read_length,
        });
    }

    let mut trailing = [0_u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(GcScheduleError::LocalFileLengthMismatch {
            field,
            path: path.to_path_buf(),
            expected_bytes: expected_length,
            observed_bytes: expected_length.saturating_add(1),
        });
    }

    let observed_length = file.metadata()?.len();
    if observed_length != expected_length {
        return Err(GcScheduleError::LocalFileLengthMismatch {
            field,
            path: path.to_path_buf(),
            expected_bytes: expected_length,
            observed_bytes: observed_length,
        });
    }

    Ok(bytes)
}

#[cfg(any(target_os = "linux", test))]
fn ensure_local_file_size_within_limit(
    field: &'static str,
    path: &Path,
    observed_bytes: u64,
    maximum_bytes: u64,
) -> Result<(), GcScheduleError> {
    if observed_bytes > maximum_bytes {
        return Err(GcScheduleError::LocalFileTooLarge {
            field,
            path: path.to_path_buf(),
            observed_bytes,
            maximum_bytes,
        });
    }

    Ok(())
}

#[cfg(test)]
fn set_before_local_text_file_read_hook(path: PathBuf, hook: impl FnOnce() + Send + 'static) {
    let mut slot = match BEFORE_LOCAL_TEXT_FILE_READ_HOOK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *slot = Some(LocalTextFileReadHookRegistration {
        path,
        hook: Box::new(hook),
    });
}

#[cfg(test)]
fn run_before_local_text_file_read_hook_for_tests(path: &Path) {
    let hook = match BEFORE_LOCAL_TEXT_FILE_READ_HOOK.lock() {
        Ok(mut guard) => take_matching_local_text_file_read_hook(&mut guard, path),
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            take_matching_local_text_file_read_hook(&mut guard, path)
        }
    };

    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_matching_local_text_file_read_hook(
    slot: &mut LocalTextFileReadHookSlot,
    path: &Path,
) -> Option<LocalTextFileReadHook> {
    if slot
        .as_ref()
        .is_none_or(|registration| registration.path != path)
    {
        return None;
    }

    slot.take().map(|registration| registration.hook)
}

#[cfg(not(test))]
#[cfg(any(target_os = "linux", test))]
const fn run_before_local_text_file_read_hook_for_tests(_path: &Path) {}

#[cfg(target_os = "linux")]
fn render_service_unit(options: &GcScheduleInstallOptions) -> String {
    format!(
        "[Unit]\nDescription=Shardline garbage collection\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nUser={user}\nGroup={group}\nEnvironmentFile={env_file}\nExecStart={binary_path} gc --mark --sweep --retention-seconds {retention_seconds}\nWorkingDirectory={working_directory}\nNoNewPrivileges=true\nPrivateTmp=true\nProtectSystem=strict\nProtectHome=true\nReadWritePaths={working_directory}\n",
        user = options.user,
        group = options.group,
        env_file = options.env_file.display(),
        binary_path = options.binary_path.display(),
        retention_seconds = options.retention_seconds,
        working_directory = options.working_directory.display(),
    )
}

#[cfg(target_os = "linux")]
fn render_timer_unit(options: &GcScheduleInstallOptions) -> String {
    format!(
        "[Unit]\nDescription=Run Shardline garbage collection on a schedule\n\n[Timer]\nOnCalendar={calendar}\nPersistent=true\nUnit={unit_prefix}.service\n\n[Install]\nWantedBy=timers.target\n",
        calendar = options.calendar,
        unit_prefix = options.unit_prefix,
    )
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::{fs::read_to_string, io::ErrorKind, os::unix::fs::symlink};
    use std::{
        fs::{OpenOptions, write as write_file},
        io::Write,
        path::{Path, PathBuf},
    };

    use super::{
        GcScheduleError, MAX_SYSTEMD_ENV_FILE_BYTES, read_text_file_with_limit,
        set_before_local_text_file_read_hook,
    };
    #[cfg(target_os = "linux")]
    use super::{GcScheduleInstallOptions, install_gc_schedule, uninstall_gc_schedule};

    #[cfg(target_os = "linux")]
    #[test]
    fn install_gc_schedule_writes_systemd_units() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let env_file = sandbox.path().join("shardline.env");
        let signing_key = sandbox.path().join("token.key");
        let root_dir = sandbox.path().join("data");
        let env_write = write_file(
            &env_file,
            format!(
                "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
                root_dir.display(),
                signing_key.display()
            ),
        );
        assert!(env_write.is_ok());
        let key_write = write_file(&signing_key, b"token");
        assert!(key_write.is_ok());

        let options = GcScheduleInstallOptions {
            output_dir: sandbox.path().to_path_buf(),
            retention_seconds: 600,
            calendar: "hourly".to_owned(),
            env_file,
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        };

        let report = install_gc_schedule(&options);
        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };

        let service = read_to_string(report.service_path);
        assert!(service.is_ok());
        let Ok(service) = service else {
            return;
        };
        assert!(service.contains("gc --mark --sweep --retention-seconds 600"));
        assert!(service.contains(&format!("WorkingDirectory={}", root_dir.display())));

        let timer = read_to_string(report.timer_path);
        assert!(timer.is_ok());
        let Ok(timer) = timer else {
            return;
        };
        assert!(timer.contains("OnCalendar=hourly"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn uninstall_gc_schedule_removes_both_units() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let env_file = sandbox.path().join("shardline.env");
        let signing_key = sandbox.path().join("token.key");
        let env_write = write_file(
            &env_file,
            format!(
                "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
                sandbox.path().join("data").display(),
                signing_key.display()
            ),
        );
        assert!(env_write.is_ok());
        let key_write = write_file(&signing_key, b"token");
        assert!(key_write.is_ok());

        let install = install_gc_schedule(&GcScheduleInstallOptions {
            output_dir: sandbox.path().to_path_buf(),
            env_file,
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        });
        assert!(install.is_ok());

        let report = uninstall_gc_schedule(sandbox.path(), "shardline-gc");
        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };
        assert!(report.removed_service);
        assert!(report.removed_timer);
        assert!(!report.service_path.exists());
        assert!(!report.timer_path.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn install_gc_schedule_rejects_control_characters() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let env_file = sandbox.path().join("shardline.env");
        let env_write = write_file(&env_file, "SHARDLINE_ROOT_DIR=/var/lib/shardline\n");
        assert!(env_write.is_ok());

        let result = install_gc_schedule(&GcScheduleInstallOptions {
            output_dir: sandbox.path().to_path_buf(),
            calendar: "daily\nmalicious".to_owned(),
            env_file,
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        });

        assert!(matches!(
            result,
            Err(GcScheduleError::ControlCharacters { field: "calendar" })
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn install_gc_schedule_uses_root_dir_from_env_file() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let env_file = sandbox.path().join("shardline.env");
        let signing_key = sandbox.path().join("token.key");
        let configured_root = sandbox.path().join("custom-root");
        let env_write = write_file(
            &env_file,
            format!(
                "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
                configured_root.display(),
                signing_key.display()
            ),
        );
        assert!(env_write.is_ok());
        let key_write = write_file(&signing_key, b"token");
        assert!(key_write.is_ok());

        let report = install_gc_schedule(&GcScheduleInstallOptions {
            output_dir: sandbox.path().to_path_buf(),
            env_file,
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        });
        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };
        assert_eq!(report.working_directory, configured_root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn install_gc_schedule_rejects_missing_referenced_secret() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let env_file = sandbox.path().join("shardline.env");
        let env_write = write_file(
            &env_file,
            format!(
                "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
                sandbox.path().join("data").display(),
                sandbox.path().join("missing.key").display()
            ),
        );
        assert!(env_write.is_ok());

        let result = install_gc_schedule(&GcScheduleInstallOptions {
            output_dir: sandbox.path().to_path_buf(),
            env_file,
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        });

        assert!(matches!(
            result,
            Err(GcScheduleError::MissingReferencedPath {
                field: "SHARDLINE_TOKEN_SIGNING_KEY_FILE",
                ..
            })
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn install_gc_schedule_rejects_relative_env_file() {
        let result = install_gc_schedule(&GcScheduleInstallOptions {
            env_file: "relative.env".into(),
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        });

        assert!(matches!(
            result,
            Err(GcScheduleError::NonAbsolutePath {
                field: "env-file",
                ..
            })
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn install_gc_schedule_rejects_oversized_env_file() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let env_file = sandbox.path().join("oversized.env");
        let env_write = write_file(
            &env_file,
            format!(
                "SHARDLINE_ROOT_DIR=/var/lib/shardline\n{}",
                "A".repeat(usize::try_from(MAX_SYSTEMD_ENV_FILE_BYTES).unwrap_or(0))
            ),
        );
        assert!(env_write.is_ok());

        let result = install_gc_schedule(&GcScheduleInstallOptions {
            output_dir: sandbox.path().to_path_buf(),
            env_file,
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        });

        assert!(matches!(
            result,
            Err(GcScheduleError::LocalFileTooLarge {
                field: "environment file",
                maximum_bytes: MAX_SYSTEMD_ENV_FILE_BYTES,
                ..
            })
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resolve_binary_path_returns_valid_path() {
        // When a non-default path is supplied, it should be returned as-is.
        let custom = Path::new("/opt/shardline/bin/shardline");
        let result = super::resolve_binary_path(custom);
        assert_eq!(result, custom);

        // When the default path is supplied, resolve_binary_path attempts
        // current_exe() first, falling back to the default. Either way
        // the returned path should be non-empty.
        let default = Path::new("/usr/local/bin/shardline");
        let result = super::resolve_binary_path(default);
        assert!(!result.as_os_str().is_empty());
    }

    #[test]
    fn read_text_file_with_limit_rejects_growth_after_validation() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };

        let file_path = sandbox.path().join("growth.env");
        let write = write_file(&file_path, b"SHARDLINE_ROOT_DIR=/var/lib/shardline\n");
        assert!(write.is_ok());
        let append_path = file_path.clone();

        set_before_local_text_file_read_hook(file_path.clone(), move || {
            let append = OpenOptions::new()
                .append(true)
                .open(&append_path)
                .and_then(|mut file| file.write_all(b"INJECTED=1\n"));
            assert!(append.is_ok());
        });

        let result = read_text_file_with_limit(
            "environment file",
            Path::new(&file_path),
            MAX_SYSTEMD_ENV_FILE_BYTES,
        );

        assert!(matches!(
            result,
            Err(GcScheduleError::LocalFileLengthMismatch {
                field: "environment file",
                ..
            })
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn install_gc_schedule_rejects_symlinked_env_file() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let env_target = sandbox.path().join("real.env");
        let signing_key = sandbox.path().join("token.key");
        let env_link = sandbox.path().join("shardline.env");
        let env_write = write_file(
            &env_target,
            format!(
                "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
                sandbox.path().join("data").display(),
                signing_key.display()
            ),
        );
        assert!(env_write.is_ok());
        let key_write = write_file(&signing_key, b"token");
        assert!(key_write.is_ok());
        let linked = symlink(&env_target, &env_link);
        assert!(linked.is_ok());

        let result = install_gc_schedule(&GcScheduleInstallOptions {
            output_dir: sandbox.path().to_path_buf(),
            env_file: env_link,
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        });

        assert!(matches!(
            result,
            Err(GcScheduleError::Io(error)) if error.kind() == ErrorKind::InvalidInput
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn install_gc_schedule_rejects_symlinked_referenced_secret() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let env_file = sandbox.path().join("shardline.env");
        let target = sandbox.path().join("real.key");
        let link = sandbox.path().join("linked.key");
        let target_write = write_file(&target, b"token");
        assert!(target_write.is_ok());
        let linked = symlink(&target, &link);
        assert!(linked.is_ok());
        let env_write = write_file(
            &env_file,
            format!(
                "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
                sandbox.path().join("data").display(),
                link.display()
            ),
        );
        assert!(env_write.is_ok());

        let result = install_gc_schedule(&GcScheduleInstallOptions {
            output_dir: sandbox.path().to_path_buf(),
            env_file,
            user: "root".to_owned(),
            group: "root".to_owned(),
            ..GcScheduleInstallOptions::default()
        });

        assert!(matches!(
            result,
            Err(GcScheduleError::Io(error)) if error.kind() == ErrorKind::InvalidInput
        ));
    }

    // ── GcScheduleError Display ───────────────────────────────────────────

    #[test]
    fn gc_schedule_error_empty_value_display() {
        let err = GcScheduleError::EmptyValue {
            field: "unit-prefix",
        };
        assert_eq!(
            err.to_string(),
            "invalid unit-prefix: value must not be empty"
        );
    }

    #[test]
    fn gc_schedule_error_control_characters_display() {
        let err = GcScheduleError::ControlCharacters { field: "calendar" };
        assert_eq!(
            err.to_string(),
            "invalid calendar: control characters are not allowed"
        );
    }

    #[test]
    fn gc_schedule_error_invalid_output_directory_display() {
        let err = GcScheduleError::InvalidOutputDirectory(PathBuf::from("/bad/dir"));
        assert_eq!(err.to_string(), "invalid output directory: /bad/dir");
    }

    #[test]
    fn gc_schedule_error_non_absolute_path_display() {
        let err = GcScheduleError::NonAbsolutePath {
            field: "env-file",
            path: PathBuf::from("relative.env"),
        };
        assert_eq!(
            err.to_string(),
            "env-file must be an absolute path: relative.env"
        );
    }

    #[test]
    fn gc_schedule_error_missing_binary_display() {
        let err = GcScheduleError::MissingBinary(PathBuf::from("/usr/local/bin/shardline"));
        assert_eq!(
            err.to_string(),
            "shardline binary was not found at /usr/local/bin/shardline"
        );
    }

    #[test]
    fn gc_schedule_error_missing_env_file_display() {
        let err = GcScheduleError::MissingEnvFile(PathBuf::from("/etc/shardline/shardline.env"));
        assert!(err.to_string().contains("environment file was not found"));
    }

    #[test]
    fn gc_schedule_error_local_file_too_large_display() {
        let err = GcScheduleError::LocalFileTooLarge {
            field: "environment file",
            path: PathBuf::from("/env"),
            observed_bytes: 70000,
            maximum_bytes: 65536,
        };
        let msg = err.to_string();
        assert!(msg.contains("exceeded supported size"));
        assert!(msg.contains("environment file"));
    }

    #[test]
    fn gc_schedule_error_local_file_length_mismatch_display() {
        let err = GcScheduleError::LocalFileLengthMismatch {
            field: "environment file",
            path: PathBuf::from("/env"),
            expected_bytes: 100,
            observed_bytes: 105,
        };
        let msg = err.to_string();
        assert!(msg.contains("changed during bounded read"));
    }

    #[test]
    fn gc_schedule_error_invalid_env_file_line_display() {
        let err = GcScheduleError::InvalidEnvFileLine {
            line: 5,
            detail: "expected KEY=VALUE",
        };
        let msg = err.to_string();
        assert!(msg.contains("line 5"));
        assert!(msg.contains("expected KEY=VALUE"));
    }

    #[test]
    fn gc_schedule_error_root_directory_conflict_display() {
        let err = GcScheduleError::RootDirectoryConflict {
            working_directory: "/var/lib/shardline".to_owned(),
            configured_root: "/opt/shardline".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("/var/lib/shardline"));
        assert!(msg.contains("/opt/shardline"));
    }

    #[test]
    fn gc_schedule_error_missing_referenced_path_display() {
        let err = GcScheduleError::MissingReferencedPath {
            field: "SHARDLINE_TOKEN_SIGNING_KEY_FILE",
            path: PathBuf::from("/run/secrets/token.key"),
        };
        let msg = err.to_string();
        assert!(msg.contains("points to a missing path"));
        assert!(msg.contains("SHARDLINE_TOKEN_SIGNING_KEY_FILE"));
    }

    #[test]
    fn gc_schedule_error_missing_user_display() {
        let err = GcScheduleError::MissingUser("nobody".to_owned());
        assert_eq!(
            err.to_string(),
            "service user was not found on the host: nobody"
        );
    }

    #[test]
    fn gc_schedule_error_missing_group_display() {
        let err = GcScheduleError::MissingGroup("nogroup".to_owned());
        assert_eq!(
            err.to_string(),
            "service group was not found on the host: nogroup"
        );
    }

    #[test]
    fn gc_schedule_error_io_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let err = GcScheduleError::Io(io_err);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn gc_schedule_error_debug() {
        let err = GcScheduleError::EmptyValue { field: "test" };
        let debug = format!("{err:?}");
        assert!(debug.contains("EmptyValue"));
    }

    // ── render_service_unit / render_timer_unit (non-linux) ──────────────

    #[test]
    fn install_gc_schedule_on_non_linux_returns_unsupported_error() {
        // On non-Linux platforms, install_gc_schedule returns an Unsupported Io error.
        let options = super::GcScheduleInstallOptions::default();
        let result = super::install_gc_schedule(&options);
        #[cfg(not(target_os = "linux"))]
        {
            assert!(result.is_err());
            let Err(err) = result else { return };
            assert!(
                matches!(err, GcScheduleError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::Unsupported)
            );
        }
        #[cfg(target_os = "linux")]
        {
            // On Linux, it will try to validate paths which don't exist.
            let _ = result;
        }
    }

    // ── validate_text_field ────────────────────────────────────────────────

    #[test]
    fn validate_text_field_accepts_valid_value() {
        let result = super::validate_text_field("test", "valid-value");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_text_field_rejects_empty_value() {
        let result = super::validate_text_field("unit-prefix", "");
        assert!(matches!(
            result,
            Err(GcScheduleError::EmptyValue {
                field: "unit-prefix"
            })
        ));
    }

    #[test]
    fn validate_text_field_rejects_whitespace_only() {
        let result = super::validate_text_field("calendar", "   ");
        assert!(matches!(
            result,
            Err(GcScheduleError::EmptyValue { field: "calendar" })
        ));
    }

    #[test]
    fn validate_text_field_rejects_control_characters() {
        let result = super::validate_text_field("user", "user\nname");
        assert!(matches!(
            result,
            Err(GcScheduleError::ControlCharacters { field: "user" })
        ));
    }

    #[test]
    fn validate_text_field_rejects_tab_character() {
        let result = super::validate_text_field("group", "my\tgroup");
        assert!(matches!(
            result,
            Err(GcScheduleError::ControlCharacters { field: "group" })
        ));
    }

    // ── GcScheduleInstallReport::print_summary ──────────────────────────

    #[test]
    fn install_report_print_summary() {
        let report = super::GcScheduleInstallReport {
            service_path: PathBuf::from("/etc/systemd/system/shardline-gc.service"),
            timer_path: PathBuf::from("/etc/systemd/system/shardline-gc.timer"),
            binary_path: PathBuf::from("/usr/local/bin/shardline"),
            env_file: PathBuf::from("/etc/shardline/shardline.env"),
            working_directory: PathBuf::from("/var/lib/shardline"),
            calendar: "daily".to_owned(),
            retention_seconds: 86400,
        };
        report.print_summary();
        assert_eq!(report.retention_seconds, 86400);
        assert_eq!(report.calendar, "daily");
    }

    // ── GcScheduleUninstallReport::print_summary ────────────────────────

    #[test]
    fn uninstall_report_print_summary() {
        let report = super::GcScheduleUninstallReport {
            service_path: PathBuf::from("/etc/systemd/system/test.service"),
            timer_path: PathBuf::from("/etc/systemd/system/test.timer"),
            removed_service: true,
            removed_timer: false,
        };
        report.print_summary();
        assert!(report.removed_service);
        assert!(!report.removed_timer);
    }

    #[test]
    fn uninstall_report_both_removed() {
        let report = super::GcScheduleUninstallReport {
            service_path: PathBuf::from("/etc/systemd/system/a.service"),
            timer_path: PathBuf::from("/etc/systemd/system/a.timer"),
            removed_service: true,
            removed_timer: true,
        };
        report.print_summary();
        assert!(report.removed_service);
        assert!(report.removed_timer);
    }

    // ── GcScheduleInstallOptions Default ────────────────────────────────

    #[test]
    fn install_options_defaults() {
        let opts = super::GcScheduleInstallOptions::default();
        assert_eq!(opts.unit_prefix, "shardline-gc");
        assert_eq!(opts.retention_seconds, 86_400);
        assert_eq!(opts.calendar, "*-*-* 03:17:00");
        assert!(!opts.dry_run);
    }

    // ── strip_wrapping_quotes (linux only) ───────────────────────────────

    #[cfg(target_os = "linux")]
    #[test]
    fn strip_wrapping_quotes_double_quotes() {
        assert_eq!(super::strip_wrapping_quotes("\"value\""), "value");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn strip_wrapping_quotes_single_quotes() {
        assert_eq!(super::strip_wrapping_quotes("'value'"), "value");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn strip_wrapping_quotes_no_quotes() {
        assert_eq!(super::strip_wrapping_quotes("value"), "value");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn strip_wrapping_quotes_empty_string() {
        assert_eq!(super::strip_wrapping_quotes(""), "");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn strip_wrapping_quotes_single_char() {
        assert_eq!(super::strip_wrapping_quotes("a"), "a");
    }

    // ── remove_if_present (the local_output wrapper) ─────────────────────

    #[test]
    fn remove_if_present_absent_file_returns_ok_false() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("nonexistent");
        // remove_if_present delegates to remove_output_file_if_present
        let result = super::remove_if_present(&path);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    // ── GcScheduleInstallOptions Default ────────────────────────────────

    #[test]
    fn install_options_default_values() {
        let opts = super::GcScheduleInstallOptions::default();
        assert_eq!(opts.output_dir, PathBuf::from("/etc/systemd/system"));
        assert_eq!(opts.unit_prefix, "shardline-gc");
        assert_eq!(opts.calendar, "*-*-* 03:17:00");
        assert_eq!(opts.retention_seconds, 86_400);
        assert_eq!(opts.binary_path, PathBuf::from("/usr/local/bin/shardline"));
        assert_eq!(opts.env_file, PathBuf::from("/etc/shardline/shardline.env"));
        assert_eq!(opts.working_directory, PathBuf::from("/var/lib/shardline"));
        assert_eq!(opts.user, "shardline");
        assert_eq!(opts.group, "shardline");
        assert!(!opts.dry_run);
    }

    // ── uninstall_gc_schedule validation ────────────────────────────────

    #[test]
    fn uninstall_gc_schedule_rejects_empty_prefix() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = super::uninstall_gc_schedule(sandbox.path(), "");
        assert!(matches!(
            result,
            Err(GcScheduleError::EmptyValue {
                field: "unit-prefix"
            })
        ));
    }

    #[test]
    fn uninstall_gc_schedule_rejects_control_chars_in_prefix() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = super::uninstall_gc_schedule(sandbox.path(), "test\nunit");
        assert!(matches!(
            result,
            Err(GcScheduleError::ControlCharacters {
                field: "unit-prefix"
            })
        ));
    }

    #[test]
    fn uninstall_gc_schedule_removes_existing_files() {
        let sandbox = tempfile::tempdir().unwrap();
        let svc = sandbox.path().join("test.service");
        let tim = sandbox.path().join("test.timer");
        std::fs::write(&svc, b"service").unwrap();
        std::fs::write(&tim, b"timer").unwrap();

        let result = super::uninstall_gc_schedule(sandbox.path(), "test");
        assert!(result.is_ok());
        let report = result.unwrap();
        assert!(report.removed_service);
        assert!(report.removed_timer);
        assert!(!svc.exists());
        assert!(!tim.exists());
    }

    // ── ensure_local_file_size_within_limit ─────────────────────────────

    #[test]
    fn ensure_local_file_size_within_limit_accepts_exact_max() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("test.txt");
        let result = super::ensure_local_file_size_within_limit(
            "test",
            &path,
            super::MAX_SYSTEMD_ENV_FILE_BYTES,
            super::MAX_SYSTEMD_ENV_FILE_BYTES,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn ensure_local_file_size_within_limit_rejects_oversized() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("oversized.txt");
        let result = super::ensure_local_file_size_within_limit(
            "test",
            &path,
            100_000,
            super::MAX_SYSTEMD_ENV_FILE_BYTES,
        );
        assert!(matches!(
            result,
            Err(GcScheduleError::LocalFileTooLarge { .. })
        ));
    }

    #[test]
    fn ensure_local_file_size_within_limit_accepts_small() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("small.txt");
        let result = super::ensure_local_file_size_within_limit(
            "test",
            &path,
            100,
            super::MAX_SYSTEMD_ENV_FILE_BYTES,
        );
        assert!(result.is_ok());
    }

    // ── ensure_regular_local_file_path ──────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn ensure_regular_local_file_path_rejects_directory() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = super::ensure_regular_local_file_path(sandbox.path());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_regular_local_file_path_rejects_symlink() {
        use std::os::unix::fs::symlink;
        let sandbox = tempfile::tempdir().unwrap();
        let target = sandbox.path().join("real.txt");
        std::fs::write(&target, b"content").unwrap();
        let link = sandbox.path().join("link.txt");
        symlink(&target, &link).unwrap();

        let result = super::ensure_regular_local_file_path(&link);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_regular_local_file_path_accepts_regular_file() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("regular.txt");
        std::fs::write(&path, b"content").unwrap();

        let result = super::ensure_regular_local_file_path(&path);
        assert!(result.is_ok());
    }

    // ── read_bounded_local_text_file with trailing data ─────────────────

    #[test]
    fn read_bounded_local_text_file_rejects_trailing_data() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("trailing.txt");
        let mut content = b"KEY=VALUE".to_vec();
        content.push(b'\n');
        std::fs::write(&path, &content).unwrap();

        let mut file = std::fs::File::open(&path).unwrap();
        let result = super::read_bounded_local_text_file(
            "test",
            &path,
            &mut file,
            content.len() as u64 - 1, // expect 1 less byte
        );
        assert!(matches!(
            result,
            Err(GcScheduleError::LocalFileLengthMismatch { .. })
        ));
    }

    // ── read_text_file_with_limit rejects missing file ──────────────────

    #[test]
    fn read_text_file_with_limit_rejects_missing_file() {
        let result = super::read_text_file_with_limit(
            "test",
            Path::new("/nonexistent-gc-test-file"),
            super::MAX_SYSTEMD_ENV_FILE_BYTES,
        );
        assert!(result.is_err());
    }

    // ── validate_text_field with edge values ────────────────────────────

    #[test]
    fn validate_text_field_rejects_newline() {
        let result = super::validate_text_field("test", "line1\nline2");
        assert!(matches!(
            result,
            Err(GcScheduleError::ControlCharacters { field: "test" })
        ));
    }

    #[test]
    fn validate_text_field_rejects_tab() {
        let result = super::validate_text_field("test", "col1\tcol2");
        assert!(matches!(
            result,
            Err(GcScheduleError::ControlCharacters { field: "test" })
        ));
    }
}
