use serde::{Deserialize, Serialize};

/// Observation identity only. A foreign owner cannot be fenced or declared dead
/// from its recorded PID; effects still require independent reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskOwner {
    /// Process that owns admission and completion.
    pub pid: u32,
    /// Machine identity when available.
    pub host: Option<String>,
    /// Boot identity when available.
    pub boot: Option<String>,
    /// Linux PID namespace inode when available.
    pub namespace: Option<u64>,
    /// Process start ticks, preventing PID-reuse observations.
    pub birth: Option<u64>,
}

impl TaskOwner {
    /// Captures available local identity without requiring another service.
    #[must_use]
    pub fn current() -> Self {
        let pid = std::process::id();
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            Self {
                pid,
                host: read_identity("/etc/machine-id"),
                boot: read_identity("/proc/sys/kernel/random/boot_id"),
                namespace: std::fs::metadata("/proc/self/ns/pid")
                    .ok()
                    .map(|value| value.ino()),
                birth: process(pid).ok().flatten().map(|(_, birth)| birth),
            }
        }
        #[cfg(not(target_os = "linux"))]
        Self {
            pid,
            host: None,
            boot: None,
            namespace: None,
            birth: None,
        }
    }

    /// Returns a local process observation, or `None` for an unobservable owner.
    /// A dead controller observation does not establish settlement of remote effects.
    #[must_use]
    pub fn alive_here(&self) -> Option<bool> {
        #[cfg(target_os = "linux")]
        {
            let here = Self::current();
            if self.boot.is_none()
                || self.namespace.is_none()
                || self.birth.is_none()
                || self.boot != here.boot
                || self.namespace != here.namespace
                || self.host != here.host
            {
                return None;
            }
            match process(self.pid) {
                Ok(Some((live, birth))) => Some(live && Some(birth) == self.birth),
                Ok(None) => Some(false),
                Err(_) => None,
            }
        }
        #[cfg(not(target_os = "linux"))]
        None
    }
}

#[cfg(target_os = "linux")]
fn read_identity(path: &str) -> Option<String> {
    use std::io::Read;
    let mut text = String::new();
    std::fs::File::open(path)
        .ok()?
        .take(1024)
        .read_to_string(&mut text)
        .ok()?;
    let value = text.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

#[cfg(target_os = "linux")]
fn process(pid: u32) -> std::io::Result<Option<(bool, u64)>> {
    let text = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let invalid =
        || std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid process identity");
    let tail = text.rsplit_once(')').ok_or_else(invalid)?.1;
    let state = tail.split_whitespace().next().ok_or_else(invalid)?;
    let birth = tail
        .split_whitespace()
        .nth(19)
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_error| invalid())?;
    Ok(Some((!matches!(state, "Z" | "X"), birth)))
}
