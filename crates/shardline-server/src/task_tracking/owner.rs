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
            // A process from an earlier boot cannot still own work on this
            // same identified host. This permits completed local evidence to
            // be exported after reboot without mistaking a reused PID for it.
            // Different hosts or PID namespaces within one boot stay unknown.
            if self.host.is_some()
                && self.host == here.host
                && self.boot.is_some()
                && here.boot.is_some()
                && self.boot != here.boot
            {
                return Some(false);
            }
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::TaskOwner;

    #[test]
    fn prior_boot_on_same_identified_host_has_no_live_owner() {
        let mut owner = TaskOwner::current();
        if owner.host.is_none() || owner.boot.is_none() {
            return;
        }
        owner.boot = Some("previous-boot".to_owned());
        assert_eq!(owner.alive_here(), Some(false));
    }

    #[test]
    fn foreign_hosts_and_namespaces_never_establish_local_death() {
        let mut foreign = TaskOwner::current();
        foreign.host = Some("another-host".to_owned());
        foreign.boot = Some("another-boot".to_owned());
        assert_eq!(foreign.alive_here(), None);
        let mut namespace = TaskOwner::current();
        namespace.namespace = Some(namespace.namespace.unwrap_or_default().saturating_add(1));
        assert_eq!(namespace.alive_here(), None);
        let mut unidentified = TaskOwner::current();
        unidentified.host = None;
        unidentified.boot = Some("unidentified-boot".to_owned());
        assert_eq!(unidentified.alive_here(), None);
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
