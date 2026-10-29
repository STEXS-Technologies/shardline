//! Owned process groups for CLI lifecycle observations and interruption drills.
use std::{
    io,
    os::unix::process::CommandExt,
    process::{Child, Command, Stdio},
};

pub(crate) struct OwnedProcess(pub(crate) Child);

impl OwnedProcess {
    pub(crate) fn spawn(command: &mut Command) -> io::Result<Self> {
        command
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command.spawn().map(Self)
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if let Ok(pid) = libc::pid_t::try_from(self.0.id())
            && let Some(group) = pid.checked_neg()
        {
            // SAFETY: spawn establishes this owned group with the child's PID;
            // the test runner and unrelated fixtures are outside that group.
            unsafe { libc::kill(group, libc::SIGKILL) };
        }
        drop(self.0.kill());
        drop(self.0.wait());
    }
}
