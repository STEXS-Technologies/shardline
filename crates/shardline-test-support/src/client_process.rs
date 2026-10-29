//! Deadlines for real-client contracts, including recursive client subprocesses.
use std::{
    io::{self, Read, Seek, Write},
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

struct ClientProcess {
    child: Child,
}

impl Drop for ClientProcess {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Ok(group) = libc::pid_t::try_from(self.child.id())
            && let Some(group) = group.checked_neg()
        {
            // SAFETY: spawn creates a new process group whose ID is the child's
            // PID. Signal only that owned group, never the test runner's group.
            unsafe { libc::kill(group, libc::SIGKILL) };
        }
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

/// Run a native client with a deadline, without logging arguments or credentials.
/// On Unix, clean up its entire process group, including recursive lazy fetches.
/// On Linux, the immediate client is also killed if its calling test thread dies.
/// Non-Unix platforms terminate the immediate child only.
///
/// Temporary output files prevent a full stdout/stderr pipe from deadlocking
/// the deadline loop. A timeout returns `TimedOut` rather than a passing result.
///
/// # Errors
/// Returns an I/O error if output capture, process startup or waiting fails,
/// or `TimedOut` when the client exceeds the deadline.
pub fn native_client_output(command: &mut Command, timeout: Duration) -> io::Result<Output> {
    run_client(command, timeout, Stdio::null())
}

/// Run a native client with exact input bytes and the same deadline/cleanup
/// guarantees as [`native_client_output`]. Private temporary files avoid pipe
/// deadlocks when the child emits output before reading its input.
///
/// # Errors
/// Returns input/output capture, startup, waiting or deadline errors.
pub fn native_client_output_with_input(
    command: &mut Command,
    timeout: Duration,
    input: &[u8],
) -> io::Result<Output> {
    let mut stdin = tempfile::tempfile()?;
    stdin.write_all(input)?;
    stdin.rewind()?;
    run_client(command, timeout, Stdio::from(stdin))
}

fn run_client(command: &mut Command, timeout: Duration, stdin: Stdio) -> io::Result<Output> {
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    command
        .stdin(stdin)
        .stdout(stdout.try_clone()?)
        .stderr(stderr.try_clone()?);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: getpid has no preconditions and does not modify process state.
        let parent = unsafe { libc::getpid() };
        // SAFETY: the child hook calls only Linux process-control syscalls and
        // constructs an I/O error on failure; it takes no locks or allocations
        // on the successful path between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                // The caller may have died between fork and installing the
                // death signal. Do not launch an unowned client in that race.
                if libc::getppid() != parent {
                    return Err(io::Error::from(io::ErrorKind::Interrupted));
                }
                Ok(())
            });
        }
    }
    let mut process = ClientProcess {
        child: command.spawn()?,
    };
    let started = Instant::now();
    let status = loop {
        if let Some(status) = process.child.try_wait()? {
            break status;
        }
        if started.elapsed() >= timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "native client exceeded test deadline",
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    drop(process);
    stdout.rewind()?;
    stderr.rewind()?;
    let mut output = Output {
        status,
        stdout: Vec::new(),
        stderr: Vec::new(),
    };
    stdout.read_to_end(&mut output.stdout)?;
    stderr.read_to_end(&mut output.stderr)?;
    Ok(output)
}

#[cfg(all(test, unix))]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    #[test]
    fn captures_output_larger_than_a_pipe_without_deadlock() {
        let output = native_client_output(
            Command::new("sh").args([
                "-c",
                "head -c 1048576 /dev/zero; printf diagnostic >&2; exit 7",
            ]),
            Duration::from_secs(5),
        )
        .expect("client fixture must run");
        assert_eq!(output.status.code(), Some(7));
        assert_eq!(output.stdout.len(), 1048576);
        assert_eq!(output.stderr, b"diagnostic");
    }

    #[test]
    fn captures_output_before_consuming_large_binary_input() {
        let input: Vec<u8> = (0..1_048_576).map(|index| (index % 256) as u8).collect();
        let output = native_client_output_with_input(
            Command::new("sh").args(["-c", "head -c 1048576 /dev/zero; cat"]),
            Duration::from_secs(5),
            &input,
        )
        .expect("bidirectional client fixture must run");
        assert!(output.status.success());
        let (prefix, echoed) = output.stdout.split_at(input.len());
        assert!(prefix.iter().all(|byte| *byte == 0));
        assert_eq!(echoed, input);
    }

    #[test]
    fn deadline_applies_when_client_never_consumes_input() {
        let error = native_client_output_with_input(
            Command::new("sh").args(["-c", "sleep 10"]),
            Duration::from_millis(100),
            &vec![0; 1_048_576],
        )
        .expect_err("non-reading client must time out");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn deadline_stops_descendants_before_they_can_write() {
        let directory = tempfile::tempdir().expect("client fixture directory");
        let marker = directory.path().join("escaped");
        let started = Instant::now();
        let result = native_client_output(
            Command::new("sh")
                .args([
                    "-c",
                    "(sleep 1; printf escaped > \"$1\") & wait",
                    "client-fixture",
                ])
                .arg(&marker),
            Duration::from_millis(100),
        );
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::TimedOut));
        assert!(started.elapsed() < Duration::from_secs(2));
        std::thread::sleep(Duration::from_millis(1200));
        assert!(
            !marker.exists(),
            "timed-out client's descendant escaped cleanup"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "subprocess entry for parent death regression"]
    fn parent_death_fixture() {
        let marker = std::env::var_os("SHARDLINE_NATIVE_PARENT_DEATH_MARKER")
            .expect("parent-death marker required");
        native_client_output(
            Command::new("sh")
                .args([
                    "-c",
                    "printf '%s' \"$$\" > \"$1\"; exec sleep 30",
                    "fixture",
                ])
                .arg(marker),
            Duration::from_secs(40),
        )
        .expect("parent should be killed before the client completes");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn abrupt_parent_death_stops_native_client() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let marker = directory.path().join("client-pid");
        let mut parent = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "client_process::tests::parent_death_fixture",
                "--ignored",
            ])
            .env("SHARDLINE_NATIVE_PARENT_DEATH_MARKER", &marker)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let startup_deadline = Instant::now() + Duration::from_secs(5);
        let client = loop {
            if let Ok(contents) = std::fs::read_to_string(&marker)
                && let Ok(pid) = contents.parse::<libc::pid_t>()
            {
                break pid;
            }
            if Instant::now() >= startup_deadline {
                drop(parent.kill());
                drop(parent.wait());
                return Err(io::Error::other("native client did not start"));
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        parent.kill()?;
        parent.wait()?;
        let shutdown_deadline = Instant::now() + Duration::from_secs(2);
        let stopped = loop {
            let status = std::fs::read_to_string(format!("/proc/{client}/stat"));
            let stopped = match status {
                Err(error) if error.kind() == io::ErrorKind::NotFound => true,
                Ok(status) => status
                    .rsplit_once(") ")
                    .is_some_and(|(_, fields)| fields.starts_with('Z')),
                Err(_) => false,
            };
            if stopped || Instant::now() >= shutdown_deadline {
                break stopped;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        // Clean up the owned client even when exercising the unfixed behavior.
        if !stopped {
            // SAFETY: the PID was emitted by this test's private child fixture.
            unsafe { libc::kill(client, libc::SIGKILL) };
            return Err(io::Error::other(
                "native client survived abrupt test-parent termination",
            ));
        }
        Ok(())
    }
}
