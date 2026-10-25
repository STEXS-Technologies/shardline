//! Owned process boundary for an offline SQLite backup fixture.

use super::{DrillHarness, Duration, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use tempfile::{NamedTempFile, TempDir};

const CHILD_ROOT: &str = "SHARDLINE_BACKUP_DRILL_CHILD_ROOT";
const CHILD_READY: &str = "SHARDLINE_BACKUP_DRILL_CHILD_READY";

pub(super) async fn serve_child() {
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let mut harness = DrillHarness::new(3600);
        harness.root = PathBuf::from(root);
        harness.spawn_server().await;
        std::fs::write(std::env::var_os(CHILD_READY).unwrap(), &harness.base_url).unwrap();
        std::future::pending::<()>().await;
    }
}

pub(super) struct BackupProcess {
    child: Child,
    _control: TempDir,
    _log: NamedTempFile,
}

impl BackupProcess {
    pub(super) async fn spawn(harness: &mut DrillHarness) -> Self {
        let control = tempfile::tempdir().unwrap();
        let ready = control.path().join("ready");
        let log = NamedTempFile::new().unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "drill9_backup_destroy_restore_fsck_and_download"])
            .env(CHILD_ROOT, &harness.root)
            .env(CHILD_READY, &ready)
            .stdout(Stdio::from(log.reopen().unwrap()))
            .stderr(Stdio::from(log.reopen().unwrap()))
            .spawn()
            .unwrap();
        let mut process = Self {
            child,
            _control: control,
            _log: log,
        };
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(
                    process.child.try_wait().unwrap().is_none(),
                    "backup child exited before readiness"
                );
                if let Ok(base) = std::fs::read_to_string(&ready)
                    && !base.is_empty()
                {
                    harness.base_url = base;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("backup child readiness timed out");
        process
    }

    pub(super) fn kill_and_wait(&mut self) -> ExitStatus {
        self.child.kill().unwrap();
        self.child.wait().unwrap()
    }
}

impl Drop for BackupProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
