#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Exercise completed startup in an owned released-server process.
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command},
    time::{Duration, Instant},
};

struct OwnedChild(Child);

/// Missing released binaries may skip developer runs, never qualification.
#[track_caller]
pub(crate) fn binary(variable: &str) -> Option<String> {
    let required = shardline_test_support::native_clients_required()
        || std::env::var_os("SHARDLINE_REQUIRE_POSTGRES_TESTS").is_some_and(|value| value == "1");
    resolve_binary(variable, std::env::var(variable), required)
}

#[track_caller]
fn resolve_binary(
    variable: &str,
    value: Result<String, std::env::VarError>,
    required: bool,
) -> Option<String> {
    if !required && matches!(&value, Err(std::env::VarError::NotPresent)) {
        eprintln!("SKIPPED: optional released recovery binary {variable} is not configured");
        return None;
    }
    assert!(
        value.as_ref().is_ok_and(|path| !path.trim().is_empty()),
        "released recovery binary {variable} is missing or invalid"
    );
    value.ok()
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub(crate) async fn run(binary: PathBuf, root: PathBuf, database_url: Option<String>) -> String {
    tokio::task::spawn_blocking(move || {
        let log = tempfile::NamedTempFile::new().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut command = Command::new(binary);
        command
            .env_clear()
            .current_dir(&root)
            .env("SHARDLINE_ROOT_DIR", &root)
            .env("SHARDLINE_BIND_ADDR", address.to_string())
            .env("RUST_LOG", "debug")
            .env("SHARDLINE_AUTH_PROVIDER", "local")
            .env(
                "SHARDLINE_TOKEN_SIGNING_KEY",
                "0123456789abcdef0123456789abcdef",
            )
            .arg("serve")
            .stdout(log.as_file().try_clone().unwrap())
            .stderr(log.as_file().try_clone().unwrap());
        if let Some(database_url) = database_url {
            command.env("SHARDLINE_INDEX_POSTGRES_URL", database_url);
        }
        let mut child = OwnedChild(command.spawn().unwrap());
        let started_at = Instant::now();
        let mut ready = false;
        while started_at.elapsed() < Duration::from_secs(15) {
            if let Ok(mut connection) =
                std::net::TcpStream::connect_timeout(&address, Duration::from_millis(100))
            {
                connection
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                connection
                    .set_write_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                if connection
                    .write_all(
                        b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                    )
                    .is_ok()
                {
                    let mut status = String::new();
                    if BufReader::new(connection).read_line(&mut status).is_ok()
                        && status.starts_with("HTTP/1.1 200 ")
                    {
                        ready = true;
                        break;
                    }
                }
            }
            if child.0.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        drop(child);
        let logs = std::fs::read_to_string(log.path()).unwrap();
        assert!(ready, "released server did not finish startup: {logs}");
        logs
    })
    .await
    .unwrap()
}

#[cfg(test)]
mod prerequisites {
    use super::resolve_binary;

    #[test]
    fn missing_binary_only_skips_optional_runs() {
        assert!(
            resolve_binary("TEST_BINARY", Err(std::env::VarError::NotPresent), false).is_none()
        );
        assert!(
            std::panic::catch_unwind(|| {
                resolve_binary("TEST_BINARY", Err(std::env::VarError::NotPresent), true)
            })
            .is_err()
        );
    }

    #[test]
    fn invalid_configured_binary_never_skips_or_discloses_its_value() {
        for value in [
            Ok(" ".to_owned()),
            Err(std::env::VarError::NotUnicode("private-binary-path".into())),
        ] {
            let failure = std::panic::catch_unwind(|| resolve_binary("TEST_BINARY", value, false))
                .expect_err("invalid configured binary must fail");
            let message = failure.downcast_ref::<String>().unwrap();
            assert_eq!(
                message,
                "released recovery binary TEST_BINARY is missing or invalid"
            );
        }
    }
}
