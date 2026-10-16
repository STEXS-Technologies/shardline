#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Exercise completed startup in an owned released-server process.
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command},
    time::{Duration, Instant},
};

struct OwnedChild(Child);

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
