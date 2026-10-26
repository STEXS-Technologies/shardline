//! Two stock client generations recover against independent replacement replicas.

use super::*;

pub(super) struct Peer {
    process: Process,
    _server: Process,
    _root: tempfile::TempDir,
    home: tempfile::TempDir,
}

impl Peer {
    pub(super) async fn start(
        python: &Path,
        source: &Path,
        token: &str,
        environment: &[(&str, String)],
    ) -> Result<Self> {
        assert!(
            !environment.is_empty(),
            "mixed clients require shared remote state"
        );
        let root = tempfile::tempdir()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        drop(listener);
        let base = format!("http://{address}");
        let mut server = start_with_environment(root.path(), address, None, environment)?;
        ready(&reqwest::Client::new(), &base, &mut server).await?;
        let home = tempfile::tempdir()?;
        let process = client_with_program(python, source, &base, token, home.path())?;
        Ok(Self {
            process,
            _server: server,
            _root: root,
            home,
        })
    }

    pub(super) async fn verify(&mut self, expected: &serde_json::Value) -> Result {
        let status = tokio::time::timeout(Duration::from_secs(240), async {
            loop {
                if let Some(status) = self.process.0.try_wait()? {
                    return Ok::<_, std::io::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        assert!(
            status.success(),
            "mixed-client retry failed: {status}; credential-bearing client output omitted"
        );
        let actual: serde_json::Value =
            serde_json::from_slice(&std::fs::read(self.home.path().join("result.json"))?)?;
        // Clients may choose different legal container boundaries. Their
        // reconstructed bytes must agree, independently of their wire layout.
        for field in ["size", "sha256"] {
            assert_eq!(
                actual.get(field),
                expected.get(field),
                "mixed-client {field}"
            );
        }
        Ok(())
    }
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and SHARDLINE_NATIVE_PEER_PYTHON with hf_xet 1.5.2"]
async fn stock_xet_large_mixed_clients_recover_with_postgres_and_minio() -> Result {
    qualify_mixed_clients("1.5.2").await
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and SHARDLINE_NATIVE_PEER_PYTHON with hf_xet 1.6.0"]
async fn stock_xet_16_mixed_clients_recover_with_postgres_and_minio() -> Result {
    qualify_mixed_clients("1.6.0").await
}

#[cfg(feature = "docker")]
async fn qualify_mixed_clients(peer_version: &str) -> Result {
    let peer = std::env::var_os("SHARDLINE_NATIVE_PEER_PYTHON")
        .ok_or("mixed-client recovery requires an explicit peer Python environment")?;
    let peer = std::path::PathBuf::from(peer);
    for (python, expected) in [
        (Path::new("python"), "1.7.0"),
        (peer.as_path(), peer_version),
    ] {
        let output = shardline_test_support::native_client_output(
            Command::new(python).args([
                "-c",
                "from importlib.metadata import version; from hf_xet import XetSession; print(version('hf_xet'))",
            ]),
            Duration::from_secs(10),
        )?;
        assert!(output.status.success(), "pinned stock client must import");
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), expected);
    }
    for cut in [
        "created",
        "storing",
        "object",
        "stored",
        "committed",
        "visible",
    ] {
        let remote = super::super::native_upload_remote::RemoteFixture::start().await?;
        let root = tempfile::tempdir()?;
        let result = interrupted_client(
            root.path(),
            shardline_index::PostgresIndexStore::new(remote.pool.clone()),
            &remote.environment,
            cut,
            Some(&peer),
        )
        .await;
        remote.pool.close().await;
        result?;
    }
    Ok(())
}
