#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use std::{fs, num::NonZeroUsize, path::Path, process::ExitCode};

fn configuration(root: &Path) -> Configuration {
    Configuration {
        config: ServerConfig::new(
            "127.0.0.1:8080".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.to_owned(),
            NonZeroUsize::new(4096).unwrap(),
        ),
        command: CliCommand::Fsck {
            root: Some(root.to_owned()),
        },
        config_path: None,
        migration: None,
    }
}

#[test]
fn nested_configuration_is_restored_after_errors_and_unwind() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    assert!(config().is_none());
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        scope(configuration(a.path()), || {
            assert_eq!(
                crate::config::load_server_config(None, None)
                    .unwrap()
                    .root_dir(),
                a.path()
            );
            let error: Result<(), &str> = scope(configuration(b.path()), || {
                assert_eq!(
                    crate::config::load_server_config(None, None)
                        .unwrap()
                        .root_dir(),
                    b.path()
                );
                Err("operation failed")
            });
            assert!(error.is_err());
            assert_eq!(config().unwrap().root_dir(), a.path());
            panic!("unwind the outer invocation");
        });
    }));
    assert!(failure.is_err());
    assert!(config().is_none());
    assert!(command().is_none());
}

#[test]
fn parallel_invocations_keep_separate_configurations() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let threads = [a.path(), b.path()].map(|root| {
        let admitted = configuration(root);
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let result = scope(admitted, || {
                barrier.wait();
                let result = crate::config::load_server_config(None, None);
                barrier.wait();
                result
            });
            assert!(config().is_none());
            result.unwrap().root_dir().to_owned()
        })
    });
    barrier.wait();
    let unrelated = config();
    barrier.wait();
    assert!(unrelated.is_none());
    let [first, second] = threads;
    assert_eq!(first.join().unwrap(), a.path());
    assert_eq!(second.join().unwrap(), b.path());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_handler_uses_admitted_command_after_config_and_env_files_change() {
    let root = tempfile::tempdir().unwrap();
    let config_path = root.path().join("selected.toml");
    let env_path = root.path().join("selected.env");
    let mut admitted = configuration(root.path());
    admitted.config_path = Some(config_path.clone());
    // These files were valid when the invocation was admitted. Their replacement
    // must affect the next invocation, not reconfigure the work already admitted.
    fs::write(&config_path, "[server]\nfrontends = [\"s3\"]\n").unwrap();
    fs::write(&env_path, "# valid initial file\n").unwrap();
    fs::write(&config_path, "invalid replacement config").unwrap();
    fs::write(&env_path, "BROKEN=\"unterminated\n").unwrap();
    let arguments: Vec<std::ffi::OsString> = vec![
        "shardline".into(),
        "--config".into(),
        config_path.into_os_string(),
        "--env-file".into(),
        env_path.into_os_string(),
        "fsck".into(),
        "--root".into(),
        root.path().as_os_str().to_owned(),
    ];
    // A fresh parse correctly rejects the replacement; no valid dotenv entries
    // exist to mutate the process environment in this negative control.
    assert_eq!(
        crate::entry::run_direct(arguments.clone().into_iter()).await,
        ExitCode::from(2)
    );
    let (status, restored) = tokio::task::spawn_blocking(move || {
        let status = scope(admitted, || {
            tokio::runtime::Handle::current()
                .block_on(Box::pin(crate::entry::run_direct(arguments.into_iter())))
        });
        (status, config().is_none() && command().is_none())
    })
    .await
    .unwrap();
    assert_eq!(status, ExitCode::SUCCESS);
    assert!(restored);
}

#[test]
fn migration_reads_the_same_endpoints_used_to_pin_its_target() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("original-source");
    let destination = root.path().join("original-destination");
    fs::create_dir_all(&source).unwrap();
    let mut admitted = configuration(root.path());
    admitted.migration = Some((
        StorageMigrationEndpoint::LocalStateRoot(source.clone()),
        StorageMigrationEndpoint::LocalStateRoot(destination.clone()),
    ));
    scope(admitted, || {
        for (side, expected) in [("source", source), ("destination", destination)] {
            let endpoint = crate::storage_migration::endpoint(
                shardline_server::ObjectStorageAdapter::Local,
                Some(Path::new("/different-deployment")),
                side,
            )
            .unwrap();
            assert!(
                matches!(endpoint, StorageMigrationEndpoint::LocalStateRoot(path) if path == expected)
            );
        }
    });
    assert!(endpoint("source").is_none());
}
