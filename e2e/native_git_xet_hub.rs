//! Real Hub control plane and Git transport; no mock forge or LFS negotiator.
use super::*;
use shardline_protocol::{RepositoryScope, TokenClaims};
use shardline_server_core::{AuthProvider, LocalHmacProvider};
use std::sync::atomic::Ordering;

mod fixture;
mod lost_reply;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stock_git_xet_push_and_cold_clone_use_the_real_hub_control_plane() -> Result<(), TestError>
{
    exercise(None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO plus real git-xet"]
async fn stock_git_xet_real_hub_with_postgres_and_minio() -> Result<(), TestError> {
    assert!(
        command_available("git") && command_available("git-lfs") && command_available("git-xet")
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote git-xet qualification requires Docker")?;
    exercise(Some(services)).await
}

async fn exercise(
    services: Option<shardline_test_support::DockerLocalStack>,
) -> Result<(), TestError> {
    if !(command_available("git") && command_available("git-lfs") && command_available("git-xet")) {
        return Ok(());
    }
    let writer = tempfile::tempdir()?;
    let writer_home = tempfile::tempdir()?;
    let reader = tempfile::tempdir()?;
    let reader_home = tempfile::tempdir()?;
    let credential =
        LocalHmacProvider::new(fixture::SIGNING_KEY)?.mint_token(&TokenClaims::new(
            "local",
            "native-hub",
            TokenScope::Write,
            RepositoryScope::new(RepositoryProvider::Generic, "team", "assets", None)?,
            u64::MAX,
        )?)?;
    let counts = fixture::Counts::default();
    let prefix = services
        .as_ref()
        .map(|stack| stack.unique_s3_key_prefix("native-git-xet-hub"))
        .unwrap_or_default();
    let mut node = fixture::HubNode::start(services.as_ref(), &prefix, counts.clone()).await?;
    let mut peer = if services.is_some() {
        Some(fixture::HubNode::start(services.as_ref(), &prefix, counts.clone()).await?)
    } else {
        None
    };
    let base = node.base.clone();
    let reader_base = peer
        .as_ref()
        .map_or_else(|| base.clone(), |node| node.base.clone());
    let fixture::Counts {
        xorbs,
        shards,
        basic,
        downloads,
        lose_git_reply,
        dropped_git_replies,
    } = counts;
    // git-xet obtains its Hub bearer token through Git's credential helper;
    // Git's http.extraHeader alone is not inherited by that token exchange.
    // Keep credentials in disposable, private client homes, never in argv.
    for home in [writer_home.path(), reader_home.path()] {
        let credentials = home.join(".git-credentials");
        let addresses =
            std::iter::once(base.as_str()).chain(peer.as_ref().map(|node| node.base.as_str()));
        let mut entries = String::new();
        for base in addresses {
            let address = base
                .strip_prefix("http://")
                .ok_or("fixture HTTP endpoint")?;
            entries.push_str(&format!("http://native:{credential}@{address}\n"));
        }
        write_file(&credentials, entries.as_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600))?;
        }
        run_git_with_home(
            home,
            home,
            ["config", "--global", "credential.helper", "store"],
        )?;
    }
    let created = Client::new()
        .post(format!("{base}/api/repos/create"))
        .bearer_auth(&credential)
        .json(&json!({"type":"model", "name":"team/assets", "private":true}))
        .send()
        .await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    let remote = format!("{base}/models/team/assets.git");
    initialize_git_xet_push_worktree(
        writer.path(),
        writer_home.path(),
        &remote,
        &format!("{remote}/info/lfs"),
    )?;
    run_git_with_home(
        writer.path(),
        writer_home.path(),
        [
            "config",
            "http.extraHeader",
            &format!("Authorization: Bearer {credential}"),
        ],
    )?;
    // Hub creation already published its initial commit. Build on that history
    // rather than trying to overwrite it with an unrelated local root commit.
    run_git_with_home(
        writer.path(),
        writer_home.path(),
        ["fetch", "origin", "main"],
    )?;
    run_git_with_home(
        writer.path(),
        writer_home.path(),
        ["checkout", "-B", "main", "FETCH_HEAD"],
    )?;
    let expected = seeded_bytes(9 * 1024 * 1024, 67);
    lost_reply::publish_without_reply(
        writer.path(),
        writer_home.path(),
        &expected,
        &lose_git_reply,
        &dropped_git_replies,
    )?;
    assert!(
        xorbs.load(Ordering::Relaxed) > 0,
        "real Hub negotiation must select native Xorb upload"
    );
    assert!(
        shards.load(Ordering::Relaxed) > 0,
        "native shard publication must complete"
    );
    assert_eq!(
        basic.load(Ordering::Relaxed),
        0,
        "basic LFS body upload must not replace Xet"
    );
    let oid = hex::encode(Sha256::digest(&expected));
    let repeated = Client::new()
        .post(format!("{remote}/info/lfs/objects/batch"))
        .bearer_auth(&credential)
        .json(&json!({
            "operation": "upload", "transfers": ["xet"],
            "objects": [{"oid": oid, "size": expected.len()}],
            "ref": {"name": "main"}
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    assert_eq!(repeated["transfer"], "xet");
    assert!(
        repeated["objects"][0]["actions"].is_null(),
        "verified native publication must make the identical upload skippable"
    );
    if peer.is_some() {
        // The independent peer must work after the original HTTP router and
        // its background workers stop, without access to its local scratch.
        node.stop().await?;
        assert!(
            Client::new()
                .get(format!("{base}/health"))
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .is_err(),
            "the original Hub must be unreachable before peer qualification"
        );
        let peer_remote = format!("{reader_base}/models/team/assets.git");
        run_git_with_home(
            writer.path(),
            writer_home.path(),
            ["remote", "set-url", "origin", &peer_remote],
        )?;
        run_git_with_home(
            writer.path(),
            writer_home.path(),
            ["config", "lfs.url", &format!("{peer_remote}/info/lfs")],
        )?;
    }
    lost_reply::retry_committed(writer.path(), writer_home.path(), &xorbs, &shards)?;
    let downloads_before = downloads.load(Ordering::Relaxed);
    create_dir_all(reader_home.path().join(".config"))?;
    let mut install = Command::new("git-xet");
    run_command_checked(
        with_test_home(&mut install, reader_home.path()).args(["install"]),
        "cold git-xet install",
    )?;
    let destination = reader.path().join("clone");
    let mut clone = Command::new("git");
    let model_remote = format!("{reader_base}/team/assets.git");
    run_command_checked(
        with_test_home(&mut clone, reader_home.path())
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "http.extraHeader")
            .env(
                "GIT_CONFIG_VALUE_0",
                format!("Authorization: Bearer {credential}"),
            )
            .args(["clone", &model_remote])
            .arg(&destination),
        "cold Hub git-xet clone",
    )?;
    assert_eq!(read(destination.join("asset.bin"))?, expected);
    // Stock git-xet implements upload only and delegates downloads to Git LFS.
    // Serve native publication here without requiring a duplicate basic upload.
    assert!(
        downloads.load(Ordering::Relaxed) > downloads_before,
        "cold clone must download the native publication through standard Git LFS"
    );
    let first_commit =
        git_stdout_with_home(&destination, reader_home.path(), ["rev-parse", "HEAD"])?;
    let updated = mutated_bytes(&expected, expected.len() - 256 * 1024, 256 * 1024, 0xa5);
    assert_ne!(updated, expected);
    let shards_before = shards.load(Ordering::Relaxed);
    write_commit_and_push_asset(
        writer.path(),
        writer_home.path(),
        &updated,
        "incremental native Hub Xet upload",
    )?;
    let second_commit =
        git_stdout_with_home(writer.path(), writer_home.path(), ["rev-parse", "HEAD"])?;
    assert_ne!(first_commit, second_commit);
    assert!(
        shards.load(Ordering::Relaxed) > shards_before,
        "the changed file must publish through native Xet"
    );
    // The clone's one-command authorization header is not persisted by Git.
    // Keep subsequent requests authenticated in this disposable worktree.
    run_git_with_home(
        &destination,
        reader_home.path(),
        [
            "config",
            "http.extraHeader",
            &format!("Authorization: Bearer {credential}"),
        ],
    )?;
    run_git_with_home(
        &destination,
        reader_home.path(),
        ["fetch", "origin", "main"],
    )?;
    assert_eq!(
        git_stdout_with_home(
            &destination,
            reader_home.path(),
            ["rev-parse", "origin/main"],
        )?,
        second_commit,
    );
    assert_eq!(read(destination.join("asset.bin"))?, expected);
    run_git_with_home(&destination, reader_home.path(), ["pull", "--ff-only"])?;
    assert_eq!(read(destination.join("asset.bin"))?, updated);
    assert_eq!(
        git_stdout_with_home(&destination, reader_home.path(), ["rev-parse", "HEAD"])?,
        second_commit,
    );
    run_git_with_home(
        &destination,
        reader_home.path(),
        ["checkout", "--detach", &first_commit],
    )?;
    assert_eq!(read(destination.join("asset.bin"))?, expected);
    run_git_with_home(&destination, reader_home.path(), ["checkout", "main"])?;
    assert_eq!(read(destination.join("asset.bin"))?, updated);
    assert_eq!(
        basic.load(Ordering::Relaxed),
        0,
        "incremental native Xet publication must not fall back to basic LFS upload"
    );
    run_git_with_home(&destination, reader_home.path(), ["fsck", "--strict"])?;
    if let Some(peer) = peer.as_mut() {
        peer.stop().await?;
    } else {
        node.stop().await?;
    }
    Ok(())
}
