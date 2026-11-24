//! Common protocol guarantees across the immediately preceding release.
//!
//! Historical 1.12.1 tests separately assert feature and identity transitions
//! specific to that release. They cannot qualify a different predecessor.
use super::*;

#[tokio::test]
async fn adjacent_release_preserves_s3_bytes_across_upgrade_and_rollback() {
    let Some(old) = resolve_n_minus_one_binary("adjacent release upgrade") else {
        return;
    };
    let current = resolve_shardline_binary().expect("current binary required");
    let root = TempDir::new().unwrap();
    let rollback_root = TempDir::new().unwrap();
    let token = mint_token("drill", "drill", TokenScope::Write);
    let before = ("before", deterministic_bytes(131_079, 971));
    let after = ("after", deterministic_bytes(196_619, 972));
    let rollback = ("rollback", deterministic_bytes(65_539, 973));

    migrate(&old, root.path());
    let mut previous = LocalServer::spawn(&old, root.path());
    previous.ready().await;
    let response = s3_put(&previous.base, &token, before.0, before.1.clone()).await;
    assert!(
        response.status().is_success(),
        "seed: {}",
        response.text().await.unwrap()
    );
    drop(previous);

    // A rollback uses the matching pre-upgrade backup. Copy only after the old
    // writer has stopped so SQLite state and object bytes form one snapshot.
    copy_offline_root(root.path(), rollback_root.path());

    // The candidate reads predecessor data after the explicit forward migration.
    // Stop writers before the explicit migration. This does not certify a
    // zero-downtime schema upgrade or infer that every old reader is retired.
    migrate(&current, root.path());
    let mut upgraded = LocalServer::spawn(&current, root.path());
    upgraded.ready().await;
    assert_s3_bytes(
        &upgraded.base,
        &token,
        before.0,
        &before.1,
        "upgraded reader",
    )
    .await;
    drop(upgraded);

    // The released predecessor must fail closed on the upgraded schema. The
    // supported rollback restores the matching backup into an isolated root.
    let mut previous = LocalServer::spawn(&old, root.path());
    previous
        .rejected("local frontend schema is incompatible")
        .await;
    drop(previous);

    let mut previous = LocalServer::spawn(&old, rollback_root.path());
    previous.ready().await;
    assert_s3_bytes(
        &previous.base,
        &token,
        before.0,
        &before.1,
        "restored backup reader",
    )
    .await;
    let response = s3_put(&previous.base, &token, rollback.0, rollback.1.clone()).await;
    assert!(
        response.status().is_success(),
        "restored backup writer: {}",
        response.text().await.unwrap()
    );
    assert_s3_bytes(
        &previous.base,
        &token,
        rollback.0,
        &rollback.1,
        "restored backup writer readback",
    )
    .await;
    drop(previous);

    // Once current-only writes begin, rollback requires forward recovery. The
    // upgraded server can restart and read both old and new object bytes.
    let mut upgraded = LocalServer::spawn(&current, root.path());
    upgraded.ready().await;
    let response = s3_put(&upgraded.base, &token, after.0, after.1.clone()).await;
    assert!(
        response.status().is_success(),
        "new writer after cutover: {}",
        response.text().await.unwrap()
    );
    drop(upgraded);

    let mut upgraded = LocalServer::spawn(&current, root.path());
    upgraded.ready().await;
    for (key, bytes) in [&before, &after] {
        assert_s3_bytes(&upgraded.base, &token, key, bytes, "forward restart").await;
    }
}
