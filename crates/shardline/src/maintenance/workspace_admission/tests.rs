#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

#[test]
fn full_archive_preserves_existing_workspaces_and_operator_files() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    for id in 0..2 {
        let directory = state.join(format!("{id:064x}"));
        ensure_with_limit(&state, &directory, 2).unwrap();
        std::fs::write(directory.join("journal"), b"unfinished evidence").unwrap();
    }
    std::fs::write(state.join("operator-file"), b"preserve").unwrap();
    let next = state.join(format!("{:064x}", 2));
    assert!(ensure_with_limit(&state, &next, 2).is_err());
    assert!(!next.exists());
    ensure_with_limit(&state, &state.join(format!("{:064x}", 0)), 2).unwrap();
    for id in 0..2 {
        assert_eq!(
            std::fs::read(state.join(format!("{id:064x}")).join("journal")).unwrap(),
            b"unfinished evidence"
        );
    }
    assert_eq!(
        std::fs::read(state.join("operator-file")).unwrap(),
        b"preserve"
    );
}

#[test]
fn unrelated_entries_are_bounded_without_following_or_removing_them() {
    let root = tempfile::tempdir().unwrap();
    for id in 0..8 {
        std::fs::write(root.path().join(format!("operator-{id}")), b"preserve").unwrap();
    }
    let next = root.path().join(format!("{:064x}", 0));
    assert!(ensure_with_limit(root.path(), &next, 2).is_err());
    assert!(!next.exists());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 8);
}

#[test]
fn concurrent_admissions_share_the_existing_directory_lock() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    artifact::private_directory(&state).unwrap();
    let workers: Vec<_> = (0..16)
        .map(|id| {
            let state = state.clone();
            std::thread::spawn(move || {
                ensure_with_limit(&state, &state.join(format!("{id:064x}")), 4).is_ok()
            })
        })
        .collect();
    for worker in workers {
        let _admitted = worker.join().unwrap();
    }
    assert!(std::fs::read_dir(&state).unwrap().count() <= 4);
    let lease = File::open(&state).unwrap();
    lease.try_lock().unwrap();
    let next = state.join(format!("{:064x}", 16));
    assert!(ensure_with_limit(&state, &next, 4).is_err());
    assert!(!next.exists());
}
