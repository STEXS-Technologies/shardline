#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;

#[test]
fn retired_cleanup_is_bounded_and_never_removes_unfinished_state() {
    let fixture = tempfile::tempdir().unwrap();
    let state = fixture.path().join("private-state");
    let retired = state.join(RETIRED);
    artifact::private_directory(&retired).unwrap();
    let unfinished = state.join(format!("{:064x}", 999));
    artifact::private_directory(&unfinished).unwrap();
    fs::write(unfinished.join("identity.json"), b"unfinished evidence").unwrap();
    for task in 0..66 {
        let directory = retired.join(format!("{task:064x}"));
        artifact::private_directory(&directory).unwrap();
        fs::write(directory.join("remaining-scratch"), b"already confirmed").unwrap();
    }
    retry(&state).unwrap();
    let remaining = fs::read_dir(&retired)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_dir())
        .count();
    assert_eq!(remaining, 2);
    retry(&state).unwrap();
    assert_eq!(fs::read_dir(&retired).unwrap().count(), 0);
    assert_eq!(
        fs::read(unfinished.join("identity.json")).unwrap(),
        b"unfinished evidence"
    );
}

#[test]
fn retirement_never_overwrites_an_existing_queue_entry() {
    let fixture = tempfile::tempdir().unwrap();
    let state = fixture.path().join("private-state");
    let name = format!("{:064x}", 1);
    let directory = state.join(&name);
    let queued = state.join(RETIRED).join(&name);
    artifact::private_directory(&directory).unwrap();
    artifact::private_directory(&queued).unwrap();
    fs::write(directory.join("identity.json"), b"original history").unwrap();
    assert!(completed(&directory).is_err());
    assert_eq!(
        fs::read(directory.join("identity.json")).unwrap(),
        b"original history"
    );
    assert!(queued.is_dir());
}

#[test]
fn retirement_reuses_export_lease_and_rejects_other_archives() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let directory = state.join(format!("{:064x}", 0));
    artifact::private_directory(&directory).unwrap();
    fs::write(directory.join("scratch"), b"verified scratch").unwrap();
    let batch = outbox::Batch::open(&state).unwrap().unwrap();
    assert!(completed(&directory).is_err());
    assert_eq!(
        fs::read(directory.join("scratch")).unwrap(),
        b"verified scratch"
    );
    let foreign = root.path().join("other").join(format!("{:064x}", 1));
    artifact::private_directory(&foreign).unwrap();
    fs::write(foreign.join("scratch"), b"preserve").unwrap();
    assert!(batch.retire_verified(&foreign).is_err());
    assert_eq!(fs::read(foreign.join("scratch")).unwrap(), b"preserve");
    batch.retire_verified(&directory).unwrap();
    assert!(!directory.exists());
}

#[test]
fn replacing_the_archive_root_invalidates_its_lease() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    artifact::private_directory(&state).unwrap();
    let lease = ArchiveLease::try_open(&state).unwrap().unwrap();
    fs::rename(&state, root.path().join("old-state")).unwrap();
    let directory = state.join(format!("{:064x}", 0));
    artifact::private_directory(&directory).unwrap();
    fs::write(directory.join("scratch"), b"new archive evidence").unwrap();
    assert!(completed_with_lease(&directory, &lease).is_err());
    assert_eq!(
        fs::read(directory.join("scratch")).unwrap(),
        b"new archive evidence"
    );
}

#[test]
fn verified_retirement_does_not_recreate_already_removed_scratch() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    artifact::private_directory(&state).unwrap();
    let missing = state.join(format!("{:064x}", 0));
    assert!(artifact::existing_private_directory(&missing).is_err());
    assert!(!missing.exists());
    completed(&missing).unwrap();
    assert!(!missing.exists());
    assert_eq!(fs::read_dir(&state).unwrap().count(), 0);
}
