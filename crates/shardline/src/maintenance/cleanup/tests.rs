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
