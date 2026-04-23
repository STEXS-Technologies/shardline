#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_vcs::{ProviderKind, RepositoryRef, RevisionRef};

fuzz_target!(|data: (String, String, String)| {
    let (owner, name, revision) = data;

    // INVARIANT 1: Repository reference validation is deterministic.
    let first_repository = RepositoryRef::new(ProviderKind::Generic, owner.as_str(), name.as_str());
    let second_repository =
        RepositoryRef::new(ProviderKind::Generic, owner.as_str(), name.as_str());
    assert_eq!(first_repository, second_repository);

    if let Ok(repository) = first_repository {
        assert_eq!(repository.provider(), ProviderKind::Generic);
        assert_eq!(repository.owner(), owner);
        assert_eq!(repository.name(), name);
        assert!(!repository.owner().trim().is_empty());
        assert!(!repository.name().trim().is_empty());
        assert!(!repository.owner().chars().any(char::is_control));
        assert!(!repository.name().chars().any(char::is_control));
    }

    // INVARIANT 2: Revision reference validation is deterministic.
    let first_revision = RevisionRef::new(revision.as_str());
    let second_revision = RevisionRef::new(revision.as_str());
    assert_eq!(first_revision, second_revision);

    if let Ok(reference) = first_revision {
        assert_eq!(reference.as_str(), revision);
        assert!(!reference.as_str().trim().is_empty());
        assert!(!reference.as_str().chars().any(char::is_control));
    }
});
