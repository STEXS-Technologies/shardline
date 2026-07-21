use crate::{ProviderSubject, RepositoryAccess, RepositoryRef, RevisionRef};

/// Provider authorization request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRequest {
    subject: ProviderSubject,
    repository: RepositoryRef,
    revision: RevisionRef,
    access: RepositoryAccess,
}

impl AuthorizationRequest {
    /// Creates an authorization request.
    #[must_use]
    pub const fn new(
        subject: ProviderSubject,
        repository: RepositoryRef,
        revision: RevisionRef,
        access: RepositoryAccess,
    ) -> Self {
        Self {
            subject,
            repository,
            revision,
            access,
        }
    }

    /// Returns the authenticated provider subject.
    #[must_use]
    pub const fn subject(&self) -> &ProviderSubject {
        &self.subject
    }

    /// Returns the repository reference.
    #[must_use]
    pub const fn repository(&self) -> &RepositoryRef {
        &self.repository
    }

    /// Returns the revision reference.
    #[must_use]
    pub const fn revision(&self) -> &RevisionRef {
        &self.revision
    }

    /// Returns the requested access level.
    #[must_use]
    pub const fn access(&self) -> RepositoryAccess {
        self.access
    }
}

#[cfg(test)]
mod tests {
    use super::AuthorizationRequest;
    use crate::{ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef, RevisionRef};

    #[test]
    fn authorization_request_keeps_subject_repository_revision_and_access() {
        let subject = ProviderSubject::new("gitlab-user-42");
        let repository = RepositoryRef::new(ProviderKind::GitLab, "team", "assets");
        let revision = RevisionRef::new("main");

        assert!(subject.is_ok());
        assert!(repository.is_ok());
        assert!(revision.is_ok());

        let (Ok(subject), Ok(repository), Ok(revision)) = (subject, repository, revision) else {
            return;
        };
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Write);

        assert_eq!(request.subject().as_str(), "gitlab-user-42");
        assert_eq!(request.repository().provider(), ProviderKind::GitLab);
        assert_eq!(request.revision().as_str(), "main");
        assert_eq!(request.access(), RepositoryAccess::Write);
    }

    #[test]
    fn authorization_request_works_with_read_access() {
        let subject = ProviderSubject::new("user-1").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap();
        let revision = RevisionRef::new("develop").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);

        assert_eq!(request.access(), RepositoryAccess::Read);
    }

    #[test]
    fn authorization_request_clone_and_eq() {
        let subject = ProviderSubject::new("user-1").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap();
        let revision = RevisionRef::new("main").unwrap();

        let a = AuthorizationRequest::new(
            subject.clone(),
            repository.clone(),
            revision.clone(),
            RepositoryAccess::Read,
        );
        let b = AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);

        assert_eq!(a, b);
    }

    #[test]
    fn authorization_request_debug_format() {
        let subject = ProviderSubject::new("user-1").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap();
        let revision = RevisionRef::new("main").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);

        let debug = format!("{request:?}");
        assert!(debug.contains("AuthorizationRequest"));
        assert!(debug.contains("Read") || debug.contains("read"));
    }

    #[test]
    fn authorization_request_different_subjects_unequal() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap();
        let rev = RevisionRef::new("main").unwrap();

        let a = AuthorizationRequest::new(
            ProviderSubject::new("alice").unwrap(),
            repo.clone(),
            rev.clone(),
            RepositoryAccess::Read,
        );
        let b = AuthorizationRequest::new(
            ProviderSubject::new("bob").unwrap(),
            repo,
            rev,
            RepositoryAccess::Read,
        );

        assert_ne!(a, b);
    }

    #[test]
    fn authorization_request_different_repositories_unequal() {
        let subject = ProviderSubject::new("user").unwrap();
        let rev = RevisionRef::new("main").unwrap();

        let a = AuthorizationRequest::new(
            subject.clone(),
            RepositoryRef::new(ProviderKind::GitHub, "org", "repo-a").unwrap(),
            rev.clone(),
            RepositoryAccess::Read,
        );
        let b = AuthorizationRequest::new(
            subject,
            RepositoryRef::new(ProviderKind::GitHub, "org", "repo-b").unwrap(),
            rev,
            RepositoryAccess::Read,
        );

        assert_ne!(a, b);
    }

    #[test]
    fn authorization_request_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        let subject = ProviderSubject::new("user").unwrap();
        let repo = RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap();
        let rev = RevisionRef::new("main").unwrap();
        let request = AuthorizationRequest::new(subject, repo, rev, RepositoryAccess::Read);

        assert_send::<AuthorizationRequest>();
        assert_sync::<AuthorizationRequest>();
        // keep request alive
        let _ = request;
    }
}
