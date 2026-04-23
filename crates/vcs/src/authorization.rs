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
}
