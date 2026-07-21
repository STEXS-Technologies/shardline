use shardline_vcs::RepositoryWebhookEventKind;

use super::ProviderWebhookOutcomeKind;

pub(super) fn duplicate_webhook_event_kind(
    kind: &RepositoryWebhookEventKind,
) -> ProviderWebhookOutcomeKind {
    match kind {
        RepositoryWebhookEventKind::RepositoryDeleted => {
            ProviderWebhookOutcomeKind::RepositoryDeleted
        }
        RepositoryWebhookEventKind::RepositoryRenamed { new_repository } => {
            ProviderWebhookOutcomeKind::RepositoryRenamed {
                new_owner: new_repository.owner().to_owned(),
                new_repo: new_repository.name().to_owned(),
            }
        }
        RepositoryWebhookEventKind::AccessChanged => ProviderWebhookOutcomeKind::AccessChanged,
        RepositoryWebhookEventKind::RevisionPushed { revision } => {
            ProviderWebhookOutcomeKind::RevisionPushed {
                revision: revision.as_str().to_owned(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use shardline_vcs::{ProviderKind, RepositoryRef, RepositoryWebhookEventKind, RevisionRef};

    use super::duplicate_webhook_event_kind;
    use crate::ProviderWebhookOutcomeKind;

    #[test]
    fn duplicate_event_kind_repository_deleted() {
        let kind = RepositoryWebhookEventKind::RepositoryDeleted;
        assert_eq!(
            duplicate_webhook_event_kind(&kind),
            ProviderWebhookOutcomeKind::RepositoryDeleted
        );
    }

    #[test]
    fn duplicate_event_kind_access_changed() {
        let kind = RepositoryWebhookEventKind::AccessChanged;
        assert_eq!(
            duplicate_webhook_event_kind(&kind),
            ProviderWebhookOutcomeKind::AccessChanged
        );
    }

    #[test]
    fn duplicate_event_kind_revision_pushed() {
        let revision = RevisionRef::new("refs/heads/feature").unwrap();
        let kind = RepositoryWebhookEventKind::RevisionPushed { revision };
        assert_eq!(
            duplicate_webhook_event_kind(&kind),
            ProviderWebhookOutcomeKind::RevisionPushed {
                revision: "refs/heads/feature".to_owned(),
            }
        );
    }

    #[test]
    fn duplicate_event_kind_repository_renamed() {
        let new_repository =
            RepositoryRef::new(ProviderKind::GitHub, "new-owner", "new-repo").unwrap();
        let kind = RepositoryWebhookEventKind::RepositoryRenamed { new_repository };
        assert_eq!(
            duplicate_webhook_event_kind(&kind),
            ProviderWebhookOutcomeKind::RepositoryRenamed {
                new_owner: "new-owner".to_owned(),
                new_repo: "new-repo".to_owned(),
            }
        );
    }
}
