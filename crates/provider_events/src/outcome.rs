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
