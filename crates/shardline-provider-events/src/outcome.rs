use shardline_vcs::{RepositoryWebhookEvent, RepositoryWebhookEventKind};

use super::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind};

impl ProviderWebhookOutcome {
    pub(crate) fn for_event(event: &RepositoryWebhookEvent) -> Self {
        Self {
            provider: event.repository().provider(),
            owner: event.repository().owner().to_owned(),
            repo: event.repository().name().to_owned(),
            delivery_id: event.delivery_id().as_str().to_owned(),
            event_kind: event_kind(event.kind()),
            affected_file_versions: 0,
            affected_chunks: 0,
            applied_holds: 0,
            retention_seconds: None,
        }
    }
}

fn event_kind(kind: &RepositoryWebhookEventKind) -> ProviderWebhookOutcomeKind {
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
