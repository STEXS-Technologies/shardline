use std::{collections::HashSet, path::Path};

use shardline_index::AsyncIndexStore;
use shardline_protocol::RepositoryScope;

use super::{
    FsckIssueDetail, FsckIssueKind, FsckReachability, FsckReport,
    ProviderRepositoryStateTimestampField, WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS,
    object_location_display, push_issue,
};
use crate::{
    ServerError, clock::unix_now_seconds_checked, object_store::ServerObjectStore,
    overflow::checked_increment, repository_scope_path::provider_directory,
};

pub(super) async fn inspect_lifecycle_metadata<IndexAdapter>(
    index_store: &IndexAdapter,
    object_root: &Path,
    object_store: &ServerObjectStore,
    reachability: &FsckReachability,
    report: &mut FsckReport,
) -> Result<(), ServerError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    let now_unix_seconds = unix_now_seconds_checked()?;
    let mut quarantined_object_keys = HashSet::new();

    index_store
        .visit_quarantine_candidates(|candidate| {
            let object_key = candidate.object_key();
            let object_key_string = object_key.as_str().to_owned();
            quarantined_object_keys.insert(object_key_string.clone());
            let location = object_location_display(object_root, object_store, object_key);

            if candidate.delete_after_unix_seconds()
                < candidate.first_seen_unreachable_at_unix_seconds()
            {
                push_issue(
                    report,
                    FsckIssueKind::InvalidQuarantineCandidate,
                    location.clone(),
                    FsckIssueDetail::InvalidQuarantineTimeline {
                        delete_after_unix_seconds: candidate.delete_after_unix_seconds(),
                        first_seen_unreachable_at_unix_seconds: candidate
                            .first_seen_unreachable_at_unix_seconds(),
                    },
                )?;
            }

            match object_store.metadata(object_key)? {
                Some(metadata) => {
                    if metadata.length() != candidate.observed_length() {
                        push_issue(
                            report,
                            FsckIssueKind::QuarantineLengthMismatch,
                            location.clone(),
                            FsckIssueDetail::LengthMismatch {
                                expected_length: candidate.observed_length(),
                                observed_length: metadata.length(),
                            },
                        )?;
                    }
                }
                None => {
                    push_issue(
                        report,
                        FsckIssueKind::MissingQuarantinedObject,
                        location.clone(),
                        FsckIssueDetail::QuarantineReferencedMissingObject,
                    )?;
                }
            }

            if reachability
                .referenced_object_keys
                .contains(&object_key_string)
            {
                push_issue(
                    report,
                    FsckIssueKind::ReachableQuarantinedObject,
                    location,
                    FsckIssueDetail::QuarantineTargetedReachableObject,
                )?;
            }
            Ok::<(), ServerError>(())
        })
        .await?;

    index_store
        .visit_retention_holds(|hold| {
            let object_key = hold.object_key();
            let object_key_string = object_key.as_str().to_owned();
            let location = object_location_display(object_root, object_store, object_key);

            if let Some(release_after_unix_seconds) = hold.release_after_unix_seconds()
                && release_after_unix_seconds < hold.held_at_unix_seconds()
            {
                push_issue(
                    report,
                    FsckIssueKind::InvalidRetentionHold,
                    location.clone(),
                    FsckIssueDetail::InvalidRetentionTimeline {
                        release_after_unix_seconds,
                        held_at_unix_seconds: hold.held_at_unix_seconds(),
                    },
                )?;
            }

            if hold.is_active_at(now_unix_seconds) {
                if object_store.metadata(object_key)?.is_none() {
                    push_issue(
                        report,
                        FsckIssueKind::MissingHeldObject,
                        location.clone(),
                        FsckIssueDetail::ActiveRetentionHoldReason {
                            reason: hold.reason().to_owned(),
                        },
                    )?;
                }
                if quarantined_object_keys.contains(&object_key_string) {
                    push_issue(
                        report,
                        FsckIssueKind::HeldQuarantinedObject,
                        location,
                        FsckIssueDetail::ActiveRetentionHoldQuarantined,
                    )?;
                }
            }
            Ok::<(), ServerError>(())
        })
        .await?;

    let max_processed_at_unix_seconds = now_unix_seconds
        .checked_add(WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS)
        .ok_or(ServerError::Overflow)?;
    let max_provider_state_timestamp = max_processed_at_unix_seconds;
    index_store
        .visit_webhook_deliveries(|delivery| {
            report.inspected_webhook_deliveries =
                checked_increment(report.inspected_webhook_deliveries)?;
            if delivery.processed_at_unix_seconds() > max_processed_at_unix_seconds {
                push_issue(
                    report,
                    FsckIssueKind::InvalidWebhookDeliveryTimestamp,
                    format!(
                        "{}/{}",
                        provider_directory(delivery.provider()),
                        delivery.delivery_id()
                    ),
                    FsckIssueDetail::WebhookDeliveryTimestampExceeded {
                        processed_at_unix_seconds: delivery.processed_at_unix_seconds(),
                        max_allowed_unix_seconds: max_processed_at_unix_seconds,
                    },
                )?;
            }
            Ok::<(), ServerError>(())
        })
        .await?;

    index_store
        .visit_provider_repository_states(|state| {
            report.inspected_provider_repository_states =
                checked_increment(report.inspected_provider_repository_states)?;
            let location = format!(
                "{}/{}/{}",
                provider_directory(state.provider()),
                state.owner(),
                state.repo()
            );
            if RepositoryScope::new(state.provider(), state.owner(), state.repo(), None).is_err() {
                push_issue(
                    report,
                    FsckIssueKind::InvalidProviderRepositoryState,
                    location.clone(),
                    FsckIssueDetail::ProviderRepositoryIdentityInvalid,
                )?;
            }
            inspect_provider_state_timestamp(
                report,
                &location,
                ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
                state.last_access_changed_at_unix_seconds(),
                max_provider_state_timestamp,
            )?;
            inspect_provider_state_timestamp(
                report,
                &location,
                ProviderRepositoryStateTimestampField::LastRevisionPushedAtUnixSeconds,
                state.last_revision_pushed_at_unix_seconds(),
                max_provider_state_timestamp,
            )?;
            inspect_provider_state_timestamp(
                report,
                &location,
                ProviderRepositoryStateTimestampField::LastCacheInvalidatedAtUnixSeconds,
                state.last_cache_invalidated_at_unix_seconds(),
                max_provider_state_timestamp,
            )?;
            inspect_provider_state_timestamp(
                report,
                &location,
                ProviderRepositoryStateTimestampField::LastAuthorizationRecheckedAtUnixSeconds,
                state.last_authorization_rechecked_at_unix_seconds(),
                max_provider_state_timestamp,
            )?;
            inspect_provider_state_timestamp(
                report,
                &location,
                ProviderRepositoryStateTimestampField::LastDriftCheckedAtUnixSeconds,
                state.last_drift_checked_at_unix_seconds(),
                max_provider_state_timestamp,
            )?;
            Ok::<(), ServerError>(())
        })
        .await?;

    Ok(())
}

fn inspect_provider_state_timestamp(
    report: &mut FsckReport,
    location: &str,
    field: ProviderRepositoryStateTimestampField,
    timestamp: Option<u64>,
    max_allowed_unix_seconds: u64,
) -> Result<(), ServerError> {
    let Some(timestamp) = timestamp else {
        return Ok(());
    };
    if timestamp > max_allowed_unix_seconds {
        push_issue(
            report,
            FsckIssueKind::InvalidProviderRepositoryStateTimestamp,
            location.to_owned(),
            FsckIssueDetail::ProviderRepositoryStateTimestampExceeded {
                field,
                timestamp,
                max_allowed_unix_seconds,
            },
        )?;
    }

    Ok(())
}
