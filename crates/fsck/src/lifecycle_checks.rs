use std::{collections::HashSet, path::Path};

use shardline_index::AsyncIndexStore;
use shardline_protocol::RepositoryScope;
use shardline_server_core::{ServerObjectStore, checked_increment, provider_directory};
use shardline_storage::ObjectStore;

use super::{
    FsckError, FsckIssueDetail, FsckIssueKind, FsckReachability, FsckReport,
    ProviderRepositoryStateTimestampField, WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS,
    object_location_display, push_issue, unix_now_seconds_checked,
};

pub(super) async fn inspect_lifecycle_metadata<IndexAdapter>(
    index_store: &IndexAdapter,
    object_root: &Path,
    object_store: &ServerObjectStore,
    reachability: &FsckReachability,
    report: &mut FsckReport,
) -> Result<(), FsckError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<FsckError>,
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
            Ok::<(), FsckError>(())
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
            Ok::<(), FsckError>(())
        })
        .await?;

    let max_processed_at_unix_seconds = now_unix_seconds
        .checked_add(WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS)
        .ok_or(FsckError::Overflow)?;
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
            Ok::<(), FsckError>(())
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
            Ok::<(), FsckError>(())
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
) -> Result<(), FsckError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn clean_report() -> FsckReport {
        FsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: Vec::new(),
        }
    }

    fn empty_reachability() -> FsckReachability {
        FsckReachability {
            referenced_object_keys: HashSet::new(),
            live_dedupe_chunk_hashes: HashSet::new(),
        }
    }

    fn make_key(path: &str) -> shardline_storage::ObjectKey {
        shardline_storage::ObjectKey::parse(path).unwrap()
    }

    // ── inspect_provider_state_timestamp ────────────────────────────────

    #[test]
    fn provider_state_timestamp_none_is_ok() {
        let mut report = clean_report();
        inspect_provider_state_timestamp(
            &mut report,
            "test/loc",
            ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
            None,
            100,
        )
        .unwrap();
        assert!(report.is_clean());
    }

    #[test]
    fn provider_state_timestamp_within_bounds_is_ok() {
        let mut report = clean_report();
        inspect_provider_state_timestamp(
            &mut report,
            "test/loc",
            ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
            Some(50),
            100,
        )
        .unwrap();
        assert!(report.is_clean());
    }

    #[allow(clippy::panic, clippy::wildcard_enum_match_arm)]
    #[test]
    fn provider_state_timestamp_exceeding_max_creates_issue() {
        let mut report = clean_report();
        inspect_provider_state_timestamp(
            &mut report,
            "test/loc",
            ProviderRepositoryStateTimestampField::LastRevisionPushedAtUnixSeconds,
            Some(200),
            100,
        )
        .unwrap();
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            FsckIssueKind::InvalidProviderRepositoryStateTimestamp
        );
        assert_eq!(report.issues[0].location, "test/loc");
        match &report.issues[0].detail {
            FsckIssueDetail::ProviderRepositoryStateTimestampExceeded {
                field,
                timestamp,
                max_allowed_unix_seconds,
            } => {
                assert_eq!(
                    *field,
                    ProviderRepositoryStateTimestampField::LastRevisionPushedAtUnixSeconds
                );
                assert_eq!(*timestamp, 200);
                assert_eq!(*max_allowed_unix_seconds, 100);
            }
            other => panic!("unexpected detail: {other:?}"),
        }
    }

    // ── inspect_lifecycle_metadata ──────────────────────────────────────

    /// Helper: create stores, run inspect_lifecycle_metadata, return report.
    async fn run_lifecycle_check(
        index_store: &shardline_index::MemoryIndexStore,
        reachability: Option<FsckReachability>,
    ) -> FsckReport {
        let object_root = std::path::Path::new("/tmp");
        let object_store = ServerObjectStore::blackhole();
        let mut report = clean_report();
        let reach = reachability.unwrap_or_else(empty_reachability);
        inspect_lifecycle_metadata(index_store, object_root, &object_store, &reach, &mut report)
            .await
            .unwrap();
        report
    }

    #[tokio::test]
    async fn lifecycle_clean_with_no_data() {
        let store = shardline_index::MemoryIndexStore::new();
        let report = run_lifecycle_check(&store, None).await;
        assert!(report.is_clean());
        assert_eq!(report.inspected_webhook_deliveries, 0);
        assert_eq!(report.inspected_provider_repository_states, 0);
    }

    #[tokio::test]
    async fn quarantine_missing_object_detected() {
        use shardline_index::LifecycleStore;
        let store = shardline_index::MemoryIndexStore::new();
        let obj_key = make_key("ab/1234");
        let candidate =
            shardline_index::QuarantineCandidate::new(obj_key.clone(), 100, 100, 200).unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &candidate).unwrap();
        // blackhole store returns None for metadata => MissingQuarantinedObject
        let report = run_lifecycle_check(&store, None).await;
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            FsckIssueKind::MissingQuarantinedObject
        );
    }

    #[tokio::test]
    async fn quarantined_reachable_object_detected() {
        use shardline_index::LifecycleStore;
        let store = shardline_index::MemoryIndexStore::new();
        let obj_key = make_key("ab/1234");
        let candidate =
            shardline_index::QuarantineCandidate::new(obj_key.clone(), 100, 100, 200).unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &candidate).unwrap();
        let mut reach = empty_reachability();
        reach
            .referenced_object_keys
            .insert("ab/1234".to_owned());
        let report = run_lifecycle_check(&store, Some(reach)).await;
        // 1 missing object (blackhole) + 1 reachable
        assert_eq!(report.issue_count(), 2);
        assert_eq!(
            report.issues[1].kind,
            FsckIssueKind::ReachableQuarantinedObject
        );
    }

    #[tokio::test]
    async fn retention_hold_missing_object_detected() {
        use shardline_index::LifecycleStore;
        let store = shardline_index::MemoryIndexStore::new();
        let obj_key = make_key("ab/1234");
        // Active hold (no release_after = permanently active) with missing object
        let hold = shardline_index::RetentionHold::new(
            obj_key.clone(),
            "test-reason".to_owned(),
            100,
            None, // no release = always active
        )
        .unwrap();
        LifecycleStore::upsert_retention_hold(&store, &hold).unwrap();
        let report = run_lifecycle_check(&store, None).await;
        assert_eq!(report.issue_count(), 1);
        assert_eq!(report.issues[0].kind, FsckIssueKind::MissingHeldObject);
    }

    #[tokio::test]
    async fn held_quarantined_object_detected() {
        use shardline_index::LifecycleStore;
        let store = shardline_index::MemoryIndexStore::new();
        let obj_key = make_key("ab/1234");
        let candidate =
            shardline_index::QuarantineCandidate::new(obj_key.clone(), 100, 100, 200).unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &candidate).unwrap();
        let hold = shardline_index::RetentionHold::new(
            obj_key.clone(),
            "reason".to_owned(),
            100,
            None,
        )
        .unwrap();
        LifecycleStore::upsert_retention_hold(&store, &hold).unwrap();
        let report = run_lifecycle_check(&store, None).await;
        // 1 missing object (quarantine) + 1 held+quarantined
        let held_quarantined = report
            .issues
            .iter()
            .filter(|i| i.kind == FsckIssueKind::HeldQuarantinedObject)
            .count();
        assert_eq!(held_quarantined, 1);
    }

    #[tokio::test]
    async fn webhook_delivery_future_timestamp_detected() {
        use shardline_index::LifecycleStore;
        let store = shardline_index::MemoryIndexStore::new();
        let delivery = shardline_index::WebhookDelivery::new(
            shardline_protocol::RepositoryProvider::GitHub,
            "owner".to_owned(),
            "repo".to_owned(),
            "delivery-1".to_owned(),
            u64::MAX,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        let report = run_lifecycle_check(&store, None).await;
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            FsckIssueKind::InvalidWebhookDeliveryTimestamp
        );
    }

    #[tokio::test]
    async fn provider_state_invalid_identity_detected() {
        use shardline_index::LifecycleStore;
        let store = shardline_index::MemoryIndexStore::new();
        // Empty owner + repo creates an invalid RepositoryScope
        let state = shardline_index::ProviderRepositoryState::new(
            shardline_protocol::RepositoryProvider::GitHub,
            String::new(),
            String::new(),
            None,
            None,
            None,
        );
        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let report = run_lifecycle_check(&store, None).await;
        let count = report
            .issues
            .iter()
            .filter(|i| i.kind == FsckIssueKind::InvalidProviderRepositoryState)
            .count();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn expired_retention_hold_does_not_trigger_missing_object() {
        use shardline_index::LifecycleStore;
        let store = shardline_index::MemoryIndexStore::new();
        let obj_key = make_key("ab/1234");
        // A hold that expires immediately (release_after = 1) is inactive at
        // any realistic current timestamp.
        let hold = shardline_index::RetentionHold::new(
            obj_key.clone(),
            "short hold".to_owned(),
            0,
            Some(1), // release_after = 1 → inactive at current time
        )
        .unwrap();
        LifecycleStore::upsert_retention_hold(&store, &hold).unwrap();
        // Even though the object doesn't exist, the hold is inactive so no issue.
        let report = run_lifecycle_check(&store, None).await;
        assert!(report.is_clean(), "expected no issues for expired hold, got: {report:?}");
    }

    #[tokio::test]
    async fn quarantine_length_mismatch_detected() {
        use shardline_index::LifecycleStore;

        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let object_root = root.join("chunks");

        let index_store = shardline_index::MemoryIndexStore::new();
        let obj_key = make_key("ab/1234");

        // Create the object at the expected key with a specific length
        let content = b"actual object content that is 27 bytes";
        let object_storage_path = object_root.join(obj_key.as_str());
        if let Some(parent) = object_storage_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&object_storage_path, content).unwrap();

        // Create the object store after writing the file
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Create a quarantine candidate with a DIFFERENT observed_length (10 instead of 27)
        let candidate = shardline_index::QuarantineCandidate::new(
            obj_key.clone(),
            10,   // observed_length (different from actual 27)
            100,  // first_seen
            200,  // delete_after
        )
        .unwrap();
        LifecycleStore::upsert_quarantine_candidate(&index_store, &candidate).unwrap();

        let mut report = clean_report();
        let reach = empty_reachability();
        inspect_lifecycle_metadata(
            &index_store,
            &object_root,
            &object_store,
            &reach,
            &mut report,
        )
        .await
        .unwrap();

        assert!(!report.is_clean(), "expected issues");
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            FsckIssueKind::QuarantineLengthMismatch,
            "expected QuarantineLengthMismatch, got: {:#?}",
            report.issues
        );
    }
}
