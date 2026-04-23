#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_server::{FuzzLifecycleRepairSummary, fuzz_lifecycle_repair_summary};

const MAX_CASES: usize = 256;

type QuarantineState = (bool, bool, bool);
type RetentionState = (Option<u64>, u64, bool);

fuzz_target!(|data: (
    u64,
    u64,
    Vec<QuarantineState>,
    Vec<RetentionState>,
    Vec<u64>,
)| {
    let (
        now_unix_seconds,
        webhook_retention_seconds,
        quarantine_states,
        retention_states,
        webhook_processed_at_unix_seconds,
    ) = data;

    let quarantine_states = quarantine_states
        .into_iter()
        .take(MAX_CASES)
        .collect::<Vec<_>>();
    let retention_states = retention_states
        .into_iter()
        .take(MAX_CASES)
        .collect::<Vec<_>>();
    let webhook_processed_at_unix_seconds = webhook_processed_at_unix_seconds
        .into_iter()
        .take(MAX_CASES)
        .collect::<Vec<_>>();

    let first = summarize(
        now_unix_seconds,
        webhook_retention_seconds,
        quarantine_states.as_slice(),
        retention_states.as_slice(),
        webhook_processed_at_unix_seconds.as_slice(),
    );
    let second = summarize(
        now_unix_seconds,
        webhook_retention_seconds,
        quarantine_states.as_slice(),
        retention_states.as_slice(),
        webhook_processed_at_unix_seconds.as_slice(),
    );

    assert_eq!(first.is_ok(), second.is_ok());
    match (&first, &second) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(left, right),
        _ => return,
    }

    let Ok(summary) = first else {
        return;
    };
    let expected = expected_summary(
        now_unix_seconds,
        webhook_retention_seconds,
        quarantine_states.as_slice(),
        retention_states.as_slice(),
        webhook_processed_at_unix_seconds.as_slice(),
    );
    let Ok(expected) = expected else {
        return;
    };
    assert_eq!(summary, expected);
    assert_eq!(
        quarantine_total(&summary),
        u64::try_from(quarantine_states.len()).unwrap_or(u64::MAX)
    );
    assert_eq!(
        retention_total(&summary),
        u64::try_from(retention_states.len()).unwrap_or(u64::MAX)
    );
    assert_eq!(
        webhook_total(&summary),
        u64::try_from(webhook_processed_at_unix_seconds.len()).unwrap_or(u64::MAX)
    );
});

fn summarize(
    now_unix_seconds: u64,
    webhook_retention_seconds: u64,
    quarantine_states: &[QuarantineState],
    retention_states: &[RetentionState],
    webhook_processed_at_unix_seconds: &[u64],
) -> Result<FuzzLifecycleRepairSummary, String> {
    fuzz_lifecycle_repair_summary(
        now_unix_seconds,
        webhook_retention_seconds,
        quarantine_states,
        retention_states,
        webhook_processed_at_unix_seconds,
    )
    .map_err(|error| format!("{error}"))
}

fn expected_summary(
    now_unix_seconds: u64,
    webhook_retention_seconds: u64,
    quarantine_states: &[QuarantineState],
    retention_states: &[RetentionState],
    webhook_processed_at_unix_seconds: &[u64],
) -> Result<FuzzLifecycleRepairSummary, String> {
    let max_processed_at_unix_seconds = now_unix_seconds
        .checked_add(300)
        .ok_or_else(|| "webhook future threshold overflowed".to_owned())?;
    let stale_cutoff_unix_seconds = now_unix_seconds.saturating_sub(webhook_retention_seconds);
    let mut summary = FuzzLifecycleRepairSummary {
        quarantine_keep: 0,
        quarantine_delete_missing: 0,
        quarantine_delete_reachable: 0,
        quarantine_delete_held: 0,
        retention_keep: 0,
        retention_delete_expired: 0,
        retention_delete_missing: 0,
        webhook_keep: 0,
        webhook_delete_stale: 0,
        webhook_delete_future: 0,
    };

    for &(object_exists, is_reachable, is_held) in quarantine_states {
        if !object_exists {
            summary.quarantine_delete_missing =
                checked_increment(summary.quarantine_delete_missing)?;
        } else if is_reachable {
            summary.quarantine_delete_reachable =
                checked_increment(summary.quarantine_delete_reachable)?;
        } else if is_held {
            summary.quarantine_delete_held = checked_increment(summary.quarantine_delete_held)?;
        } else {
            summary.quarantine_keep = checked_increment(summary.quarantine_keep)?;
        }
    }

    for &(release_after_unix_seconds, held_at_unix_seconds, object_exists) in retention_states {
        let is_expired = release_after_unix_seconds.is_some_and(|release_after| {
            release_after < held_at_unix_seconds || release_after <= now_unix_seconds
        });
        if is_expired {
            summary.retention_delete_expired = checked_increment(summary.retention_delete_expired)?;
        } else if object_exists {
            summary.retention_keep = checked_increment(summary.retention_keep)?;
        } else {
            summary.retention_delete_missing = checked_increment(summary.retention_delete_missing)?;
        }
    }

    for &processed_at_unix_seconds in webhook_processed_at_unix_seconds {
        if processed_at_unix_seconds > max_processed_at_unix_seconds {
            summary.webhook_delete_future = checked_increment(summary.webhook_delete_future)?;
        } else if processed_at_unix_seconds <= stale_cutoff_unix_seconds {
            summary.webhook_delete_stale = checked_increment(summary.webhook_delete_stale)?;
        } else {
            summary.webhook_keep = checked_increment(summary.webhook_keep)?;
        }
    }

    Ok(summary)
}

fn checked_increment(value: u64) -> Result<u64, String> {
    value
        .checked_add(1)
        .ok_or_else(|| "summary counter overflowed".to_owned())
}

const fn quarantine_total(summary: &FuzzLifecycleRepairSummary) -> u64 {
    summary
        .quarantine_keep
        .saturating_add(summary.quarantine_delete_missing)
        .saturating_add(summary.quarantine_delete_reachable)
        .saturating_add(summary.quarantine_delete_held)
}

const fn retention_total(summary: &FuzzLifecycleRepairSummary) -> u64 {
    summary
        .retention_keep
        .saturating_add(summary.retention_delete_expired)
        .saturating_add(summary.retention_delete_missing)
}

const fn webhook_total(summary: &FuzzLifecycleRepairSummary) -> u64 {
    summary
        .webhook_keep
        .saturating_add(summary.webhook_delete_stale)
        .saturating_add(summary.webhook_delete_future)
}
