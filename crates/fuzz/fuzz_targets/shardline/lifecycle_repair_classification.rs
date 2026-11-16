#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_server::{
    FuzzQuarantineAction, FuzzRetentionAction, FuzzWebhookAction, fuzz_classify_quarantine,
    fuzz_classify_retention, fuzz_classify_webhook,
};

fuzz_target!(|data: (
    bool,
    bool,
    bool, // quarantine: object_exists, is_reachable, is_held
    Option<u64>,
    u64, // retention: release_after, now
    u64,
    u64,
    u64, // webhook: processed_at, stale_cutoff, max_processed_at
)| {
    let (q_exists, q_reachable, q_held, r_release, r_now, w_processed, w_stale, w_max) = data;

    // --- Quarantine classification ---
    let q1 = fuzz_classify_quarantine(q_exists, q_reachable, q_held);
    let q2 = fuzz_classify_quarantine(q_exists, q_reachable, q_held);
    assert_eq!(q1, q2, "quarantine classification must be deterministic");

    // Verify all states are reachable
    match (q_exists, q_reachable, q_held) {
        (false, _, _) => assert_eq!(q1, FuzzQuarantineAction::DeleteMissing),
        (true, true, _) => assert_eq!(q1, FuzzQuarantineAction::DeleteReachable),
        (true, false, true) => assert_eq!(q1, FuzzQuarantineAction::DeleteHeld),
        (true, false, false) => assert_eq!(q1, FuzzQuarantineAction::Keep),
    }

    // --- Retention classification ---
    // Missing bytes do not end a finite or permanent hold; only its expiry does.
    let expected_retention = if r_release.is_some_and(|release| release <= r_now) {
        FuzzRetentionAction::DeleteExpired
    } else {
        FuzzRetentionAction::Keep
    };
    for _ in 0..2 {
        let result = fuzz_classify_retention(r_release, r_now);
        assert!(
            matches!(&result, Ok(action) if *action == expected_retention),
            "release_after={r_release:?}, now={r_now}: {result:?}"
        );
    }

    // --- Webhook classification ---
    let w1 = fuzz_classify_webhook(w_processed, w_stale, w_max);
    let w2 = fuzz_classify_webhook(w_processed, w_stale, w_max);
    assert_eq!(w1, w2, "webhook classification must be deterministic");

    if w_processed > w_max {
        assert_eq!(w1, FuzzWebhookAction::DeleteFuture);
    } else if w_processed <= w_stale {
        assert_eq!(w1, FuzzWebhookAction::DeleteStale);
    } else {
        assert_eq!(w1, FuzzWebhookAction::Keep);
    }
});
