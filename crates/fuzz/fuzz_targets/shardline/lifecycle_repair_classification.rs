#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_server::{
    FuzzQuarantineAction, FuzzRetentionAction, FuzzWebhookAction,
    fuzz_classify_quarantine, fuzz_classify_retention, fuzz_classify_webhook,
};

fuzz_target!(|data: (
    bool, bool, bool,           // quarantine: object_exists, is_reachable, is_held
    Option<u64>, u64, bool, u64, // retention: release_after, held_at, exists, now
    u64, u64, u64,               // webhook: processed_at, stale_cutoff, max_processed_at
)| {
    let (q_exists, q_reachable, q_held, r_release, r_held_at, r_exists, r_now, w_processed, w_stale, w_max) = data;

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
    let r1 = fuzz_classify_retention(r_release, r_held_at, r_exists, r_now);
    let r2 = fuzz_classify_retention(r_release, r_held_at, r_exists, r_now);
    assert_eq!(r1, r2, "retention classification must be deterministic");

    // Verify state consistency
    if r_release.is_some_and(|ra| ra <= r_now) {
        assert_eq!(r1, FuzzRetentionAction::DeleteExpired,
            "release_after={ra:?} <= now={r_now} should expire", ra = r_release);
    } else if !r_exists {
        assert_eq!(r1, FuzzRetentionAction::DeleteMissing);
    } else {
        assert_eq!(r1, FuzzRetentionAction::Keep);
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
