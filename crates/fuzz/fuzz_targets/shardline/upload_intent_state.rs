#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_index::UploadIntentState;

const STATES: [UploadIntentState; 6] = [
    UploadIntentState::Created,
    UploadIntentState::Storing,
    UploadIntentState::Stored,
    UploadIntentState::MetadataCommitted,
    UploadIntentState::Visible,
    UploadIntentState::Failed,
];

const fn rank(state: UploadIntentState) -> u8 {
    match state {
        UploadIntentState::Created => 0,
        UploadIntentState::Storing => 1,
        UploadIntentState::Stored => 2,
        UploadIntentState::MetadataCommitted => 3,
        UploadIntentState::Visible => 4,
        UploadIntentState::Failed => 5,
    }
}

fuzz_target!(|data: (&str, &[u8])| {
    let (persisted, choices) = data;
    if let Some(state) = UploadIntentState::parse(persisted) {
        assert_eq!(
            state.as_str(),
            persisted,
            "accepted state text must be canonical"
        );
    }

    let mut current = UploadIntentState::Created;
    for choice in choices.iter().take(4096) {
        let next = match choice {
            0..=42 => UploadIntentState::Created,
            43..=85 => UploadIntentState::Storing,
            86..=128 => UploadIntentState::Stored,
            129..=171 => UploadIntentState::MetadataCommitted,
            172..=213 => UploadIntentState::Visible,
            _ => UploadIntentState::Failed,
        };
        if current.can_transition_to(next) {
            let previous = current;
            current = next;
            assert!(rank(current) >= rank(previous), "accepted state regressed");
            if current != UploadIntentState::Failed {
                assert!(
                    rank(current)
                        .checked_sub(rank(previous))
                        .is_some_and(|difference| difference <= 1),
                    "accepted state skipped a durability boundary"
                );
            }
        }

        if matches!(
            current,
            UploadIntentState::Visible | UploadIntentState::Failed
        ) {
            for candidate in STATES {
                assert_eq!(
                    current.can_transition_to(candidate),
                    candidate == current,
                    "terminal state accepted a non-idempotent transition"
                );
            }
        }
    }
});
