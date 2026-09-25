#![no_main]

use std::num::NonZeroU64;

use libfuzzer_sys::fuzz_target;
use shardline_index::{
    ResumableSession, ResumableSessionPart, ResumableSessionProtocol, resumable_state_digest,
};

fuzz_target!(|input: &[u8]| {
    let Some(bounded) = input.get(..input.len().min(256)) else {
        return;
    };
    let session = ResumableSession::new(
        format!("session-{}", bounded.len()),
        ResumableSessionProtocol::LfsPatch,
        "fuzz-scope".to_owned(),
        "fuzz-target".to_owned(),
        std::time::Duration::from_secs(1_700_000_000),
    );
    let parts = bounded
        .chunks(8)
        .enumerate()
        .filter_map(|(index, chunk)| {
            let number = NonZeroU64::new(u64::try_from(index.checked_add(1)?).ok()?)?;
            let generation = NonZeroU64::new(u64::from(chunk.first().copied().unwrap_or(1)) + 1)?;
            Some(ResumableSessionPart::new(
                number,
                generation,
                format!("staging/{index}"),
                u64::try_from(chunk.len()).ok()?,
                None,
            ))
        })
        .collect::<Vec<_>>();

    let Ok(first) = resumable_state_digest(&session, &parts) else {
        return;
    };
    let Ok(second) = resumable_state_digest(&session, &parts) else {
        return;
    };
    assert_eq!(first, second);
});
