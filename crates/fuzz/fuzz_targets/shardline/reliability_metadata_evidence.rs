#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{HubRefEvidenceLog, HubRefSnapshot, verify_hub_ref_events};

fuzz_target!(|input: &[u8]| {
    let Ok(values) = serde_json::from_slice::<Vec<HubRefSnapshot>>(input) else {
        return;
    };
    let Some((first, rest)) = values.split_first() else {
        return;
    };
    let Ok(mut evidence) = HubRefEvidenceLog::baseline(first.clone()) else {
        return;
    };
    for snapshot in rest {
        if evidence.record(snapshot.clone()).is_err() {
            return;
        }
    }
    let Some(last) = evidence.events().last() else {
        return;
    };
    drop(verify_hub_ref_events(evidence.events(), &last.after));
});
