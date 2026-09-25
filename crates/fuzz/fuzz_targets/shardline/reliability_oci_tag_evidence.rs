#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{OciTagEvidenceLog, OciTagSnapshot, verify_oci_tag_events};

fuzz_target!(|input: &[u8]| {
    let Ok(values) = serde_json::from_slice::<Vec<OciTagSnapshot>>(input) else {
        return;
    };
    let Some((first, rest)) = values.split_first() else {
        return;
    };
    let Ok(mut evidence) = OciTagEvidenceLog::baseline(first.clone()) else {
        return;
    };
    for value in rest {
        if evidence.record(value.clone()).is_err() {
            return;
        }
    }
    let Some(last) = evidence.events().last() else {
        return;
    };
    drop(verify_oci_tag_events(evidence.events(), &last.after));
});
