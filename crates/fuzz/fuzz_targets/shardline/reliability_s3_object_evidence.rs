#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{S3ObjectEvidenceLog, S3ObjectSnapshot, verify_s3_object_events};

fuzz_target!(|input: &[u8]| {
    let Ok(values) = serde_json::from_slice::<Vec<S3ObjectSnapshot>>(input) else {
        return;
    };
    let Some((first, rest)) = values.split_first() else {
        return;
    };
    let Ok(mut evidence) = S3ObjectEvidenceLog::baseline(first.clone()) else {
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
    drop(verify_s3_object_events(evidence.events(), &last.after));
});
