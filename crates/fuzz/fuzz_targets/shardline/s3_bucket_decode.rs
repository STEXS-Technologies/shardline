#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_s3_adapter::decode_bucket;

/// Bound on the fuzzed bucket string (S3 buckets are at most 63 bytes, but we
/// fuzz well beyond the protocol bound to exercise the rejection paths).
const MAX_BUCKET_BYTES: usize = 256;

fuzz_target!(|bucket: String| {
    if bucket.len() > MAX_BUCKET_BYTES {
        return;
    }

    // Determinism: decoding the same bucket twice yields the same outcome.
    let first = decode_bucket(&bucket);
    let second = decode_bucket(&bucket);
    match (first, second) {
        (Ok((owner_a, name_a)), Ok((owner_b, name_b))) => {
            assert_eq!((owner_a.clone(), name_a.clone()), (owner_b, name_b));
            // The decoded owner never contains a dot (first-dot split) and
            // re-joining reproduces the input exactly.
            assert!(!owner_a.contains('.'));
            assert_eq!(format!("{owner_a}.{name_a}"), bucket);
        }
        (Err(left), Err(right)) => assert_eq!(left, right),
        _ => return,
    }
});
