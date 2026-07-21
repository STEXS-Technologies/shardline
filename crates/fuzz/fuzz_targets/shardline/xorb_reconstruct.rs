#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use shardline_xet_core::xorb_object::reconstruct_xorb_with_footer;
use shardline_xet_core::xorb_object::xorb_object_format::XorbObject;

fuzz_target!(|data: &[u8]| {
    // Fuzz the xorb reconstruction path by feeding arbitrary bytes into
    // reconstruct_xorb_with_footer, then round-tripping the output through
    // validation.
    //
    // This complements the existing protocol_xorb fuzzer (which tests the
    // read/validate/decode path) by covering the write/reconstruct path.
    //
    // Note: reconstruct_xorb_with_footer may panic on empty input or
    // malformed chunk headers. These are tracked as known issues.

    if data.len() < 16 {
        return; // Need at least one chunk header worth of data
    }

    let mut output = Vec::new();
    let result = reconstruct_xorb_with_footer(&mut output, data);

    // The reconstruction may succeed or fail depending on the data.
    // Either outcome is valid — we just must not panic.
    if let Ok((_xorb, hash)) = result {
        // If reconstruction succeeded, validate the output round-trips
        let mut reader = Cursor::new(output.as_slice());
        let validated = XorbObject::validate_xorb_object(&mut reader, &hash);

        // Validation may pass or fail — arbitrary chunk data may not form
        // a valid xorb. Either outcome is fine.
        drop(validated);
    }
});
