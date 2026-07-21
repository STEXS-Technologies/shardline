#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_protocol_adapters::LfsOperation;

fuzz_target!(|data: &[u8]| {
    let Ok(raw) = std::str::from_utf8(data) else {
        return;
    };

    // INVARIANT: LfsOperation::from_str is deterministic.
    let first = raw.parse::<LfsOperation>();
    let second = raw.parse::<LfsOperation>();
    assert_eq!(first.is_ok(), second.is_ok());

    // INVARIANT: Any accepted value round-trips through as_str.
    if let Ok(op) = first {
        let s = op.as_str();
        let reparsed: Result<LfsOperation, _> = s.parse();
        assert!(matches!(reparsed, Ok(re) if re == op));

        // Verify the output is always the canonical form.
        match op {
            LfsOperation::Download => assert_eq!(s, "download"),
            LfsOperation::Upload => assert_eq!(s, "upload"),
        }
    }
});
