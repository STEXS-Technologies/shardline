#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let data = data.get(..data.len().min(MAX_INPUT)).unwrap_or(data);
    let Ok(raw) = std::str::from_utf8(data) else {
        return;
    };

    // INVARIANT 1: parsing is deterministic (both success and error paths).
    let first = sdx::url::XetUrl::parse(raw);
    let second = sdx::url::XetUrl::parse(raw);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));

    // INVARIANT 2: an accepted URL round-trips through its canonical display
    // form (parsing the display string yields the same display form).
    if let Ok(url) = first {
        let reparsed = sdx::url::XetUrl::parse(&url.display());
        assert!(reparsed.is_ok());
        if let Ok(reparsed) = reparsed {
            assert_eq!(reparsed.display(), url.display());
        }
    }
});
