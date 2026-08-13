#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let data = data.get(..data.len().min(MAX_INPUT)).unwrap_or(data);
    let Ok(raw) = std::str::from_utf8(data) else {
        return;
    };

    // Arbitrary text must parse as SdxConfig without panicking, deterministically.
    let first: Result<sdx::config::SdxConfig, _> = toml::from_str(raw);
    let second: Result<sdx::config::SdxConfig, _> = toml::from_str(raw);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
});
