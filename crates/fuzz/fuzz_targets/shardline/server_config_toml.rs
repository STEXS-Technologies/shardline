#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let data = data.get(..data.len().min(MAX_INPUT)).unwrap_or(data);
    let Ok(raw) = std::str::from_utf8(data) else {
        return;
    };

    let first = shardline_server::parse_toml_config_for_fuzzing(raw);
    let second = shardline_server::parse_toml_config_for_fuzzing(raw);
    assert_eq!(first, second);
});
