#![no_main]

use std::str::from_utf8;

use libfuzzer_sys::fuzz_target;
use shardline_storage::{ObjectKey, ObjectPrefix};

fuzz_target!(|data: &[u8]| {
    let Ok(raw) = from_utf8(data) else {
        return;
    };

    // INVARIANT 1: Object key validation is deterministic.
    let first = ObjectKey::parse(raw).map(|key| key.as_str().to_owned());
    let second = ObjectKey::parse(raw).map(|key| key.as_str().to_owned());
    assert_eq!(first, second);

    // INVARIANT 2: Accepted keys are stable, relative, and free of traversal markers.
    if let Ok(key) = ObjectKey::parse(raw) {
        assert_eq!(key.as_str(), raw);
        assert!(!key.as_str().is_empty());
        assert_safe_storage_segments(key.as_str(), false);
    }

    // INVARIANT 3: Object prefix validation is deterministic.
    let first_prefix = ObjectPrefix::parse(raw).map(|prefix| prefix.as_str().to_owned());
    let second_prefix = ObjectPrefix::parse(raw).map(|prefix| prefix.as_str().to_owned());
    assert_eq!(first_prefix, second_prefix);

    // INVARIANT 4: Accepted prefixes preserve the exact caller value while bounding
    // path semantics to relative, non-traversing inventory roots.
    if let Ok(prefix) = ObjectPrefix::parse(raw) {
        assert_eq!(prefix.as_str(), raw);
        assert_safe_storage_segments(prefix.as_str(), true);
    }
});

fn assert_safe_storage_segments(value: &str, allow_empty_or_trailing_separator: bool) {
    assert!(!value.starts_with('/'));
    assert!(!value.contains('\\'));
    assert!(!value.chars().any(char::is_control));

    let trimmed = if allow_empty_or_trailing_separator {
        value.strip_suffix('/').unwrap_or(value)
    } else {
        value
    };

    if allow_empty_or_trailing_separator && trimmed.is_empty() {
        return;
    }

    assert!(!trimmed.is_empty());
    for segment in trimmed.split('/') {
        assert!(!segment.is_empty());
        assert_ne!(segment, ".");
        assert_ne!(segment, "..");
    }
}
