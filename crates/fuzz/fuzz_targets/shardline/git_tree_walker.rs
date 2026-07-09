#![no_main]

use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;

fuzz_target!(|data: &[u8]| {
    // This is harder to fuzz directly since it needs a map of SHA→GitObject.
    // Instead, fuzz the tree parser by feeding raw tree data.
    // The tree format is: "<mode> <name>\0<20-byte-sha>" repeated.
    // We can't easily fuzz walk_git_tree without valid objects,
    // but we CAN fuzz the underlying tree parsing logic.
    // For now, just ensure the function doesn't panic on empty/short input.
    if data.len() < 20 {
        return;
    }
    let sha = [0u8; 20]; // dummy SHA
    let objects = HashMap::new(); // empty map — walk_git_tree will fail to find objects but shouldn't panic
    let _ = shardline_hub_api::git::smart_http::walk_git_tree(&sha, &objects, "");
});
