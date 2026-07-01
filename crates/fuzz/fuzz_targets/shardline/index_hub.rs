#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_index::hub::HubRepoType;

fuzz_target!(|data: &str| {
    let first = HubRepoType::parse_str(data);
    let second = HubRepoType::parse_str(data);
    assert_eq!(first, second);

    #[allow(clippy::let_underscore_must_use)]
    let _ = HubRepoType::from_api_repo_type(data);
});
