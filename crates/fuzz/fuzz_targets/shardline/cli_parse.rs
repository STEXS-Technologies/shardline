#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline::CliCommand;

fuzz_target!(|data: Vec<String>| {
    // INVARIANT 1: CLI parsing is deterministic for all argument vectors.
    let first = CliCommand::parse(data.clone());
    let second = CliCommand::parse(data);
    assert_eq!(first, second);

    // INVARIANT 2: Help text remains available and references the executable command.
    assert!(CliCommand::help_text().contains("shardline"));
});
