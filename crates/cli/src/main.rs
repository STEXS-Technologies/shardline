#![deny(unsafe_code)]

use std::{env::args_os, process::ExitCode};

#[tokio::main]
async fn main() -> ExitCode {
    shardline::entry::run(args_os()).await
}
