use std::{hint::black_box, process::ExitCode, time::Instant};

use blake3::hash as blake3_hash;
use shardline_protocol::ShardlineHash;
use shardline_storage::{
    LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore,
};
use tempfile::TempDir;

#[path = "support/oci_tags.rs"]
mod tag_pages;
use tag_pages::{new_flat_namespace_page, old_full_scan_page};

const OCI_TAG_COUNT: usize = 50_000;
const OCI_TAG_PAGE_SIZE: usize = 128;
const OCI_TAG_DEEP_OFFSET: usize = 49_000;
const DEFAULT_ITERATIONS: usize = 200;

struct OciTagFixture {
    _storage: TempDir,
    prefix: ObjectPrefix,
    store: LocalObjectStore,
    deep_last: Option<String>,
}

impl OciTagFixture {
    fn new(tag_count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let storage = tempfile::tempdir()?;
        let store = LocalObjectStore::new(storage.path().to_path_buf())?;
        let prefix = ObjectPrefix::parse("protocols/oci/global/repos/fixture/tags/")?;
        let digest = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3_hash(digest).as_bytes()),
            u64::try_from(digest.len())?,
        );
        let mut deep_last = None;
        for index in 0..tag_count {
            let tag = format!("tag-{index:06}");
            let key = ObjectKey::parse(&format!("{}{}", prefix.as_str(), tag))?;
            let _stored = store.put_if_absent(&key, ObjectBody::from_slice(digest), &integrity)?;
            if index == OCI_TAG_DEEP_OFFSET {
                deep_last = Some(tag);
            }
        }
        Ok(Self {
            _storage: storage,
            prefix,
            store,
            deep_last,
        })
    }
}

fn main() -> ExitCode {
    let mut mode = "new";
    let mut iterations = DEFAULT_ITERATIONS;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "old" | "new" => mode = Box::leak(argument.into_boxed_str()),
            "--bench" => {}
            "--iterations" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --iterations");
                    return ExitCode::FAILURE;
                };
                match value.parse::<usize>() {
                    Ok(parsed) if parsed > 0 => iterations = parsed,
                    Ok(_) | Err(_) => {
                        eprintln!("invalid --iterations value: {value}");
                        return ExitCode::FAILURE;
                    }
                }
            }
            _ => {
                eprintln!(
                    "usage: cargo bench --bench oci_tags_fixed -- [old|new] [--iterations N]"
                );
                return ExitCode::FAILURE;
            }
        }
    }

    let fixture = match OciTagFixture::new(OCI_TAG_COUNT) {
        Ok(fixture) => fixture,
        Err(error) => {
            eprintln!("fixture setup failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let Some(last) = fixture.deep_last.as_deref() else {
        eprintln!("missing deep-page marker");
        return ExitCode::FAILURE;
    };

    let start = Instant::now();
    for _ in 0..iterations {
        let page = match mode {
            "old" => old_full_scan_page(
                &fixture.store,
                &fixture.prefix,
                OCI_TAG_PAGE_SIZE,
                Some(last),
            ),
            "new" => new_flat_namespace_page(
                &fixture.store,
                &fixture.prefix,
                OCI_TAG_PAGE_SIZE,
                Some(last),
            ),
            invalid => {
                eprintln!("unsupported mode: {invalid}");
                return ExitCode::FAILURE;
            }
        };
        black_box(page);
    }
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis();
    let elapsed_micros_remainder = elapsed.subsec_micros() % 1000;
    println!(
        "mode={mode} iterations={iterations} elapsed_ms={elapsed_ms}.{elapsed_micros_remainder:03}"
    );
    ExitCode::SUCCESS
}
