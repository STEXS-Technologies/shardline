use std::{hint::black_box, process::ExitCode, time::Instant};

use blake3::hash as blake3_hash;
use shardline_protocol::ShardlineHash;
use shardline_storage::{
    LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix,
    ObjectStore,
};
use tempfile::TempDir;

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
            "old" => old_full_scan_page(&fixture.store, &fixture.prefix, OCI_TAG_PAGE_SIZE, last),
            "new" => {
                new_flat_namespace_page(&fixture.store, &fixture.prefix, OCI_TAG_PAGE_SIZE, last)
            }
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

fn old_full_scan_page(
    store: &LocalObjectStore,
    prefix: &ObjectPrefix,
    page_size: usize,
    last: &str,
) -> Vec<String> {
    let mut tags = std::collections::BTreeSet::new();
    let mut has_more = false;
    if let Err(error) = store.visit_prefix(prefix, |object: ObjectMetadata| {
        let Some(tag) = object.key().as_str().rsplit('/').next() else {
            return Ok::<(), shardline_storage::LocalObjectStoreError>(());
        };
        if tag <= last {
            return Ok(());
        }
        let _inserted = tags.insert(tag.to_owned());
        if tags.len() > page_size {
            let _removed = tags.pop_last();
            has_more = true;
        }
        Ok(())
    }) {
        eprintln!("old_full_scan_page failed: {error}");
        std::process::abort();
    }
    let tags = tags.into_iter().collect::<Vec<_>>();
    black_box(has_more);
    tags
}

fn new_flat_namespace_page(
    store: &LocalObjectStore,
    prefix: &ObjectPrefix,
    page_size: usize,
    last: &str,
) -> Vec<String> {
    let start_after =
        ObjectKey::parse(&format!("{}{}", prefix.as_str(), last)).unwrap_or_else(|error| {
            eprintln!("start-after key parse failed: {error}");
            std::process::abort();
        });
    let page = store
        .list_flat_namespace_page(prefix, Some(&start_after), page_size.saturating_add(1))
        .unwrap_or_else(|error| {
            eprintln!("new_flat_namespace_page failed: {error}");
            std::process::abort();
        });
    let mut tags = page
        .into_iter()
        .filter_map(|object| {
            object
                .key()
                .as_str()
                .rsplit('/')
                .next()
                .map(ToOwned::to_owned)
        })
        .collect::<Vec<_>>();
    let has_more = tags.len() > page_size;
    tags.truncate(page_size);
    black_box(has_more);
    tags
}
