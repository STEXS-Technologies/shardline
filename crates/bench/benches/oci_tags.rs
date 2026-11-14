use std::hint::black_box;

use blake3::hash as blake3_hash;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use shardline_protocol::ShardlineHash;
use shardline_storage::{
    LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore,
};
use tempfile::TempDir;

#[path = "support/oci_tags.rs"]
mod tag_pages;
use tag_pages::{must, new_flat_namespace_page, old_full_scan_page};

const OCI_TAG_COUNT: usize = 50_000;
const OCI_TAG_PAGE_SIZE: usize = 128;
const OCI_TAG_DEEP_OFFSET: usize = 49_000;

struct OciTagFixture {
    _storage: TempDir,
    prefix: ObjectPrefix,
    store: LocalObjectStore,
    tags: Vec<String>,
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
        let mut tags = Vec::with_capacity(tag_count);
        for index in 0..tag_count {
            let tag = format!("tag-{index:06}");
            let key = ObjectKey::parse(&format!("{}{}", prefix.as_str(), tag))?;
            let _stored = store.put_if_absent(&key, ObjectBody::from_slice(digest), &integrity)?;
            tags.push(tag);
        }
        Ok(Self {
            _storage: storage,
            prefix,
            store,
            tags,
        })
    }
}

fn oci_tag_benchmarks(criterion: &mut Criterion) {
    let fixture = must(
        OciTagFixture::new(OCI_TAG_COUNT),
        "oci tag benchmark fixture should initialize",
    );
    let deep_last = fixture.tags.get(OCI_TAG_DEEP_OFFSET).cloned();
    let first_last = fixture.tags.get(OCI_TAG_PAGE_SIZE).cloned();

    let mut group = criterion.benchmark_group("shardline_server_oci_tags");
    group.sample_size(20);

    for (name, last) in [
        ("first_page", None),
        ("mid_page", first_last.as_deref()),
        ("deep_page", deep_last.as_deref()),
    ] {
        group.bench_with_input(
            BenchmarkId::new("old_full_scan", name),
            &last,
            |bench, last| {
                bench.iter(|| {
                    black_box(old_full_scan_page(
                        &fixture.store,
                        &fixture.prefix,
                        OCI_TAG_PAGE_SIZE,
                        *last,
                    ))
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("new_flat_namespace", name),
            &last,
            |bench, last| {
                bench.iter(|| {
                    black_box(new_flat_namespace_page(
                        &fixture.store,
                        &fixture.prefix,
                        OCI_TAG_PAGE_SIZE,
                        *last,
                    ))
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, oci_tag_benchmarks);
criterion_main!(benches);
