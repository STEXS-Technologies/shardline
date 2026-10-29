//! Explicit Linux normalization I/O measurement, separate from HTTP benchmarks.

use super::{normalize_owned_with_checkpoint, normalize_uploaded_xorb_file};
use shardline_xet_core::xorb_object::{
    CompressionScheme, SerializedXorbObject,
    xorb_format_test_utils::{ChunkSize, build_raw_xorb},
};
use std::{fs::File, io::Write, path::Path};

#[derive(Clone, Copy)]
struct ProcessIo {
    wchar: u64,
    write_bytes: u64,
}

impl ProcessIo {
    fn read() -> Self {
        let counters = std::fs::read_to_string("/proc/self/io").unwrap();
        let value = |name: &str| {
            counters
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .expect("required Linux process I/O counter")
                .trim()
                .parse::<u64>()
                .unwrap()
        };
        Self {
            wchar: value("wchar:"),
            write_bytes: value("write_bytes:"),
        }
    }

    const fn since(self, before: Self) -> Self {
        Self {
            wchar: self.wchar.checked_sub(before.wchar).unwrap(),
            write_bytes: self.write_bytes.checked_sub(before.write_bytes).unwrap(),
        }
    }
}

fn digest(path: &Path) -> blake3::Hash {
    let mut digest = blake3::Hasher::new();
    std::io::copy(&mut File::open(path).unwrap(), &mut digest).unwrap();
    digest.finalize()
}

#[test]
#[ignore = "explicit Linux I/O diagnostic; run alone with --ignored --test-threads=1 --nocapture"]
fn footerless_owned_staging_avoids_one_payload_write() {
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        build_raw_xorb(256, ChunkSize::Fixed(256 * 1024)),
        CompressionScheme::None,
        false,
    )
    .unwrap();
    let hash = serialized.hash.hex();
    let wire_length = u64::try_from(serialized.serialized_data.len()).unwrap();
    let mut borrowed = tempfile::NamedTempFile::new().unwrap();
    let mut owned = tempfile::NamedTempFile::new().unwrap();
    borrowed.write_all(&serialized.serialized_data).unwrap();
    owned.write_all(&serialized.serialized_data).unwrap();
    drop(serialized);

    // Exclude fixture creation and later comparison reads. Neither operation
    // publishes objects: this measures normalization, not total upload I/O.
    let before = ProcessIo::read();
    let normalized = normalize_uploaded_xorb_file(&hash, borrowed.path())
        .unwrap()
        .expect("footerless fixture must use the copy path for borrowed input");
    let copied = ProcessIo::read().since(before);
    let before = ProcessIo::read();
    normalize_owned_with_checkpoint(&hash, owned.path(), &|| Ok(())).unwrap();
    let appended = ProcessIo::read().since(before);

    assert_eq!(digest(normalized.path()), digest(owned.path()));
    assert_eq!(borrowed.as_file().metadata().unwrap().len(), wire_length);
    assert_eq!(
        copied.wchar.checked_sub(appended.wchar).unwrap(),
        wire_length
    );
    eprintln!(
        "footerless normalization only: payload_wire_bytes={wire_length}, borrowed_wchar={}, owned_wchar={}, borrowed_write_bytes={}, owned_write_bytes={}; process accounting is not device wear or total upload cost",
        copied.wchar, appended.wchar, copied.write_bytes, appended.write_bytes
    );
}
