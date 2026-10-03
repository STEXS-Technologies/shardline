use std::{
    ffi::{OsStr, OsString},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static TEMPORARY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a collision-resistant temporary basename of at most 255 encoded bytes.
/// Callers must create it exclusively and retry collisions.
///
/// Short destination names retain the legacy suffix so managed-object crash
/// temporaries remain recognizable by GC. Long destinations use an independent
/// ASCII name containing the process ID, timestamp and counter.
#[must_use]
pub fn temporary_file_name(file_name: &OsStr) -> OsString {
    let counter = TEMPORARY_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let suffix = format!(".tmp-{unix_nanos}-{counter}");
    if file_name.as_encoded_bytes().len() <= 255_usize.saturating_sub(suffix.len()) {
        let mut name = file_name.to_os_string();
        name.push(suffix);
        return name;
    }
    let pid = std::process::id();
    format!("shardline.tmp-{unix_nanos:032x}-{pid:08x}-{counter:016x}").into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_object_names_keep_gc_recognizable_suffixes() {
        let hash = "a".repeat(64);
        for destination in [
            hash.clone(),
            format!("{hash}.xorb"),
            format!("{hash}.shard"),
            "last-gc-clock-anchor".to_owned(),
            "gc-clock-boot-observation".to_owned(),
        ] {
            let name = temporary_file_name(OsStr::new(&destination));
            let name = name.to_str().unwrap();
            let (base, suffix) = name.rsplit_once(".tmp-").unwrap();
            assert_eq!(base, destination);
            let (nanos, counter) = suffix.split_once('-').unwrap();
            assert!(nanos.parse::<u128>().is_ok());
            assert!(counter.parse::<u64>().is_ok());
            assert!(name.len() <= 255);
        }
    }

    #[test]
    fn names_are_bounded_ascii_and_unique_for_long_destinations() {
        let destination = OsString::from(format!("{}x", "é".repeat(127)));
        let names: std::collections::HashSet<_> = (0..1024)
            .map(|_| temporary_file_name(&destination))
            .collect();
        assert_eq!(names.len(), 1024);
        for name in names {
            let name = name.to_str().unwrap();
            assert!(name.len() < 255);
            assert!(name.is_ascii());
            assert!(name.starts_with("shardline.tmp-"));
        }
    }
}
