use std::{fs, io};

use shardline_storage::{LocalObjectStore, LocalObjectStoreError, ObjectKey, ObjectPrefix};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}

#[test]
fn flat_pages_preserve_byte_order_cursor_and_metadata() -> TestResult {
    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("ns"))?;
    let store = LocalObjectStore::new(root.path().to_owned())?;
    let prefix = ObjectPrefix::parse("ns/")?;
    let mut names = [
        "a",
        "A",
        "é",
        "e\u{301}",
        "literal%",
        "literal_",
        "ζ",
        "\u{10ffff}",
    ];
    names.sort_unstable();
    for name in names.iter().rev() {
        fs::write(root.path().join("ns").join(name), name.as_bytes())?;
    }
    fs::create_dir(root.path().join("ns/subdirectory"))?;
    let expected: Vec<_> = names.iter().map(|name| format!("ns/{name}")).collect();
    for limit in [1, 3, 100] {
        let mut cursor = None;
        let mut actual = Vec::new();
        loop {
            let page = store.list_flat_namespace_page(&prefix, cursor.as_ref(), limit)?;
            if page.is_empty() {
                break;
            }
            check(page.len() <= limit, "page exceeds limit")?;
            for row in &page {
                let name = row.key().as_str().strip_prefix("ns/");
                check(
                    name.is_some_and(|name| row.length() == name.len() as u64),
                    "observed length changed",
                )?;
                check(
                    row.modified_unix_nanos().is_some(),
                    "observed mtime missing",
                )?;
            }
            cursor = page.last().map(|row| row.key().clone());
            actual.extend(page.into_iter().map(|row| row.key().as_str().to_owned()));
            check(actual.len() <= expected.len(), "cursor did not advance")?;
        }
        check(
            actual == expected,
            "byte ordering or cursor page membership changed",
        )?;
    }
    let outside = ObjectKey::parse("outside/key")?;
    check(
        matches!(
            store.list_flat_namespace_page(&prefix, Some(&outside), 0),
            Err(LocalObjectStoreError::InvalidStartAfter)
        ),
        "zero limit bypassed cursor validation",
    )?;
    Ok(())
}

#[test]
fn tiny_and_zero_pages_do_not_retain_a_namespace_sized_buffer() -> TestResult {
    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("ns"))?;
    let store = LocalObjectStore::new(root.path().to_owned())?;
    let prefix = ObjectPrefix::parse("ns/")?;
    for index in (0..4096).rev() {
        fs::write(root.path().join("ns").join(format!("{index:064x}")), [])?;
    }
    for limit in [0, 1, 17] {
        let page = store.list_flat_namespace_page(&prefix, None, limit)?;
        check(page.len() == limit, "wrong page size")?;
        check(
            page.capacity() <= limit.saturating_mul(2).max(4),
            "returned page retained storage proportional to the full namespace",
        )?;
        for (index, row) in page.iter().enumerate() {
            check(
                row.key().as_str() == format!("ns/{index:064x}"),
                "heap selected the wrong keys",
            )?;
        }
    }
    // A huge requested limit must not cause an upfront allocation of that size.
    let all = store.list_flat_namespace_page(&prefix, None, usize::MAX)?;
    check(all.len() == 4096, "large limit lost entries")?;
    let midpoint = ObjectKey::parse(&format!("ns/{:064x}", 2048))?;
    let page = store.list_flat_namespace_page(&prefix, Some(&midpoint), 1)?;
    check(
        page.first()
            .is_some_and(|row| row.key().as_str() == format!("ns/{:064x}", 2049)),
        "after-cursor selection changed",
    )?;
    check(page.capacity() <= 4, "cursor page retained excess storage")?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn invalid_stored_keys_beyond_the_selected_page_still_fail() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("ns"))?;
    let store = LocalObjectStore::new(root.path().to_owned())?;
    let prefix = ObjectPrefix::parse("ns/")?;
    fs::write(root.path().join("ns/a"), [])?;
    // Both names fall outside the first valid key's page; neither may be skipped
    // just because the heap is full or the requested page is empty.
    for invalid in [OsString::from("z\n"), OsString::from_vec(vec![b'z', 0xff])] {
        let path = root.path().join("ns").join(invalid);
        fs::write(&path, [])?;
        for limit in [0, 1] {
            for cursor in [None, Some(ObjectKey::parse("ns/a")?)] {
                check(
                    matches!(
                        store.list_flat_namespace_page(&prefix, cursor.as_ref(), limit),
                        Err(LocalObjectStoreError::InvalidStoredKey)
                    ),
                    "selection skipped corrupt stored-key validation",
                )?;
            }
        }
        fs::remove_file(path)?;
    }
    Ok(())
}
