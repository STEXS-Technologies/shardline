#![cfg(unix)]

use std::{fs, io};

use shardline_storage::{LocalObjectStore, LocalObjectStoreError, ObjectPrefix, ObjectStore};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn recursive_inventory_rejects_literal_backslashes_without_inventing_keys() -> TestResult {
    for relative in [r"ns/a\b", r"ns/a\b/object"] {
        let root = tempfile::tempdir()?;
        let path = root.path().join(relative);
        fs::create_dir_all(
            path.parent()
                .ok_or_else(|| io::Error::other("missing parent"))?,
        )?;
        fs::write(path, b"payload")?;
        let store = LocalObjectStore::open(root.path().to_owned());
        let prefix = ObjectPrefix::parse("ns/")?;
        if !matches!(
            ObjectStore::list_prefix(&store, &prefix),
            Err(LocalObjectStoreError::InvalidStoredKey)
        ) {
            return Err(io::Error::other("inventory accepted a literal backslash").into());
        }
        let mut visited = Vec::new();
        let result = ObjectStore::visit_prefix(&store, &prefix, |metadata| {
            visited.push(metadata);
            Ok::<_, LocalObjectStoreError>(())
        });
        if !matches!(result, Err(LocalObjectStoreError::InvalidStoredKey)) || !visited.is_empty() {
            return Err(io::Error::other("visitor received an invented object key").into());
        }
    }
    Ok(())
}

#[test]
fn recursive_inventory_retains_real_nested_keys_and_metadata() -> TestResult {
    let root = tempfile::tempdir()?;
    fs::create_dir_all(root.path().join("ns/a"))?;
    fs::write(root.path().join("ns/a/b"), b"payload")?;
    let store = LocalObjectStore::open(root.path().to_owned());
    let prefix = ObjectPrefix::parse("ns/")?;
    let rows = ObjectStore::list_prefix(&store, &prefix)?;
    let row = rows
        .first()
        .ok_or_else(|| io::Error::other("nested object missing"))?;
    if rows.len() != 1
        || row.key().as_str() != "ns/a/b"
        || row.length() != 7
        || !ObjectStore::contains(&store, row.key())?
    {
        return Err(io::Error::other("nested object metadata changed").into());
    }
    let mut visited = Vec::new();
    ObjectStore::visit_prefix(&store, &prefix, |metadata| {
        visited.push(metadata);
        Ok::<_, LocalObjectStoreError>(())
    })?;
    if visited != rows {
        return Err(io::Error::other("list and visitor inventories disagree").into());
    }
    Ok(())
}
