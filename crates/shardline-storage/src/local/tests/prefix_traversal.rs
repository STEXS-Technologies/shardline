//! Prefix pruning preserves partial matches and avoids unrelated directories.
use super::*;

#[test]
fn directory_pruning_preserves_partial_and_nested_prefix_results() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    for key in [
        "wanted/one",
        "wanted/inner/four",
        "wanted-nested/two",
        "other/three",
    ] {
        store
            .put_if_absent(
                &ObjectKey::parse(key).unwrap(),
                ObjectBody::from_slice(b"data"),
                &ObjectIntegrity::new(super::super::util::chunk_hash(b"data"), 4),
            )
            .unwrap();
    }
    for (prefix, expected) in [
        (
            "",
            vec![
                "other/three",
                "wanted-nested/two",
                "wanted/inner/four",
                "wanted/one",
            ],
        ),
        (
            "wanted",
            vec!["wanted-nested/two", "wanted/inner/four", "wanted/one"],
        ),
        ("wanted/", vec!["wanted/inner/four", "wanted/one"]),
        ("wanted/in", vec!["wanted/inner/four"]),
        ("wanted/inner/f", vec!["wanted/inner/four"]),
        ("wanted/inner/four/", vec![]),
        ("missing/", vec![]),
    ] {
        let prefix = ObjectPrefix::parse(prefix).unwrap();
        let listed: Vec<_> = store
            .list_prefix(&prefix)
            .unwrap()
            .into_iter()
            .map(|entry| entry.key().as_str().to_owned())
            .collect();
        assert_eq!(listed, expected);
        let mut visited = Vec::new();
        store
            .visit_prefix(&prefix, |entry| {
                visited.push(entry.key().as_str().to_owned());
                Ok::<_, LocalObjectStoreError>(())
            })
            .unwrap();
        visited.sort();
        assert_eq!(visited, expected);
    }
}

#[cfg(unix)]
#[test]
fn scoped_inventory_does_not_enter_unrelated_unreadable_directory() {
    use std::os::unix::fs::PermissionsExt;
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    store
        .put_if_absent(
            &ObjectKey::parse("wanted/one").unwrap(),
            ObjectBody::from_slice(b"data"),
            &ObjectIntegrity::new(super::super::util::chunk_hash(b"data"), 4),
        )
        .unwrap();
    let unrelated = storage.path().join("unrelated");
    fs::create_dir(&unrelated).unwrap();
    fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o000)).unwrap();
    let prefix = ObjectPrefix::parse("wanted/").unwrap();
    let scoped = store.list_prefix(&prefix);
    let mut visited = Vec::new();
    let visit: Result<(), LocalObjectStoreError> = store.visit_prefix(&prefix, |entry| {
        visited.push(entry.key().as_str().to_owned());
        Ok(())
    });
    let full = store.list_prefix(&ObjectPrefix::parse("").unwrap());
    fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(full.is_err(), "fixture must enforce directory permissions");
    assert_eq!(scoped.unwrap().len(), 1);
    visit.unwrap();
    assert_eq!(visited, ["wanted/one"]);
}
