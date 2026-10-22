use super::*;

fn entry(mode: &str, name: &str, object: &GitObject) -> Vec<u8> {
    let mut bytes = format!("{mode} {name}\0").into_bytes();
    bytes.extend_from_slice(&object.sha1());
    bytes
}

#[test]
fn tree_filter_uses_minimum_depth_and_preserves_explicit_objects() {
    let blob = GitObject::blob(b"shared bytes".to_vec());
    let nested = GitObject::tree(entry("100644", "file", &blob));
    let mut root_data = entry("100644", "direct", &blob);
    root_data.extend(entry("40000", "nested", &nested));
    let root = GitObject::tree(root_data);
    let root_id = hex::encode(root.sha1());
    let nested_id = hex::encode(nested.sha1());
    let blob_id = hex::encode(blob.sha1());
    let commit = GitObject::commit(format!("tree {root_id}\n\ncommit").into_bytes());
    let objects = vec![blob.clone(), nested, root, commit];
    let zero = Filter::parse(&[b"filter tree:0".to_vec()]).unwrap();
    let selected = zero.apply(objects.clone(), &[]).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].object_type, ObjectType::Commit);
    let one = Filter::parse(&[b"filter tree:1".to_vec()]).unwrap();
    let selected = one.apply(objects.clone(), &[]).unwrap();
    assert_eq!(selected.len(), 2);
    assert!(
        selected
            .iter()
            .any(|object| hex::encode(object.sha1()) == root_id)
    );
    let two = Filter::parse(&[b"filter tree:2".to_vec()]).unwrap();
    assert_eq!(two.apply(objects.clone(), &[]).unwrap().len(), 4);
    // A separately requested subtree becomes a root at depth zero.
    let selected = one.apply(objects.clone(), &[nested_id]).unwrap();
    assert_eq!(selected.len(), 3);
    assert!(
        !selected
            .iter()
            .any(|object| object.object_type == ObjectType::Blob)
    );
    assert!(
        zero.apply(objects, &[blob_id])
            .unwrap()
            .iter()
            .any(|object| object.data == blob.data)
    );
}

#[test]
fn blob_omission_preserves_commits_and_explicitly_wanted_blobs() {
    let blob = GitObject::blob(b"verified bytes".to_vec());
    let id = hex::encode(blob.sha1());
    let commit = GitObject::commit(b"author fixture\n\ncommit".to_vec());
    let filter = Filter::parse(&[b"filter blob:none".to_vec()]).unwrap();
    let selected = filter.apply(vec![blob.clone(), commit], &[]).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].object_type, ObjectType::Commit);
    let selected = filter.apply(vec![blob.clone()], &[id]).unwrap();
    assert_eq!(selected[0].data, blob.data);
}

#[test]
fn blob_limits_use_exclusive_threshold_and_checked_binary_units() {
    for (spec, expected) in [
        ("blob:limit=1024", 1024),
        ("blob:limit=1k", 1024),
        ("blob:limit=2M", 2097152),
        ("blob:limit=1g", 1073741824),
        ("blob:limit=0", 0),
    ] {
        assert_eq!(
            Filter::parse(&[format!("filter {spec}").into_bytes()])
                .unwrap()
                .blob_limit,
            Some(expected)
        );
    }
    let filter = Filter::parse(&[b"filter blob:limit=4".to_vec()]).unwrap();
    let objects = vec![GitObject::blob(vec![0; 3]), GitObject::blob(vec![0; 4])];
    let selected = filter.apply(objects, &[]).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].data.len(), 3);
}

#[test]
fn malformed_duplicate_and_unimplemented_filters_fail_explicitly() {
    for lines in [
        vec!["filter"],
        vec!["filter blob:none extra"],
        vec!["filter blob:none", "filter blob:none"],
        vec!["filter blob:limit=-1"],
        vec!["filter blob:limit=18446744073709551615g"],
        vec!["filter blob:limit=k"],
        vec!["filter tree:-1"],
        vec!["filter tree:18446744073709551616"],
        vec!["filter sparse:path=/etc/passwd"],
        vec!["filter combine:blob:none+tree:1"],
    ] {
        assert!(
            Filter::parse(
                &lines
                    .into_iter()
                    .map(|line| line.as_bytes().to_vec())
                    .collect::<Vec<_>>()
            )
            .is_err()
        );
    }
}
