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
fn object_type_filter_intersects_and_explicit_wants_override_it() {
    let blob = GitObject::blob(b"selected blob".to_vec());
    let tree = GitObject::tree(entry("100644", "file", &blob));
    let commit = GitObject::commit(
        format!("tree {}\n\nobject type fixture", hex::encode(tree.sha1())).into_bytes(),
    );
    let tag = GitObject {
        object_type: ObjectType::Tag,
        data: format!(
            "object {}\ntype commit\ntag v1\n\nfixture",
            hex::encode(commit.sha1())
        )
        .into_bytes(),
    };
    let requested_commit = hex::encode(commit.sha1());
    let objects = vec![blob, tree, commit, tag];
    for (kind, expected) in [
        ("blob", ObjectType::Blob),
        ("tree", ObjectType::Tree),
        ("commit", ObjectType::Commit),
        ("tag", ObjectType::Tag),
    ] {
        let filter = Filter::parse(&[format!("filter object:type={kind}").into_bytes()]).unwrap();
        let selected = filter.apply(objects.clone(), &[]).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].object_type, expected);
    }

    let intersected =
        Filter::parse(&[b"filter combine:object%3Atype%3Dblob+blob%3Alimit%3D32".to_vec()])
            .unwrap();
    let selected = intersected.apply(objects.clone(), &[]).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].object_type, ObjectType::Blob);

    let selected = Filter::parse(&[b"filter object:type=blob".to_vec()])
        .unwrap()
        .apply(objects, std::slice::from_ref(&requested_commit))
        .unwrap();
    assert!(
        selected
            .iter()
            .any(|object| object.object_type == ObjectType::Blob)
    );
    assert!(
        selected
            .iter()
            .any(|object| hex::encode(object.sha1()) == requested_commit)
    );
    assert!(selected.iter().all(|object| {
        object.object_type == ObjectType::Blob || hex::encode(object.sha1()) == requested_commit
    }));
}

#[test]
fn combined_blob_and_tree_filters_apply_intersection() {
    let small_blob = GitObject::blob(b"small".to_vec());
    let large_blob = GitObject::blob(vec![b'x'; 16]);
    let nested_tree = GitObject::tree(entry("100644", "large", &large_blob));
    let mut root_data = entry("100644", "small", &small_blob);
    root_data.extend(entry("40000", "nested", &nested_tree));
    let root_tree = GitObject::tree(root_data);
    let commit = GitObject::commit(
        format!(
            "tree {}\n\ncombined filter fixture",
            hex::encode(root_tree.sha1())
        )
        .into_bytes(),
    );
    let filter = Filter::parse(&[b"filter combine:blob%3Alimit%3D16+tree%3A2".to_vec()]).unwrap();
    let selected = filter
        .apply(
            vec![
                small_blob.clone(),
                large_blob.clone(),
                nested_tree.clone(),
                root_tree.clone(),
                commit.clone(),
            ],
            &[],
        )
        .unwrap();

    assert!(selected.iter().any(|object| object.sha1() == commit.sha1()));
    assert!(
        selected
            .iter()
            .any(|object| object.sha1() == root_tree.sha1())
    );
    assert!(
        selected
            .iter()
            .any(|object| object.sha1() == small_blob.sha1())
    );
    assert!(
        selected
            .iter()
            .any(|object| object.sha1() == nested_tree.sha1())
    );
    assert!(
        selected
            .iter()
            .all(|object| object.object_type != ObjectType::Blob || object.data.len() < 16)
    );
    assert!(
        !selected
            .iter()
            .any(|object| object.sha1() == large_blob.sha1())
    );
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
        vec!["filter sparse:oid=0123456789012345678901234567890123456789"],
        vec!["filter object:type=invalid"],
        vec!["filter combine:blob:none"],
        vec!["filter combine:blob:none+"],
        vec!["filter combine:blob%3"],
        vec!["filter combine:blob%3Anone%20+tree%3A1"],
        vec!["filter combine:blob%3Aunknown+tree%3A1"],
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
    let too_many = format!(
        "filter combine:{}",
        std::iter::repeat_n("blob:none", 17)
            .collect::<Vec<_>>()
            .join("+")
    );
    assert!(Filter::parse(&[too_many.into_bytes()]).is_err());
}
