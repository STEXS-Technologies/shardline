use super::*;

#[test]
fn blob_omission_preserves_commits_and_explicitly_wanted_blobs() {
    let blob = GitObject::blob(b"verified bytes".to_vec());
    let id = hex::encode(blob.sha1());
    let commit = GitObject::commit(b"author fixture\n\ncommit".to_vec());
    let filter = Filter::parse(&[b"filter blob:none".to_vec()]).unwrap();
    let selected = filter.apply(vec![blob.clone(), commit], &[]);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].object_type, ObjectType::Commit);
    let selected = filter.apply(vec![blob.clone()], &[id]);
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
    let selected = filter.apply(objects, &[]);
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
        vec!["filter tree:0"],
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
