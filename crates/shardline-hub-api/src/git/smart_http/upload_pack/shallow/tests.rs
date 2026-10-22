use super::*;

#[test]
fn exclusion_resolution_rejects_unknown_ambiguous_and_changed_refs() {
    use super::super::super::ref_advertisement::GitRef;
    let refs = vec![
        GitRef {
            name: "refs/heads/base".to_owned(),
            sha1: "hub-one".to_owned(),
        },
        GitRef {
            name: "refs/tags/base".to_owned(),
            sha1: "hub-two".to_owned(),
        },
    ];
    let identities = HashMap::from([
        ("hub-one".to_owned(), "1".repeat(40)),
        ("hub-two".to_owned(), "2".repeat(40)),
    ]);
    for name in ["missing", "base"] {
        let mut request = Request::parse(&[format!("deepen-not {name}").into_bytes()]).unwrap();
        assert!(request.resolve_refs(&refs, &identities).is_err());
    }
    let mut request = Request::parse(&[b"deepen-not refs/tags/base".to_vec()]).unwrap();
    assert!(request.resolve_refs(&refs, &HashMap::new()).is_err());
    request.resolve_refs(&refs, &identities).unwrap();
    assert_eq!(request.excluded_roots, HashSet::from(["2".repeat(40)]));
}

#[test]
fn exclusion_removes_all_baseline_ancestry_and_combines_with_date() {
    use super::super::super::ref_advertisement::GitRef;
    let root = GitObject::commit(b"committer Root <r@example.com> 100 +0000\n\nroot".to_vec());
    let root_id = hex::encode(root.sha1());
    let base = GitObject::commit(
        format!("parent {root_id}\ncommitter Base <b@example.com> 200 +0000\n\nbase").into_bytes(),
    );
    let base_id = hex::encode(base.sha1());
    let tip = GitObject::commit(
        format!("parent {base_id}\ncommitter Tip <t@example.com> 300 +0000\n\ntip").into_bytes(),
    );
    let tip_id = hex::encode(tip.sha1());
    let mut request = Request::parse(&[
        b"deepen-not baseline".to_vec(),
        b"deepen-since 250".to_vec(),
    ])
    .unwrap();
    request
        .resolve_refs(
            &[GitRef {
                name: "refs/tags/baseline".to_owned(),
                sha1: "hub-base".to_owned(),
            }],
            &HashMap::from([("hub-base".to_owned(), base_id)]),
        )
        .unwrap();
    let (selected, updates) = negotiate(
        vec![root, base, tip],
        std::slice::from_ref(&tip_id),
        &[],
        &request,
    )
    .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(hex::encode(selected[0].sha1()), tip_id);
    assert!(
        String::from_utf8(updates)
            .unwrap()
            .contains(&format!("shallow {tip_id}\n"))
    );
}

#[test]
fn date_selection_rejects_missing_duplicate_or_invalid_committer_time() {
    for bytes in [
        b"author A <a@example.com> 100 +0000\n\nmissing".as_slice(),
        b"committer A <a@example.com> nope +0000\n\ninvalid".as_slice(),
        b"committer A <a@example.com> 100 +0000\ncommitter B <b@example.com> 200 +0000\n\nduplicate".as_slice(),
    ] {
        let commit = GitObject::commit(bytes.to_vec());
        let id = hex::encode(commit.sha1());
        let request = Request::parse(&[b"deepen-since 1".to_vec()]).unwrap();
        assert!(negotiate(vec![commit], &[id], &[], &request).is_err());
    }
}

#[test]
fn date_cut_uses_committer_time_and_keeps_exact_boundary_timestamp() {
    let root = GitObject::commit(b"committer Root <r@example.com> 100 +0000\n\nroot".to_vec());
    let root_id = hex::encode(root.sha1());
    let middle = GitObject::commit(format!("parent {root_id}\nauthor Other <o@example.com> 999 +0000\ncommitter Middle <m@example.com> 200 -0300\n\nmiddle").into_bytes());
    let middle_id = hex::encode(middle.sha1());
    let tip = GitObject::commit(
        format!("parent {middle_id}\ncommitter Tip <t@example.com> 300 +0200\n\ntip").into_bytes(),
    );
    let tip_id = hex::encode(tip.sha1());
    let objects = vec![root, middle, tip];
    let request = Request::parse(&[b"deepen-since 200".to_vec()]).unwrap();
    let (selected, updates) = negotiate(
        objects.clone(),
        std::slice::from_ref(&tip_id),
        &[],
        &request,
    )
    .unwrap();
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].data, objects[1].data);
    assert!(
        String::from_utf8(updates)
            .unwrap()
            .contains(&format!("shallow {middle_id}\n"))
    );
    let empty = Request::parse(&[b"deepen-since 301".to_vec()]).unwrap();
    assert!(negotiate(objects, &[tip_id], &[], &empty).is_err());
}

#[test]
fn shallow_cut_cannot_conceal_missing_or_mistyped_parent() {
    let blob = GitObject::blob(b"not a commit".to_vec());
    for (parent_id, extra) in [
        ("f".repeat(40), None),
        (hex::encode(blob.sha1()), Some(blob)),
    ] {
        let tip = GitObject::commit(format!("parent {parent_id}\n\ntip").into_bytes());
        let tip_id = hex::encode(tip.sha1());
        let mut objects = vec![tip];
        objects.extend(extra);
        let request = Request::parse(&[b"deepen 1".to_vec()]).unwrap();
        assert!(negotiate(objects, &[tip_id], &[], &request).is_err());
    }
}

#[test]
fn independent_wants_keep_shared_ancestor_when_reachable_with_larger_budget() {
    let root = GitObject::commit(b"author root\n\nroot".to_vec());
    let root_id = hex::encode(root.sha1());
    let parent = GitObject::commit(format!("parent {root_id}\n\nparent").into_bytes());
    let parent_id = hex::encode(parent.sha1());
    let tip = GitObject::commit(format!("parent {parent_id}\n\ntip").into_bytes());
    let tip_id = hex::encode(tip.sha1());
    let request = Request::parse(&[b"deepen 2".to_vec()]).unwrap();
    let (selected, updates) =
        negotiate(vec![root, parent, tip], &[tip_id, parent_id], &[], &request).unwrap();
    assert_eq!(selected.len(), 3);
    assert_eq!(updates, b"0000");
}

#[test]
fn merge_depth_keeps_both_parents_and_deduplicates_shared_ancestry() {
    let root = GitObject::commit(b"author root\n\nroot".to_vec());
    let root_id = hex::encode(root.sha1());
    let left = GitObject::commit(format!("parent {root_id}\n\nleft").into_bytes());
    let right = GitObject::commit(format!("parent {root_id}\n\nright").into_bytes());
    let left_id = hex::encode(left.sha1());
    let right_id = hex::encode(right.sha1());
    let merge =
        GitObject::commit(format!("parent {left_id}\nparent {right_id}\n\nmerge").into_bytes());
    let merge_id = hex::encode(merge.sha1());
    let objects = vec![root, left, right, merge];
    let depth = Request::parse(&[b"deepen 2".to_vec()]).unwrap();
    let (selected, updates) = negotiate(
        objects.clone(),
        std::slice::from_ref(&merge_id),
        &[],
        &depth,
    )
    .unwrap();
    let selected_ids: HashSet<_> = selected
        .iter()
        .map(|object| hex::encode(object.sha1()))
        .collect();
    assert_eq!(
        selected_ids,
        HashSet::from([merge_id.clone(), left_id.clone(), right_id.clone()])
    );
    let updates = String::from_utf8(updates).unwrap();
    assert!(updates.contains(&format!("shallow {left_id}\n")));
    assert!(updates.contains(&format!("shallow {right_id}\n")));
    assert!(!updates.contains(&format!("shallow {merge_id}\n")));
    let deeper = Request::parse(&[b"deepen 3".to_vec()]).unwrap();
    let (selected, updates) = negotiate(objects, &[merge_id], &[], &deeper).unwrap();
    assert_eq!(selected.len(), 4);
    assert_eq!(
        selected
            .iter()
            .filter(|object| hex::encode(object.sha1()) == root_id)
            .count(),
        1
    );
    assert_eq!(updates, b"0000");
}

#[test]
fn depth_and_relative_deepening_preserve_commit_bytes() {
    let root = GitObject::commit(b"author root\n\nroot".to_vec());
    let root_id = hex::encode(root.sha1());
    let middle = GitObject::commit(format!("parent {root_id}\n\nmiddle").into_bytes());
    let middle_id = hex::encode(middle.sha1());
    let tip = GitObject::commit(format!("parent {middle_id}\n\ntip").into_bytes());
    let tip_id = hex::encode(tip.sha1());
    let objects = vec![root, middle, tip];
    let initial = Request::parse(&[b"deepen 1".to_vec()]).unwrap();
    let (selected, updates) = negotiate(
        objects.clone(),
        std::slice::from_ref(&tip_id),
        &[],
        &initial,
    )
    .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].data, objects[2].data);
    assert_eq!(
        updates,
        format!(
            "{}0000",
            pktline::encode_line(&format!("shallow {tip_id}\n")).unwrap()
        )
        .into_bytes()
    );

    let relative = Request::parse(&[
        format!("want {tip_id} deepen-relative").into_bytes(),
        format!("shallow {tip_id}").into_bytes(),
        b"deepen 1".to_vec(),
    ])
    .unwrap();
    let (selected, updates) = negotiate(
        objects,
        std::slice::from_ref(&tip_id),
        std::slice::from_ref(&tip_id),
        &relative,
    )
    .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(hex::encode(selected[0].sha1()), middle_id);
    let updates = String::from_utf8(updates).unwrap();
    assert!(updates.contains(&format!("unshallow {tip_id}\n")));
    assert!(updates.contains(&format!("shallow {middle_id}\n")));
    assert!(!updates.contains(&root_id));
}

#[test]
fn rejects_ambiguous_or_invalid_depth_requests() {
    for lines in [
        vec!["deepen -1"],
        vec!["deepen 2147483648"],
        vec!["deepen 1 trailing"],
        vec!["deepen 1", "deepen 2"],
        vec!["deepen-since -1"],
        vec!["deepen-since 0"],
        vec!["deepen-since 18446744073709551615"],
        vec!["deepen-since 18446744073709551616"],
        vec!["deepen-since 1 trailing"],
        vec!["deepen-since 1", "deepen-since 2"],
        vec!["deepen-since 1", "deepen 0"],
        vec!["deepen-not"],
        vec!["deepen-not one two"],
        vec!["deepen-not main", "deepen 1"],
        vec!["deepen 1", "deepen-since 1"],
        vec!["shallow invalid"],
        vec!["want 0000000000000000000000000000000000000000 deepen-relative"],
    ] {
        let lines: Vec<_> = lines
            .into_iter()
            .map(|line| line.as_bytes().to_vec())
            .collect();
        assert!(Request::parse(&lines).is_err());
    }
}

#[test]
fn accepts_zero_depth_and_normalizes_boundary_ids() {
    let request = Request::parse(&[
        b"deepen 0\n".to_vec(),
        b"shallow ABCDEF0123456789ABCDEF0123456789ABCDEF01\n".to_vec(),
    ])
    .unwrap();
    assert_eq!(request.depth, None);
    assert!(
        request
            .boundaries
            .contains("abcdef0123456789abcdef0123456789abcdef01")
    );
}
