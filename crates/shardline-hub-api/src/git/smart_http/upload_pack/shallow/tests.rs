use super::*;

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
