use super::*;

#[test]
fn negotiation_reads_haves_after_want_flush() {
    let wanted = "1".repeat(40);
    let known = "2".repeat(40);
    let body = format!(
        "{}0000{}{}",
        pktline::encode_line(&format!("want {wanted}\n")).unwrap(),
        pktline::encode_line(&format!("have {known}\n")).unwrap(),
        pktline::encode_line("done\n").unwrap()
    );
    let lines = decode_negotiation(body.as_bytes()).unwrap();
    assert_eq!(parse_wants(&lines), vec![wanted]);
    assert_eq!(parse_haves(&lines), vec![known]);
    assert!(decode_negotiation(b"0003").is_err());
    assert!(decode_negotiation(b"0010short").is_err());
}

#[test]
fn incremental_fetch_excludes_known_history_and_unrelated_objects() {
    let old = GitObject::commit(b"author old\n\nold".to_vec());
    let old_id = hex::encode(old.sha1());
    let new = GitObject::commit(format!("parent {old_id}\n\nnew").into_bytes());
    let new_id = hex::encode(new.sha1());
    let unrelated = GitObject::blob(b"unrelated".to_vec());
    let selected = negotiated_objects(
        vec![old, new, unrelated],
        std::slice::from_ref(&new_id),
        &[old_id],
    )
    .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(hex::encode(selected[0].sha1()), new_id);
}

#[test]
fn unknown_haves_do_not_hide_wanted_objects() {
    let object = GitObject::commit(b"author a\n\ncommit".to_vec());
    let id = hex::encode(object.sha1());
    let selected = negotiated_objects(vec![object], &[id], &["0".repeat(40)]).unwrap();
    assert_eq!(selected.len(), 1);
}
