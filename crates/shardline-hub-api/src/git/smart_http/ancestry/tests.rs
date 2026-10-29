#![allow(clippy::unwrap_used)]

use super::*;
use crate::git::{pack::GitObject, smart_http::projection::archive_objects};

fn commit(parents: &[&str], message: &str) -> GitObject {
    let mut text = format!("tree {}\n", "a".repeat(40));
    for parent in parents {
        text.push_str(&format!("parent {parent}\n"));
    }
    text.push_str(&format!("author Test <test@example.com> 1 +0000\ncommitter Test <test@example.com> 1 +0000\n\n{message}\n"));
    GitObject {
        object_type: ObjectType::Commit,
        data: text.into_bytes(),
    }
}

#[test]
fn ancestry_follows_both_merge_parents_and_never_reads_trees() {
    let (_root, state) = super::super::tests::make_hub_state();
    let auth = AuthorizedRepository::anonymous_full_access();
    let first = commit(&[], "first");
    let second = commit(&[], "second");
    let first_id = hex::encode(first.sha1());
    let second_id = hex::encode(second.sha1());
    let merge = commit(&[&first_id, &second_id], "merge");
    let head = hex::encode(merge.sha1());
    archive_objects(&state, "alice/repo", &head, &[first, second, merge], &auth).unwrap();
    // No tree archive exists: authorization must inspect only commit headers.
    for ancestor in [&first_id, &second_id] {
        assert!(contains(&state, "alice/repo", ancestor, &head, &auth).unwrap());
    }
    assert!(!contains(&state, "alice/repo", &"b".repeat(40), &head, &auth).unwrap());
    assert_eq!(
        walk(&state, "alice/repo", None, &head, &auth).unwrap(),
        HashSet::from([first_id.clone(), second_id, head.clone()])
    );
    assert!(contains(&state, "other/repo", &first_id, &head, &auth).is_err());
}

#[test]
fn ancestry_rejects_missing_and_malformed_parents() {
    let (_root, state) = super::super::tests::make_hub_state();
    let auth = AuthorizedRepository::anonymous_full_access();
    for parent in ["invalid".to_owned(), "b".repeat(40)] {
        let object = commit(&[&parent], "invalid ancestry");
        let head = hex::encode(object.sha1());
        archive_objects(&state, "alice/repo", &head, &[object], &auth).unwrap();
        assert!(contains(&state, "alice/repo", &"c".repeat(40), &head, &auth).is_err());
    }
}

#[test]
fn ancestry_ignores_message_edges_and_accepts_non_utf8_message_bytes() {
    let (_root, state) = super::super::tests::make_hub_state();
    let auth = AuthorizedRepository::anonymous_full_access();
    let parent = commit(&[], "parent");
    let parent_id = hex::encode(parent.sha1());
    let unrelated = "b".repeat(40);
    let mut head = commit(&[&parent_id], &format!("message\nparent {unrelated}"));
    head.data.push(0xff);
    let head_id = hex::encode(head.sha1());
    archive_objects(&state, "alice/repo", &head_id, &[parent, head], &auth).unwrap();
    assert!(contains(&state, "alice/repo", &parent_id, &head_id, &auth).unwrap());
    assert!(!contains(&state, "alice/repo", &unrelated, &head_id, &auth).unwrap());
}

#[test]
fn hub_revision_metadata_consumes_the_shared_history_byte_budget() {
    let (_root, state) = super::super::tests::make_hub_state();
    state
        .store
        .create_repo(
            shardline_index::hub::HubRepoType::Model,
            "alice/repo",
            false,
        )
        .unwrap();
    let head = state
        .store
        .resolve_revision("alice/repo", "main")
        .unwrap()
        .unwrap();
    let auth = AuthorizedRepository::anonymous_full_access();
    let mut exhausted = 0;
    assert!(parents(&state, "alice/repo", &head, &auth, &mut exhausted).is_err());
    let mut remaining = MAX_HISTORY_BYTES;
    parents(&state, "alice/repo", &head, &auth, &mut remaining).unwrap();
    assert!(remaining < MAX_HISTORY_BYTES);
}
