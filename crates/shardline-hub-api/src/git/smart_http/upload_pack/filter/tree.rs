//! Minimum object depth across every selected commit and explicit object root.
use super::*;
use std::collections::{HashMap, VecDeque};

pub(super) fn depths(
    objects: &[GitObject],
    wants: &[String],
) -> Result<HashMap<String, u64>, HubApiError> {
    let by_id: HashMap<_, _> = objects
        .iter()
        .map(|object| (hex::encode(object.sha1()), object))
        .collect();
    let mut pending = VecDeque::new();
    for object in objects {
        if object.object_type == ObjectType::Commit {
            for child in super::super::super::projection::object_children(object)? {
                if by_id
                    .get(&child)
                    .is_some_and(|child| child.object_type != ObjectType::Commit)
                {
                    pending.push_back((child, 0u64));
                }
            }
        }
    }
    for wanted in wants {
        if by_id
            .get(wanted)
            .is_some_and(|object| matches!(object.object_type, ObjectType::Tree | ObjectType::Blob))
        {
            pending.push_back((wanted.clone(), 0));
        }
    }
    let mut depths = HashMap::new();
    while let Some((id, depth)) = pending.pop_front() {
        if depths.get(&id).is_some_and(|previous| *previous <= depth) {
            continue;
        }
        depths.insert(id.clone(), depth);
        if let Some(object) = by_id.get(&id)
            && object.object_type == ObjectType::Tree
        {
            for child in super::super::super::projection::object_children(object)? {
                if by_id.contains_key(&child) {
                    pending.push_back((child, depth.saturating_add(1)));
                }
            }
        }
    }
    Ok(depths)
}
