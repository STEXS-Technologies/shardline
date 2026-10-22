//! Object omission changes transfer contents, never committed object bytes.
use super::*;
use crate::git::pack::ObjectType;

#[derive(Default)]
pub(super) struct Filter {
    blob_limit: Option<u64>,
    tree_depth: Option<u64>,
}

impl Filter {
    pub(super) fn parse(lines: &[Vec<u8>]) -> Result<Self, HubApiError> {
        let invalid =
            || HubApiError::BadRequest("unsupported or invalid Git object filter".to_owned());
        let mut filter = Self::default();
        for line in lines {
            let line = std::str::from_utf8(line).map_err(|_error| invalid())?;
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.first().copied() != Some("filter") {
                continue;
            }
            let [_, spec] = fields.as_slice() else {
                return Err(invalid());
            };
            if filter.blob_limit.is_some() || filter.tree_depth.is_some() {
                return Err(invalid());
            }
            if let Some(depth) = spec.strip_prefix("tree:") {
                if depth.is_empty() || !depth.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(invalid());
                }
                filter.tree_depth = Some(depth.parse().map_err(|_error| invalid())?);
                continue;
            }
            filter.blob_limit = Some(if *spec == "blob:none" {
                0
            } else {
                let number = spec.strip_prefix("blob:limit=").ok_or_else(invalid)?;
                let (digits, multiplier) = match number.as_bytes().last() {
                    Some(b'k' | b'K') => {
                        (number.strip_suffix(['k', 'K']).ok_or_else(invalid)?, 1024)
                    }
                    Some(b'm' | b'M') => (
                        number.strip_suffix(['m', 'M']).ok_or_else(invalid)?,
                        1024u64.pow(2),
                    ),
                    Some(b'g' | b'G') => (
                        number.strip_suffix(['g', 'G']).ok_or_else(invalid)?,
                        1024u64.pow(3),
                    ),
                    _ => (number, 1),
                };
                if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(invalid());
                }
                digits
                    .parse::<u64>()
                    .map_err(|_error| invalid())?
                    .checked_mul(multiplier)
                    .ok_or_else(invalid)?
            });
        }
        Ok(filter)
    }

    pub(super) fn apply(
        &self,
        objects: Vec<GitObject>,
        wants: &[String],
    ) -> Result<Vec<GitObject>, HubApiError> {
        let explicit: std::collections::HashSet<_> = wants.iter().collect();
        let depths = if self.tree_depth.is_some() {
            tree::depths(&objects, wants)?
        } else {
            std::collections::HashMap::new()
        };
        Ok(objects
            .into_iter()
            .filter(|object| {
                let id = hex::encode(object.sha1());
                let blob_allowed = object.object_type != ObjectType::Blob
                    || self
                        .blob_limit
                        .is_none_or(|limit| (object.data.len() as u64) < limit);
                let tree_allowed =
                    !matches!(object.object_type, ObjectType::Blob | ObjectType::Tree)
                        || self.tree_depth.is_none_or(|limit| {
                            depths.get(&id).is_some_and(|depth| *depth < limit)
                        });
                (blob_allowed && tree_allowed) || explicit.contains(&id)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests;
mod tree;
