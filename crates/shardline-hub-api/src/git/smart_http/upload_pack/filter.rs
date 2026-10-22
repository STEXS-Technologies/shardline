//! Object omission changes transfer contents, never committed object bytes.
use super::*;
use crate::git::pack::ObjectType;

#[derive(Default)]
pub(super) struct Filter {
    blob_limit: Option<u64>,
    tree_depth: Option<u64>,
    object_types: Option<Vec<ObjectType>>,
}

impl Filter {
    pub(super) fn parse(lines: &[Vec<u8>]) -> Result<Self, HubApiError> {
        let invalid =
            || HubApiError::BadRequest("unsupported or invalid Git object filter".to_owned());
        let mut filter = Self::default();
        let mut seen = false;
        let mut leaves = 0usize;
        for line in lines {
            let line = std::str::from_utf8(line).map_err(|_error| invalid())?;
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.first().copied() != Some("filter") {
                continue;
            }
            let [_, spec] = fields.as_slice() else {
                return Err(invalid());
            };
            if seen || spec.len() > MAX_FILTER_SPEC_BYTES {
                return Err(invalid());
            }
            seen = true;
            parse_spec(spec, &mut filter, 0, &mut leaves)?;
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
                let type_allowed = self
                    .object_types
                    .as_ref()
                    .is_none_or(|types| types.contains(&object.object_type));
                (blob_allowed && tree_allowed && type_allowed) || explicit.contains(&id)
            })
            .collect())
    }
}

const MAX_FILTER_SPEC_BYTES: usize = 4096;
const MAX_COMBINED_FILTERS: usize = 16;
const MAX_COMBINE_NESTING: usize = 4;

fn parse_spec(
    spec: &str,
    filter: &mut Filter,
    nesting: usize,
    leaves: &mut usize,
) -> Result<(), HubApiError> {
    let invalid = || HubApiError::BadRequest("unsupported or invalid Git object filter".to_owned());
    if let Some(combined) = spec.strip_prefix("combine:") {
        if nesting >= MAX_COMBINE_NESTING {
            return Err(invalid());
        }
        let components = combined.split('+').collect::<Vec<_>>();
        if !(2..=MAX_COMBINED_FILTERS).contains(&components.len()) {
            return Err(invalid());
        }
        for component in components {
            if component.is_empty() {
                return Err(invalid());
            }
            let next_nesting = nesting.checked_add(1).ok_or_else(invalid)?;
            parse_spec(&decode_component(component)?, filter, next_nesting, leaves)?;
        }
        return Ok(());
    }

    *leaves = leaves.checked_add(1).ok_or_else(invalid)?;
    if *leaves > MAX_COMBINED_FILTERS {
        return Err(invalid());
    }

    if let Some(depth) = spec.strip_prefix("tree:") {
        if depth.is_empty() || !depth.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        let depth = depth.parse::<u64>().map_err(|_error| invalid())?;
        filter.tree_depth = Some(
            filter
                .tree_depth
                .map_or(depth, |current| current.min(depth)),
        );
        return Ok(());
    }

    if let Some(kind) = spec.strip_prefix("object:type=") {
        let kind = match kind {
            "commit" => ObjectType::Commit,
            "tree" => ObjectType::Tree,
            "blob" => ObjectType::Blob,
            "tag" => ObjectType::Tag,
            _ => return Err(invalid()),
        };
        let allowed = filter.object_types.get_or_insert_with(|| {
            vec![
                ObjectType::Commit,
                ObjectType::Tree,
                ObjectType::Blob,
                ObjectType::Tag,
            ]
        });
        allowed.retain(|current| *current == kind);
        return Ok(());
    }

    let limit = if spec == "blob:none" {
        0
    } else {
        let number = spec.strip_prefix("blob:limit=").ok_or_else(invalid)?;
        let (digits, multiplier) = match number.as_bytes().last() {
            Some(b'k' | b'K') => (number.strip_suffix(['k', 'K']).ok_or_else(invalid)?, 1024),
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
    };
    filter.blob_limit = Some(
        filter
            .blob_limit
            .map_or(limit, |current| current.min(limit)),
    );
    Ok(())
}

fn decode_component(component: &str) -> Result<String, HubApiError> {
    let invalid = || HubApiError::BadRequest("unsupported or invalid Git object filter".to_owned());
    let bytes = component.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut offset = 0usize;
    while offset < bytes.len() {
        let current = bytes.get(offset).copied().ok_or_else(invalid)?;
        if current == b'%' {
            let hi_offset = offset.checked_add(1).ok_or_else(invalid)?;
            let lo_offset = offset.checked_add(2).ok_or_else(invalid)?;
            let hi = bytes
                .get(hi_offset)
                .and_then(|byte| hex_nibble(*byte))
                .ok_or_else(invalid)?;
            let lo = bytes
                .get(lo_offset)
                .and_then(|byte| hex_nibble(*byte))
                .ok_or_else(invalid)?;
            let value = hi
                .checked_mul(16)
                .and_then(|value| value.checked_add(lo))
                .ok_or_else(invalid)?;
            if value <= 0x20 || value == 0x7f {
                return Err(invalid());
            }
            decoded.push(value);
            offset = offset.checked_add(3).ok_or_else(invalid)?;
        } else {
            decoded.push(current);
            offset = offset.checked_add(1).ok_or_else(invalid)?;
        }
    }
    String::from_utf8(decoded).map_err(|_error| invalid())
}

fn hex_nibble(byte: u8) -> Option<u8> {
    char::from(byte)
        .to_digit(16)
        .and_then(|digit| u8::try_from(digit).ok())
}

#[cfg(test)]
mod tests;
mod tree;
