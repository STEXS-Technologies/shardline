//! Depth-limited Git traversal without changing verified commit/object bytes.
use super::*;
use crate::git::pack::ObjectType;
use std::collections::{HashMap, HashSet, VecDeque};

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(super) struct Request {
    boundaries: HashSet<String>,
    depth: Option<u32>,
    since: Option<u64>,
    exclude_refs: Vec<String>,
    excluded_roots: HashSet<String>,
    relative: bool,
}

impl Request {
    pub(super) fn parse(lines: &[Vec<u8>]) -> Result<Self, HubApiError> {
        let invalid = || HubApiError::BadRequest("invalid Git shallow request".to_owned());
        let mut request = Self::default();
        let mut depth_seen = false;
        for line in lines {
            let line = std::str::from_utf8(line).map_err(|_error| invalid())?;
            let fields: Vec<_> = line.split_whitespace().collect();
            match fields.first().copied() {
                Some("shallow") => {
                    let [_, boundary] = fields.as_slice() else {
                        return Err(invalid());
                    };
                    if boundary.len() != 40
                        || !boundary.bytes().all(|byte| byte.is_ascii_hexdigit())
                    {
                        return Err(invalid());
                    }
                    request.boundaries.insert(boundary.to_ascii_lowercase());
                }
                Some("deepen") => {
                    if depth_seen || fields.len() != 2 {
                        return Err(invalid());
                    }
                    depth_seen = true;
                    let depth: u32 = fields
                        .get(1)
                        .ok_or_else(invalid)?
                        .parse()
                        .map_err(|_error| invalid())?;
                    if depth > i32::MAX as u32 {
                        return Err(invalid());
                    }
                    request.depth = (depth > 0).then_some(depth);
                }
                Some("deepen-since") => {
                    let [_, timestamp] = fields.as_slice() else {
                        return Err(invalid());
                    };
                    if request.since.is_some()
                        || !timestamp.bytes().all(|byte| byte.is_ascii_digit())
                    {
                        return Err(invalid());
                    }
                    let timestamp = timestamp.parse().map_err(|_error| invalid())?;
                    if matches!(timestamp, 0 | u64::MAX) {
                        return Err(invalid());
                    }
                    request.since = Some(timestamp);
                }
                Some("deepen-not") => {
                    let [_, reference] = fields.as_slice() else {
                        return Err(invalid());
                    };
                    if request.exclude_refs.len() >= 10_000 || reference.len() > 1024 {
                        return Err(invalid());
                    }
                    request.exclude_refs.push((*reference).to_owned());
                }
                Some("want") => {
                    request.relative |= fields
                        .iter()
                        .skip(2)
                        .any(|field| *field == "deepen-relative");
                }
                _ => {}
            }
        }
        if (request.relative && request.depth.is_none())
            || (depth_seen && (request.since.is_some() || !request.exclude_refs.is_empty()))
        {
            return Err(invalid());
        }
        Ok(request)
    }

    pub(super) async fn resolve_exclusions(
        &mut self,
        state: &HubState,
        repo_id: &str,
        identities: &HashMap<String, String>,
    ) -> Result<(), HubApiError> {
        if self.exclude_refs.is_empty() {
            return Ok(());
        }
        let refs = super::super::ref_advertisement::collect_refs(state, repo_id).await?;
        self.resolve_refs(&refs, identities)
    }

    fn resolve_refs(
        &mut self,
        refs: &[super::super::ref_advertisement::GitRef],
        identities: &HashMap<String, String>,
    ) -> Result<(), HubApiError> {
        let refs_by_name: HashMap<_, _> = refs
            .iter()
            .map(|reference| (reference.name.as_str(), reference))
            .collect();
        for requested in &self.exclude_refs {
            let aliases = [
                requested.clone(),
                format!("refs/{requested}"),
                format!("refs/tags/{requested}"),
                format!("refs/heads/{requested}"),
                format!("refs/remotes/{requested}"),
                format!("refs/remotes/{requested}/HEAD"),
            ];
            let mut matches = aliases
                .iter()
                .filter_map(|alias| refs_by_name.get(alias.as_str()));
            let reference = matches.next().ok_or_else(|| {
                HubApiError::BadRequest("unknown Git shallow exclusion ref".to_owned())
            })?;
            if matches.next().is_some() {
                return Err(HubApiError::BadRequest(
                    "ambiguous Git shallow exclusion ref".to_owned(),
                ));
            }
            let id = identities.get(&reference.sha1).ok_or_else(|| {
                HubApiError::CasError("Git exclusion ref changed during projection".to_owned())
            })?;
            self.excluded_roots.insert(id.clone());
        }
        Ok(())
    }
}

fn parents(object: &GitObject) -> Result<Vec<String>, HubApiError> {
    let text = std::str::from_utf8(&object.data)
        .map_err(|error| HubApiError::CasError(error.to_string()))?;
    let headers = text.split_once("\n\n").map_or(text, |(headers, _)| headers);
    Ok(headers
        .lines()
        .filter_map(|line| line.strip_prefix("parent ").map(str::to_owned))
        .collect())
}

fn commit_time(object: &GitObject) -> Result<u64, HubApiError> {
    let invalid = || HubApiError::CasError("invalid Git committer timestamp".to_owned());
    let text = std::str::from_utf8(&object.data).map_err(|_error| invalid())?;
    let headers = text.split_once("\n\n").map_or(text, |(headers, _)| headers);
    let mut committers = headers
        .lines()
        .filter_map(|line| line.strip_prefix("committer "));
    let committer = committers.next().ok_or_else(invalid)?;
    if committers.next().is_some() {
        return Err(invalid());
    }
    committer
        .split_whitespace()
        .rev()
        .nth(1)
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_error| invalid())
}

pub(super) fn negotiate(
    objects: Vec<GitObject>,
    wants: &[String],
    haves: &[String],
    request: &Request,
) -> Result<(Vec<GitObject>, Vec<u8>), HubApiError> {
    let by_id: HashMap<_, _> = objects
        .iter()
        .map(|object| (hex::encode(object.sha1()), object))
        .collect();
    // A depth cut is intentional; a missing or mistyped parent is corruption.
    // Validate before cutting so a shallow request cannot conceal broken history.
    for object in &objects {
        if object.object_type == ObjectType::Commit {
            for parent in parents(object)? {
                if !by_id
                    .get(&parent)
                    .is_some_and(|ancestor| ancestor.object_type == ObjectType::Commit)
                {
                    return Err(HubApiError::CasError(
                        "missing or invalid Git parent commit".to_owned(),
                    ));
                }
            }
        }
    }
    for boundary in &request.boundaries {
        if !by_id
            .get(boundary)
            .is_some_and(|object| object.object_type == ObjectType::Commit)
        {
            return Err(HubApiError::BadRequest(
                "shallow commit is not in this repository".to_owned(),
            ));
        }
    }
    if !request.exclude_refs.is_empty() && request.excluded_roots.is_empty() {
        return Err(HubApiError::CasError(
            "unresolved Git shallow exclusions".to_owned(),
        ));
    }
    let mut excluded = HashSet::new();
    let mut exclude_pending: Vec<_> = request.excluded_roots.iter().cloned().collect();
    while let Some(id) = exclude_pending.pop() {
        if excluded.insert(id.clone()) {
            let object = by_id
                .get(&id)
                .ok_or_else(|| HubApiError::CasError("missing Git exclusion commit".to_owned()))?;
            exclude_pending.extend(parents(object)?);
        }
    }
    let mut selected = HashSet::new();
    let mut budgets = HashMap::<String, Option<u32>>::new();
    let mut pending: VecDeque<_> = wants
        .iter()
        .cloned()
        .map(|id| {
            (
                id,
                if request.relative {
                    None
                } else {
                    request.depth
                },
            )
        })
        .collect();
    while let Some((id, mut budget)) = pending.pop_front() {
        if excluded.contains(&id) {
            continue;
        }
        let Some(object) = by_id.get(&id) else {
            continue;
        };
        if object.object_type != ObjectType::Commit {
            continue;
        }
        if let Some(since) = request.since
            && commit_time(object)? < since
        {
            continue;
        }
        if request.boundaries.contains(&id) {
            if request.relative {
                budget = request.depth.map(|depth| depth.saturating_add(1));
            } else if request.depth.is_none()
                && request.since.is_none()
                && request.exclude_refs.is_empty()
            {
                budget = Some(1);
            }
        }
        if let Some(previous) = budgets.get(&id)
            && (previous.is_none() || (budget.is_some() && *previous >= budget))
        {
            continue;
        }
        budgets.insert(id.clone(), budget);
        selected.insert(id);
        if budget != Some(1) {
            for parent in parents(object)? {
                pending.push_back((parent, budget.map(|value| value.saturating_sub(1))));
            }
        }
    }
    let mut updates = Vec::new();
    if request.depth.is_some() || request.since.is_some() || !request.exclude_refs.is_empty() {
        if selected.is_empty() {
            return Err(HubApiError::BadRequest(
                "no commits selected for shallow request".to_owned(),
            ));
        }
        let mut ordered: Vec<_> = selected.iter().collect();
        ordered.sort_unstable();
        for id in ordered {
            let object = by_id
                .get(id)
                .ok_or_else(|| HubApiError::CasError("missing selected Git commit".to_owned()))?;
            let parents = parents(object)?;
            let incomplete = parents.iter().any(|parent| !selected.contains(parent));
            if incomplete && !request.boundaries.contains(id) {
                updates.extend_from_slice(
                    pktline::encode_line(&format!("shallow {id}\n"))?.as_bytes(),
                );
            } else if !incomplete && request.boundaries.contains(id) {
                updates.extend_from_slice(
                    pktline::encode_line(&format!("unshallow {id}\n"))?.as_bytes(),
                );
            }
        }
        updates.extend_from_slice(FLUSH.as_bytes());
    }
    let reachable = |roots: &[String], known: bool| -> Result<HashSet<String>, HubApiError> {
        let mut visited = HashSet::new();
        let mut pending_objects: Vec<_> = roots
            .iter()
            .filter(|id| {
                by_id.get(*id).is_some_and(|object| {
                    known || selected.contains(*id) || object.object_type != ObjectType::Commit
                })
            })
            .cloned()
            .collect();
        let mut discovered: HashSet<_> = pending_objects.iter().cloned().collect();
        while let Some(id) = pending_objects.pop() {
            visited.insert(id.clone());
            if let Some(object) = by_id.get(&id) {
                for child in super::super::projection::object_children(object)? {
                    if let Some(child_object) = by_id.get(&child) {
                        if child_object.object_type == ObjectType::Commit
                            && (if known {
                                request.boundaries.contains(&id)
                            } else {
                                !selected.contains(&child)
                            })
                        {
                            continue;
                        }
                        if discovered.insert(child.clone()) {
                            pending_objects.push(child);
                        }
                    }
                }
            }
        }
        Ok(visited)
    };
    let wanted = reachable(wants, false)?;
    let known = reachable(haves, true)?;
    Ok((
        objects
            .into_iter()
            .filter(|object| {
                let id = hex::encode(object.sha1());
                wanted.contains(&id)
                    && (!known.contains(&id)
                        || (object.object_type != ObjectType::Commit && wants.contains(&id)))
            })
            .collect(),
        updates,
    ))
}
