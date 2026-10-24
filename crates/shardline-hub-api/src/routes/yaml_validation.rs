//! Bounded model-card syntax validation, independent of Hub catalog policy.
pub(super) const MAX_CARD_BYTES: usize = 256 * 1024;

pub(super) fn validate(body: &serde_json::Value) -> Result<(), String> {
    let content = body
        .get("content")
        .and_then(serde_json::Value::as_str)
        .ok_or("content must be a string")?;
    if content.len() > MAX_CARD_BYTES {
        return Err("card exceeds the 256 KiB validation limit".to_owned());
    }
    if let Some(kind) = body.get("repoType")
        && !matches!(kind.as_str(), Some("model" | "dataset" | "space"))
    {
        return Err("repoType must be model, dataset, or space".to_owned());
    }
    parse_metadata(content).map(|_| ())
}

/// Reads the same bounded metadata representation used by validation.
/// The markdown body is not part of the metadata budget.
pub(super) fn parse_metadata(content: &str) -> Result<Option<serde_json::Value>, String> {
    let mut lines = content.trim_start_matches('\u{feff}').trim_start().lines();
    if lines.next().map(str::trim) != Some("---") {
        return Ok(None);
    }
    let mut yaml = String::new();
    let mut closed = false;
    for line in lines {
        if line.trim() == "---" {
            closed = true;
            break;
        }
        if yaml.len().saturating_add(line.len()).saturating_add(1) > MAX_CARD_BYTES {
            return Err("YAML front matter exceeds the 256 KiB limit".to_owned());
        }
        yaml.push_str(line);
        yaml.push('\n');
    }
    if !closed {
        return Err("YAML front matter is missing its closing --- delimiter".to_owned());
    }
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_depth: 32, max_events: 16384, max_nodes: 8192,
            max_aliases: 32, max_anchors: 32, max_documents: 1,
            max_total_scalar_bytes: MAX_CARD_BYTES,
            max_recorded_anchor_bytes: MAX_CARD_BYTES, max_recorded_anchor_events: 16384,
        },
        alias_limits: serde_saphyr::alias_limits! {
            max_total_replayed_events: 16384, max_replay_stack_depth: 32,
            max_alias_expansions_per_anchor: 32,
        },
    };
    let value: serde_json::Value = serde_saphyr::from_str_with_options(&yaml, options)
        .map_err(|error| format!("invalid YAML front matter: {error}"))?;
    if !value.is_null() && !value.is_object() {
        return Err("YAML front matter must be a mapping".to_owned());
    }
    Ok(if value.as_object().is_some_and(|map| !map.is_empty()) {
        Some(value)
    } else {
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_nested_cards_and_rejects_malformed_or_excessive_yaml() {
        for card in [
            "# No metadata",
            "---\n---\n# Empty",
            "---\ntags:\n- rust\nmodel-index:\n- name: example\n  results: []\n---\n# Card",
        ] {
            assert!(validate(&serde_json::json!({"content":card,"repoType":"model"})).is_ok());
        }
        for card in [
            "---\ntags: [unfinished\n---",
            "---\ntags: example",
            "---\n- invalid-root\n---",
        ] {
            assert!(validate(&serde_json::json!({"content":card})).is_err());
        }
        let deep = format!("---\nx: {}0{}\n---", "[".repeat(64), "]".repeat(64));
        assert!(validate(&serde_json::json!({"content":deep})).is_err());
        let alias_bomb = format!(
            "---\nbase: &base [x, y]\nvalues: [{}]\n---",
            vec!["*base"; 64].join(",")
        );
        assert!(validate(&serde_json::json!({"content":alias_bomb})).is_err());
        assert!(
            validate(&serde_json::json!({"content":"---\nbase: &base [en]\nlanguage: *base\n---"}))
                .is_ok()
        );
        assert!(validate(&serde_json::json!({"content":"x".repeat(MAX_CARD_BYTES+1)})).is_err());
    }
}
