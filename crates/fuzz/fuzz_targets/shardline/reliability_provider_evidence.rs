#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    ProviderEvidenceLog, ProviderLifecycleObservations, ProviderLifecycleSnapshot,
    ProviderRepositoryIdentity, verify_provider_lifecycle_events,
};

fn snapshot(input: &[u8], revision: Option<String>) -> Option<ProviderLifecycleSnapshot> {
    ProviderLifecycleSnapshot::from_parts(
        ProviderRepositoryIdentity::new("github", "fuzz-owner", "fuzz-repository"),
        ProviderLifecycleObservations::new(
            Some(u64::from(input.first().copied().unwrap_or_default())),
            Some(u64::from(input.get(1).copied().unwrap_or_default())),
            revision,
            Some(u64::from(input.get(2).copied().unwrap_or_default())),
            Some(u64::from(input.get(3).copied().unwrap_or_default())),
            Some(u64::from(input.get(4).copied().unwrap_or_default())),
        ),
    )
    .ok()
}

fuzz_target!(|input: &[u8]| {
    if let Ok(decoded) = serde_json::from_slice::<ProviderEvidenceLog>(input) {
        let expected = snapshot(input, Some("decoded".to_owned()));
        if let Some(expected) = expected {
            drop(verify_provider_lifecycle_events(
                decoded.events(),
                &expected,
            ));
        }
    }

    let first = snapshot(input, Some(format!("revision-{}", input.len())));
    let second = snapshot(input, Some(format!("revision-{}-next", input.len())));
    let (Some(first), Some(second)) = (first, second) else {
        return;
    };
    let Ok(mut evidence) = ProviderEvidenceLog::baseline(first) else {
        return;
    };
    if evidence.record(second.clone()).is_err() {
        return;
    }
    assert!(verify_provider_lifecycle_events(evidence.events(), &second).is_ok());

    let Ok(encoded) = serde_json::to_vec(&evidence) else {
        return;
    };
    let Ok(decoded) = serde_json::from_slice::<ProviderEvidenceLog>(&encoded) else {
        return;
    };
    assert!(verify_provider_lifecycle_events(decoded.events(), &second).is_ok());
});
