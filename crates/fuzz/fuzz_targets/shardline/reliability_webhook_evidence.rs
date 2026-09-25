#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    WebhookDeliveryEvidenceLog, WebhookDeliveryIdentity, WebhookDeliveryLifecycleState,
    WebhookDeliverySnapshot, verify_webhook_delivery_events,
};

fn snapshot(
    state: WebhookDeliveryLifecycleState,
    processed_at: u64,
) -> Option<WebhookDeliverySnapshot> {
    Some(WebhookDeliverySnapshot::new(
        WebhookDeliveryIdentity::new("github", "fuzz-owner", "fuzz-repo", "delivery-1").ok()?,
        processed_at,
        state,
    ))
}

fuzz_target!(|input: &[u8]| {
    if let Ok(decoded) = serde_json::from_slice::<WebhookDeliveryEvidenceLog>(input)
        && let Some(expected) = snapshot(
            WebhookDeliveryLifecycleState::Processed,
            u64::from(input.first().copied().unwrap_or_default()),
        )
    {
        drop(verify_webhook_delivery_events(decoded.events(), &expected));
    }

    let Some(processed) = snapshot(
        WebhookDeliveryLifecycleState::Processed,
        u64::from(input.first().copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Some(released) = snapshot(
        WebhookDeliveryLifecycleState::Released,
        u64::from(input.get(1).copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Some(recovered) = snapshot(
        WebhookDeliveryLifecycleState::Processed,
        u64::from(input.get(2).copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Ok(mut evidence) = WebhookDeliveryEvidenceLog::baseline(processed) else {
        return;
    };
    if evidence.record(released).is_err() || evidence.record(recovered.clone()).is_err() {
        return;
    }
    assert!(verify_webhook_delivery_events(evidence.events(), &recovered).is_ok());

    let Ok(encoded) = serde_json::to_vec(&evidence) else {
        return;
    };
    let Ok(decoded) = serde_json::from_slice::<WebhookDeliveryEvidenceLog>(&encoded) else {
        return;
    };
    assert!(verify_webhook_delivery_events(decoded.events(), &recovered).is_ok());
});
