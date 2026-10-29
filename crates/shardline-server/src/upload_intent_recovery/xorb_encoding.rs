//! Native content identities can retain a different verified wire encoding.

use super::*;

pub(crate) async fn native_xorb_has_equivalent_encoding(
    intent: &UploadIntent,
    objects: ServerObjectStore,
    pool: &BoundedPool,
) -> Result<bool, ServerError> {
    if !intent.intent_id().starts_with("xorb-v2-") {
        return Ok(false);
    }
    let key = shardline_xet_adapter::xorb_object_key(intent.object_hash())?;
    if key.as_str() != intent.object_key() {
        return Ok(false);
    }
    let hash = shardline_index::parse_xet_hash_hex(intent.object_hash())?;
    crate::admission::blocking_work(pool, move || {
        let Some((mut source, _)) = objects.materialize_bounded_object_if_present(&key, 0)? else {
            return Ok(false);
        };
        // Footer-only or length-only checks cannot authenticate an alternate
        // representation. Verify the complete retained container and members.
        shardline_xet_adapter::validate_serialized_xorb(&mut source, hash)
            .map_err(shardline_xet_adapter::XetAdapterError::from)?;
        Ok(true)
    })
    .await
}
