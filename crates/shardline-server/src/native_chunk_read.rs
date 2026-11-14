//! Direct chunk reads with bounded, authenticated native-member fallback.

use crate::{
    ServerError,
    admission::{BoundedPool, blocking_work},
    object_store::ServerObjectStore,
};

#[cfg(all(test, feature = "docker"))]
mod remote_gc_tests;
#[cfg(all(test, feature = "docker"))]
mod remote_tests;
use shardline_index::XetXorbDescriptor;
use std::future::Future;

pub(crate) async fn read_chunk<F, Page>(
    objects: ServerObjectStore,
    hash: &str,
    pool: &BoundedPool,
    mut page: F,
) -> Result<Vec<u8>, ServerError>
where
    F: FnMut(Option<String>) -> Page,
    Page: Future<Output = Result<Vec<XetXorbDescriptor>, ServerError>>,
{
    let object_key = crate::chunk_store::chunk_object_key(hash)?;
    match crate::object_store::read_small_object(objects.clone(), object_key).await {
        Ok(bytes) => return Ok(bytes),
        Err(ServerError::NotFound) => {}
        Err(error) => return Err(error),
    }
    let mut after = None;
    loop {
        let candidates = page(after.clone()).await?;
        if candidates.is_empty() {
            return Err(ServerError::NotFound);
        }
        let next = candidates.last().map(|descriptor| descriptor.hash.clone());
        if next <= after {
            return Err(ServerError::NotFound);
        }
        for candidate in candidates {
            let source = objects.clone();
            let hash = hash.to_owned();
            // The database is a discovery hint. Authenticate the actual source
            // and selected member; do not use cached offsets as authority.
            let member = blocking_work(pool, move || {
                Ok(shardline_xet_adapter::read_verified_xorb_chunk(
                    &source,
                    &candidate.hash,
                    &hash,
                )?)
            })
            .await?;
            if let Some(bytes) = member {
                return Ok(bytes);
            }
        }
        after = next;
    }
}
