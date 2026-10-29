//! One response-local source: adjacent terms reuse bytes, never validation.

use std::io::Seek;

use shardline_server_core::MaterializedObject;

use super::{ServerError, ServerObjectStore, xorb_object_key};

#[derive(Default)]
pub(super) struct Source(Option<(String, MaterializedObject)>);

impl Source {
    pub(super) fn read(
        &mut self,
        objects: &ServerObjectStore,
        hash: &str,
        checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    ) -> Result<&mut MaterializedObject, ServerError> {
        checkpoint()?;
        if !self.0.as_ref().is_some_and(|(current, _)| current == hash) {
            // Release the old spool before fetching another container. Scratch
            // usage remains bounded to one source per admitted response.
            self.0 = None;
            let key = xorb_object_key(hash)?;
            if let Some(path) = objects.local_path_for_key(&key) {
                crate::object_store::run_before_local_object_read_hook(&path);
            }
            let (materialized, _) = objects
                .materialize_bounded_object_if_present_with_checkpoint(&key, 0, checkpoint)?
                .ok_or(ServerError::MissingReferencedXorb)?;
            self.0 = Some((hash.to_owned(), materialized));
        }
        let (_, materialized) = self.0.as_mut().ok_or(ServerError::MissingReferencedXorb)?;
        materialized.rewind()?;
        Ok(materialized)
    }
}
