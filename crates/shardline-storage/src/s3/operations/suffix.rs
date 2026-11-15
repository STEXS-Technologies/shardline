//! Fetch a bounded footer trailer without an extra metadata request.
use super::*;
use object_store::GetRange;

impl S3ObjectStore {
    pub(super) async fn read_suffix_bytes(
        &self,
        key: &ObjectKey,
        length: u64,
    ) -> Result<Option<(Vec<u8>, u64)>, S3ObjectStoreError> {
        if length == 0 {
            return Ok(AsyncObjectStore::metadata(self, key)
                .await?
                .map(|metadata| (Vec::new(), metadata.length())));
        }
        let location = self.location_for_key(key)?;
        let result = match self
            .inner
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(GetRange::Suffix(length))),
            )
            .await
        {
            Ok(result) => result,
            Err(ExternalObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(S3ObjectStoreError::External(error)),
        };
        let total = result.meta.size;
        let start = total.saturating_sub(length);
        let bytes = read_range_payload(result, start..total).await?;
        Ok(Some((bytes, total)))
    }
}
