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
        if result.range != (start..total) {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        }
        let expected = total
            .checked_sub(start)
            .ok_or(S3ObjectStoreError::RangeOutOfBounds)?;
        let mut bytes = Vec::new();
        let mut stream = result.into_stream();
        while let Some(frame) = stream.next().await {
            let frame = frame.map_err(S3ObjectStoreError::External)?;
            let next = bytes
                .len()
                .checked_add(frame.len())
                .and_then(|size| u64::try_from(size).ok())
                .ok_or(S3ObjectStoreError::RangeOutOfBounds)?;
            if next > expected {
                return Err(S3ObjectStoreError::RangeOutOfBounds);
            }
            bytes.extend_from_slice(&frame);
        }
        if u64::try_from(bytes.len()).map_err(|_error| S3ObjectStoreError::RangeOutOfBounds)?
            != expected
        {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        }
        Ok(Some((bytes, total)))
    }
}
