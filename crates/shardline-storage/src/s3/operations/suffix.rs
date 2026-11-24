//! Fetch a bounded footer trailer without an extra metadata request.
use super::*;
use object_store::GetRange;

fn is_strong_entity_tag(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2
        && bytes.first() == Some(&b'"')
        && bytes.last() == Some(&b'"')
        && bytes
            .get(1..bytes.len().saturating_sub(1))
            .is_some_and(|opaque| {
                opaque
                    .iter()
                    .all(|byte| matches!(*byte, 0x21 | 0x23..=0x7e | 0x80..=0xff))
            })
}

/// A generation-bound source for several authenticated range reads.
///
/// This pins provider identity and length, not content integrity. Callers must
/// still authenticate returned bytes against independently trusted commitments.
#[derive(Clone)]
pub struct S3RangeSnapshot {
    store: S3ObjectStore,
    location: ObjectStorePath,
    total: u64,
    etag: String,
    version: Option<String>,
}

impl S3RangeSnapshot {
    /// Length observed by the GET that established this snapshot.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.total
    }

    /// Reads an exact range from the captured generation.
    ///
    /// # Errors
    /// Rejects changed generations, missing objects, invalid provider ranges,
    /// incomplete bodies and ranges extending beyond the captured length.
    pub async fn read_range(&self, range: ByteRange) -> Result<Vec<u8>, S3ObjectStoreError> {
        if range.end_inclusive() >= self.total {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        }
        let expected = validated_external_range(range)?;
        let result = self
            .store
            .inner
            .get_opts(
                &self.location,
                GetOptions::new()
                    .with_range(Some(expected.clone()))
                    .with_if_match(Some(self.etag.clone()))
                    .with_version(self.version.clone()),
            )
            .await
            .map_err(S3ObjectStoreError::External)?;
        if result.meta.size != self.total
            || result.meta.e_tag.as_ref() != Some(&self.etag)
            || self
                .version
                .as_ref()
                .is_some_and(|version| result.meta.version.as_ref() != Some(version))
        {
            return Err(S3ObjectStoreError::SnapshotIdentityMismatch);
        }
        read_range_payload(result, expected).await
    }
}

impl S3ObjectStore {
    /// Reads a nonempty suffix and captures its provider generation for later reads.
    ///
    /// # Errors
    /// Rejects a zero-length suffix, invalid response bodies and missing or weak
    /// ETags. A provider without a stable identity needs a whole-object read.
    pub async fn read_suffix_snapshot(
        &self,
        key: &ObjectKey,
        length: u64,
    ) -> Result<Option<(Vec<u8>, S3RangeSnapshot)>, S3ObjectStoreError> {
        if length == 0 {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        }
        let Some(result) = self.suffix_result(key, length).await? else {
            return Ok(None);
        };
        let etag = result
            .meta
            .e_tag
            .as_ref()
            .filter(|etag| is_strong_entity_tag(etag))
            .ok_or(S3ObjectStoreError::SnapshotIdentityUnavailable)?
            .clone();
        let snapshot = S3RangeSnapshot {
            store: self.clone(),
            location: self.location_for_key(key)?,
            total: result.meta.size,
            etag,
            // S3 uses the literal "null" for an unversioned object. If-Match
            // still binds these reads; it must not become a versionId request.
            version: result
                .meta
                .version
                .clone()
                .filter(|version| !version.is_empty() && version != "null"),
        };
        let bytes = read_range_payload(
            result,
            snapshot.total.saturating_sub(length)..snapshot.total,
        )
        .await?;
        Ok(Some((bytes, snapshot)))
    }

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
        let Some(result) = self.suffix_result(key, length).await? else {
            return Ok(None);
        };
        let total = result.meta.size;
        let start = total.saturating_sub(length);
        let bytes = read_range_payload(result, start..total).await?;
        Ok(Some((bytes, total)))
    }

    async fn suffix_result(
        &self,
        key: &ObjectKey,
        length: u64,
    ) -> Result<Option<GetResult>, S3ObjectStoreError> {
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
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn strong_entity_tag_requires_a_quoted_non_wildcard_opaque_tag() {
        for valid in ["\"etag\"", "\"multipart-1-3\"", "\"\""] {
            assert!(super::is_strong_entity_tag(valid), "{valid}");
        }
        for invalid in [
            "",
            "etag",
            "*",
            "W/\"weak\"",
            "\"unterminated",
            "\"a\"b\"",
            "\"bad\nvalue\"",
        ] {
            assert!(!super::is_strong_entity_tag(invalid), "{invalid:?}");
        }
    }
}
