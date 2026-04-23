use shardline_protocol::ByteRange;

use crate::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix, PutOutcome,
};

/// Object storage adapter contract.
pub trait ObjectStore {
    /// Adapter-specific error type.
    type Error;

    /// Stores an object if no identical object exists yet.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when storage fails or when an existing object conflicts
    /// with the supplied integrity metadata.
    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error>;

    /// Reads an inclusive byte range from an object.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the object is missing, the range cannot be served,
    /// or storage fails.
    fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error>;

    /// Returns whether an object exists.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when storage cannot answer the existence check.
    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error>;

    /// Returns stored metadata for an object.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when storage cannot answer the metadata lookup.
    fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error>;

    /// Lists objects under a validated key prefix.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when inventory lookup fails.
    fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error>;

    /// Visits objects under a validated key prefix without requiring callers to own the
    /// full inventory at once.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when inventory lookup fails or when the visitor
    /// rejects an object.
    fn visit_prefix<Visitor, VisitorError>(
        &self,
        prefix: &ObjectPrefix,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ObjectMetadata) -> Result<(), VisitorError>,
    {
        for metadata in self.list_prefix(prefix).map_err(Into::into)? {
            visitor(metadata)?;
        }

        Ok(())
    }

    /// Deletes an object if it exists.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when deletion fails.
    fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error>;
}
