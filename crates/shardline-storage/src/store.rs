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

#[cfg(test)]
mod tests {
    use crate::{
        DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix,
        ObjectStore, PutOutcome,
    };

    /// A minimal store that ONLY overrides `list_prefix` so the
    /// default `visit_prefix` implementation from the `ObjectStore`
    /// trait is exercised.
    struct MinimalStore;

    impl ObjectStore for MinimalStore {
        type Error = Box<dyn std::error::Error>;

        fn put_if_absent(
            &self,
            _key: &ObjectKey,
            _body: ObjectBody<'_>,
            _integrity: &ObjectIntegrity,
        ) -> Result<PutOutcome, Self::Error> {
            Err("not implemented".into())
        }

        fn read_range(
            &self,
            _key: &ObjectKey,
            _range: shardline_protocol::ByteRange,
        ) -> Result<Vec<u8>, Self::Error> {
            Err("not implemented".into())
        }

        fn contains(&self, _key: &ObjectKey) -> Result<bool, Self::Error> {
            Err("not implemented".into())
        }

        fn metadata(&self, _key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
            Err("not implemented".into())
        }

        fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
            // Return a single entry to test that visit_prefix visits it.
            let key = ObjectKey::parse(&format!("{}file.xorb", prefix.as_str()))
                .map_err(|e| -> Box<dyn std::error::Error> { format!("bad key: {e}").into() })?;
            Ok(vec![ObjectMetadata::new(key, 42, None)])
        }

        fn delete_if_present(&self, _key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
            Err("not implemented".into())
        }
    }

    #[test]
    fn default_visit_prefix_delegates_to_list_prefix() {
        let store = MinimalStore;
        let prefix = ObjectPrefix::parse("ns/").expect("valid prefix");

        let mut visited = Vec::new();
        let result: Result<(), Box<dyn std::error::Error>> = store.visit_prefix(&prefix, |meta| {
            visited.push(meta.key().clone());
            Ok(())
        });

        assert!(result.is_ok());
        assert_eq!(visited.len(), 1);
        assert_eq!(visited[0].as_str(), "ns/file.xorb");
    }

    #[test]
    fn default_visit_prefix_propagates_visitor_error() {
        let store = MinimalStore;
        let prefix = ObjectPrefix::parse("ns/").expect("valid prefix");

        let result: Result<(), Box<dyn std::error::Error>> =
            store.visit_prefix(&prefix, |_meta| Err("visitor rejected".into()));

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "visitor rejected");
    }
}
