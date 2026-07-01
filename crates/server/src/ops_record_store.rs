pub(crate) use shardline_server_core::OpsRecordStore;

#[cfg(test)]
mod tests {
    use shardline_index::{FileRecord, RecordTraversal};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use shardline_server_core::OpsRecordKind;

    use super::OpsRecordStore;
    use crate::record_store::LocalRecordStore;

    #[test]
    fn local_locator_helpers_extract_scoped_file_id_and_hash() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "octo", "assets", Some("main"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let store = LocalRecordStore::open("/var/lib/shardline".into());
        let record = FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: Some(scope),
            chunks: Vec::new(),
        };

        let latest = store.latest_record_locator(&record);
        let version = store.version_record_locator(&record);

        assert_eq!(
            store.locator_file_id(&latest, OpsRecordKind::Latest),
            Some("asset.bin".to_owned())
        );
        assert_eq!(
            store.locator_file_id(&version, OpsRecordKind::Version),
            Some("asset.bin".to_owned())
        );
        assert_eq!(
            store.locator_content_hash(&version, OpsRecordKind::Version),
            Some("a".repeat(64))
        );
    }
}
