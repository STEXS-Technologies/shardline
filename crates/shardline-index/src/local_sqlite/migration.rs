#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalSqliteMigration {
    pub(crate) version: &'static str,
    pub(crate) name: &'static str,
    pub(crate) up_sql: &'static str,
    pub(crate) down_sql: &'static str,
}

pub(crate) const LOCAL_SQLITE_MIGRATIONS: [LocalSqliteMigration; 20] = [
    LocalSqliteMigration {
        version: "20260417000000",
        name: "metadata_store",
        up_sql: include_str!("../../migrations/20260417000000_metadata_store.up.sql"),
        down_sql: include_str!("../../migrations/20260417000000_metadata_store.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260417010000",
        name: "retention_holds",
        up_sql: include_str!("../../migrations/20260417010000_retention_holds.up.sql"),
        down_sql: include_str!("../../migrations/20260417010000_retention_holds.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418000000",
        name: "dedupe_shards",
        up_sql: include_str!("../../migrations/20260418000000_dedupe_shards.up.sql"),
        down_sql: include_str!("../../migrations/20260418000000_dedupe_shards.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418010000",
        name: "webhook_deliveries",
        up_sql: include_str!("../../migrations/20260418010000_webhook_deliveries.up.sql"),
        down_sql: include_str!("../../migrations/20260418010000_webhook_deliveries.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418020000",
        name: "provider_repository_states",
        up_sql: include_str!("../../migrations/20260418020000_provider_repository_states.up.sql"),
        down_sql: include_str!(
            "../../migrations/20260418020000_provider_repository_states.down.sql"
        ),
    },
    LocalSqliteMigration {
        version: "20260418110000",
        name: "provider_repository_reconciliation",
        up_sql: include_str!(
            "../../migrations/20260418110000_provider_repository_reconciliation.up.sql"
        ),
        down_sql: include_str!(
            "../../migrations/20260418110000_provider_repository_reconciliation.down.sql"
        ),
    },
    LocalSqliteMigration {
        version: "20260629000000",
        name: "hub_api",
        up_sql: include_str!("../../migrations/20260629000000_hub_api.up.sql"),
        down_sql: include_str!("../../migrations/20260629000000_hub_api.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260630000000",
        name: "hub_inline_content",
        up_sql: include_str!("../../migrations/20260630000000_hub_inline_content.up.sql"),
        down_sql: include_str!("../../migrations/20260630000000_hub_inline_content.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260630000001",
        name: "hub_webhooks",
        up_sql: include_str!("../../migrations/20260630000001_hub_webhooks.up.sql"),
        down_sql: include_str!("../../migrations/20260630000001_hub_webhooks.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260630000002",
        name: "hub_refs",
        up_sql: include_str!("../../migrations/20260630000002_hub_refs.up.sql"),
        down_sql: include_str!("../../migrations/20260630000002_hub_refs.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260630000003",
        name: "drop_inline_content",
        up_sql: include_str!("../../migrations/20260630000003_drop_inline_content.up.sql"),
        down_sql: include_str!("../../migrations/20260630000003_drop_inline_content.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260630000004",
        name: "drop_lfs_objects",
        up_sql: include_str!("../../migrations/20260630000004_drop_lfs_objects.up.sql"),
        down_sql: include_str!("../../migrations/20260630000004_drop_lfs_objects.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260720000000",
        name: "fix_indexes",
        up_sql: include_str!("../../migrations/20260720000000_fix_indexes.up.sql"),
        down_sql: include_str!("../../migrations/20260720000000_fix_indexes.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260721000000",
        name: "upload_intents",
        up_sql: include_str!("../../migrations/20260721000000_upload_intents.up.sql"),
        down_sql: include_str!("../../migrations/20260721000000_upload_intents.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260805000000",
        name: "tree_store",
        up_sql: include_str!("../../migrations/20260805000000_tree_store.up.sql"),
        down_sql: include_str!("../../migrations/20260805000000_tree_store.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260813000000",
        name: "s3_object_index",
        up_sql: include_str!("../../migrations/20260813000000_s3_object_index.up.sql"),
        down_sql: include_str!("../../migrations/20260813000000_s3_object_index.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260814000000",
        name: "s3_object_etag_metadata",
        up_sql: include_str!("../../migrations/20260814000000_s3_object_etag_metadata.up.sql"),
        down_sql: include_str!("../../migrations/20260814000000_s3_object_etag_metadata.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260822000000",
        name: "oci_tags",
        up_sql: include_str!("../../migrations/20260822000000_oci_tags.up.sql"),
        down_sql: include_str!("../../migrations/20260822000000_oci_tags.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260822010000",
        name: "resource_fences",
        up_sql: include_str!("../../migrations/20260822010000_resource_fences.up.sql"),
        down_sql: include_str!("../../migrations/20260822010000_resource_fences.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260822020000",
        name: "oci_object_tombstones",
        up_sql: include_str!("../../migrations/20260822020000_oci_object_tombstones.up.sql"),
        down_sql: include_str!("../../migrations/20260822020000_oci_object_tombstones.down.sql"),
    },
];
