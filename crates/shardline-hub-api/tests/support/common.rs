use shardline_hub_api::routes::HubState;
use shardline_index::LocalIndexStore;
use shardline_index::hub::BoxedHubStore;
use shardline_server_core::ServerObjectStore;
use tempfile::TempDir;

pub(crate) struct HubTestContext {
    _temp_dir: TempDir,
    state: HubState,
}

pub(crate) const HUB_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
                repo_id TEXT PRIMARY KEY,
                repo_type TEXT NOT NULL CHECK (repo_type IN ('model', 'dataset', 'space')),
                private INTEGER NOT NULL DEFAULT 0 CHECK (private IN (0, 1)),
                default_branch TEXT NOT NULL,
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
                repo_id TEXT NOT NULL,
                ref_name TEXT NOT NULL,
                sha TEXT NOT NULL,
                parent_sha TEXT,
                message TEXT,
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                PRIMARY KEY (repo_id, sha),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos (repo_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_ref_idx
                ON shardline_hub_revisions (repo_id, ref_name);
            CREATE TABLE IF NOT EXISTS shardline_hub_refs (
                repo_id TEXT NOT NULL,
                ref_name TEXT NOT NULL,
                sha TEXT NOT NULL,
                PRIMARY KEY (repo_id, ref_name),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos (repo_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
                commit_sha TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                sha TEXT NOT NULL,
                is_lfs INTEGER NOT NULL DEFAULT 0 CHECK (is_lfs IN (0, 1)),

                PRIMARY KEY (commit_sha, path)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                url TEXT NOT NULL,
                events TEXT NOT NULL DEFAULT 'push',
                secret TEXT,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos (repo_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS shardline_hub_webhooks_repo_idx ON shardline_hub_webhooks (repo_id);";

pub(crate) fn setup() -> HubTestContext {
    let temp_dir = TempDir::new().expect("tempdir");
    let root = temp_dir.path().to_path_buf();
    let db_path = root.join("metadata.sqlite3");
    let conn = rusqlite::Connection::open(&db_path).expect("open sqlite");
    conn.execute_batch(HUB_SCHEMA).expect("execute schema");
    drop(conn);

    let store = LocalIndexStore::open(root.clone());
    let boxed = BoxedHubStore::from_store(store);
    let object_store = ServerObjectStore::local(root.join("lfs")).expect("local object store");
    let state = HubState {
        store: boxed,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
        public_base_url: "http://127.0.0.1:8080".to_owned(),
    };

    HubTestContext {
        _temp_dir: temp_dir,
        state,
    }
}

impl HubTestContext {
    pub(crate) fn state(&self) -> &HubState {
        &self.state
    }

    pub(crate) fn app(&self) -> axum::Router {
        shardline_hub_api::hub_routes(self.state.clone(), true)
    }
}
