use std::sync::{Mutex, Once, OnceLock};

use shardline_hub_api::routes::HubState;
use shardline_index::LocalIndexStore;
use shardline_index::hub::BoxedHubStore;
use tempfile::TempDir;

pub(crate) static INIT: Once = Once::new();
pub(crate) static TEMP_DIR: OnceLock<Mutex<Option<TempDir>>> = OnceLock::new();
pub(crate) static STATE: OnceLock<HubState> = OnceLock::new();

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
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_ref_idx
                ON shardline_hub_revisions (repo_id, ref_name);
            CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
                commit_sha TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                sha TEXT NOT NULL,
                is_lfs INTEGER NOT NULL DEFAULT 0 CHECK (is_lfs IN (0, 1)),
                inline_content BLOB,
                PRIMARY KEY (commit_sha, path)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
                oid TEXT PRIMARY KEY,
                data BLOB NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                url TEXT NOT NULL,
                events TEXT NOT NULL DEFAULT 'push',
                secret TEXT,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS shardline_hub_webhooks_repo_idx ON shardline_hub_webhooks (repo_id);";

pub(crate) fn setup() {
    INIT.call_once(|| {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().to_path_buf();
        let db_path = root.join("metadata.sqlite3");
        let conn = rusqlite::Connection::open(&db_path).expect("open sqlite");
        conn.execute_batch(HUB_SCHEMA).expect("execute schema");
        drop(conn);

        let store = LocalIndexStore::open(root);
        let boxed = BoxedHubStore::from_store(store);
        let state = HubState {
            store: boxed,
            auth: None,
            http_client: None,
        };
        let _ = STATE.set(state);

        let dir_lock = TEMP_DIR.get_or_init(|| Mutex::new(None));
        *dir_lock.lock().unwrap() = Some(tmp);
    });
}

pub(crate) fn state() -> &'static HubState {
    STATE.get().expect("setup() must be called first")
}

pub(crate) fn app() -> axum::Router {
    shardline_hub_api::hub_routes(state().clone())
}
