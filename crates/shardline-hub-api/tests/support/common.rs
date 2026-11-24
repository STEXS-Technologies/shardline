use shardline_hub_api::routes::HubState;
use shardline_index::LocalIndexStore;
use shardline_index::hub::BoxedHubStore;
use shardline_server_core::ServerObjectStore;
use tempfile::TempDir;

pub(crate) struct HubTestContext {
    _temp_dir: TempDir,
    state: HubState,
}

pub(crate) fn setup() -> HubTestContext {
    let temp_dir = TempDir::new().expect("tempdir");
    let root = temp_dir.path().to_path_buf();
    let store = LocalIndexStore::new(root.clone()).expect("initialize SQLite store");
    let boxed = BoxedHubStore::from_store(store);
    let object_store = ServerObjectStore::local(root.join("lfs")).expect("local object store");
    HubTestContext::with_backend(temp_dir, boxed, object_store)
}

impl HubTestContext {
    pub(crate) fn with_backend(
        temp_dir: TempDir,
        store: BoxedHubStore,
        object_store: ServerObjectStore,
    ) -> Self {
        let state = HubState {
            store,
            object_store,
            recovery_archive: None,
            auth: None,
            http_client: None,
            webhook_secret_cipher: None,
            public_base_url: "http://127.0.0.1:8080".to_owned(),
        };

        Self {
            _temp_dir: temp_dir,
            state,
        }
    }

    pub(crate) const fn state(&self) -> &HubState {
        &self.state
    }

    pub(crate) fn app(&self) -> axum::Router {
        shardline_hub_api::hub_routes(self.state.clone(), true)
    }
}
