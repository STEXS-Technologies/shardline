use crate::routes::HubState;

static STATE: std::sync::OnceLock<HubState> = std::sync::OnceLock::new();

/// Initializes the global Hub API state. Must be called before routing requests.
pub fn init(state: HubState) {
    let _ = STATE.set(state);
}

/// Returns a reference to the global Hub API state.
///
/// # Panics
///
/// Panics if [`init`] has not been called.
pub(crate) fn get() -> &'static HubState {
    STATE
        .get()
        .expect("shardline_hub_api::init() must be called before serving hub routes")
}
