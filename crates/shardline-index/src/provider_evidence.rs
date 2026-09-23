use shardline_reliability::{
    ProviderLifecycleObservations, ProviderLifecycleSnapshot, ProviderRepositoryIdentity,
};

use crate::ProviderRepositoryState;

pub(crate) fn snapshot_from_state(
    state: &ProviderRepositoryState,
) -> Result<ProviderLifecycleSnapshot, shardline_reliability::ReliabilityError> {
    ProviderLifecycleSnapshot::from_parts(
        ProviderRepositoryIdentity::new(state.provider().as_str(), state.owner(), state.repo()),
        ProviderLifecycleObservations::new(
            state.last_access_changed_at_unix_seconds(),
            state.last_revision_pushed_at_unix_seconds(),
            state.last_pushed_revision().map(ToOwned::to_owned),
            state.last_cache_invalidated_at_unix_seconds(),
            state.last_authorization_rechecked_at_unix_seconds(),
            state.last_drift_checked_at_unix_seconds(),
        ),
    )
}
