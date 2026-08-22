use super::types::LifecycleRepairBoundary;
use crate::ServerError;

#[cfg(not(test))]
#[allow(clippy::unnecessary_wraps)]
pub(super) const fn lifecycle_repair_failpoint(
    _boundary: LifecycleRepairBoundary,
) -> Result<(), ServerError> {
    Ok(())
}

#[cfg(test)]
pub(super) fn lifecycle_repair_failpoint(
    boundary: LifecycleRepairBoundary,
) -> Result<(), ServerError> {
    enabled::hit(boundary)
}

#[cfg(test)]
pub(crate) use enabled::interrupt_at;

#[cfg(test)]
mod enabled {
    use std::{cell::Cell, future::Future};

    use super::{LifecycleRepairBoundary, ServerError};

    tokio::task_local! {
        static ARMED_BOUNDARY: Cell<Option<LifecycleRepairBoundary>>;
    }

    pub(crate) async fn interrupt_at<Output>(
        boundary: LifecycleRepairBoundary,
        future: impl Future<Output = Output>,
    ) -> Output {
        ARMED_BOUNDARY
            .scope(Cell::new(Some(boundary)), future)
            .await
    }

    pub(super) fn hit(boundary: LifecycleRepairBoundary) -> Result<(), ServerError> {
        let interrupted = ARMED_BOUNDARY
            .try_with(|armed| armed.get() == Some(boundary))
            .unwrap_or(false);
        if interrupted {
            Err(ServerError::InjectedLifecycleRepairInterruption { boundary })
        } else {
            Ok(())
        }
    }
}
