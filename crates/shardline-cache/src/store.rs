use std::{future::Future, pin::Pin, sync::Arc};

use crate::{ReconstructionCacheError, ReconstructionCacheKey};

/// Boxed asynchronous reconstruction-cache operation.
pub type ReconstructionCacheFuture<'operation, T> =
    Pin<Box<dyn Future<Output = Result<T, ReconstructionCacheError>> + Send + 'operation>>;

/// The result of an atomic cache lookup and cold-load reservation attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconstructionCacheLookup {
    /// A complete cached reconstruction payload.
    Hit(Vec<u8>),
    /// This caller owns the right to reconstruct and publish the missing value.
    Reserved(ReconstructionCacheReservation),
}

/// An opaque, operation-scoped cold-load reservation.
///
/// Distributed adapters fence mutations with the owner token. Callers must
/// pass the reservation back when publishing, refreshing, or abandoning a
/// load; possession by cache key alone never establishes ownership.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconstructionCacheReservation {
    owner_token: Option<Arc<str>>,
}

impl ReconstructionCacheReservation {
    pub(crate) fn distributed(owner_token: String) -> Self {
        Self {
            owner_token: Some(Arc::from(owner_token)),
        }
    }

    pub(crate) fn owner_token(&self) -> Option<&str> {
        self.owner_token.as_deref()
    }
}

/// Asynchronous reconstruction-cache adapter contract.
pub trait AsyncReconstructionCache: Send + Sync {
    /// Verifies that the adapter is configured correctly and can serve requests.
    fn ready(&self) -> ReconstructionCacheFuture<'_, ()>;

    /// Loads one cached reconstruction payload.
    fn get<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>>;

    /// Atomically loads a value or reserves its cold reconstruction for this caller.
    fn get_or_reserve<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, ReconstructionCacheLookup> {
        Box::pin(async move {
            Ok(self.get(key).await?.map_or_else(
                || ReconstructionCacheLookup::Reserved(ReconstructionCacheReservation::default()),
                ReconstructionCacheLookup::Hit,
            ))
        })
    }

    /// Stores one reconstruction payload.
    fn put<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        payload: &'operation [u8],
    ) -> ReconstructionCacheFuture<'operation, ()>;

    /// Publishes a payload while proving ownership of its cold-load reservation.
    fn put_reserved<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        payload: &'operation [u8],
        _reservation: &'operation ReconstructionCacheReservation,
    ) -> ReconstructionCacheFuture<'operation, ()> {
        self.put(key, payload)
    }

    /// Deletes one cached reconstruction payload.
    fn delete<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool>;

    /// Deletes a value and abandons an owned cold-load reservation.
    fn delete_reserved<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        _reservation: &'operation ReconstructionCacheReservation,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        self.delete(key)
    }

    /// Refreshes the aliveness stamp of any in-flight loading latch for `key`.
    ///
    /// The service layer runs the reconstruction loader between `get()` and
    /// `put()`. A concurrent waiter at the orphan bound must be able to tell a
    /// slow-but-alive loader (fresh stamp) from a dead one (stale stamp), so
    /// the loader's caller refreshes the stamp while the load is in flight
    /// (F-90). Adapters without local loading latches (Redis, Disabled) have
    /// nothing to refresh and return `Ok(false)`.
    ///
    /// Returns `true` when a loading latch existed and was refreshed.
    fn touch_loading<'operation>(
        &'operation self,
        _key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async { Ok(false) })
    }

    /// Refreshes one explicitly owned cold-load reservation.
    fn touch_reservation<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        _reservation: &'operation ReconstructionCacheReservation,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        self.touch_loading(key)
    }

    /// Releases any in-flight loading latch for `key`, if the adapter keeps one.
    ///
    /// The service layer registers a loading latch via [`Self::get`] on a cold
    /// miss and releases it via [`Self::put`] or [`Self::delete`]. If the
    /// caller's future is dropped mid-load (client disconnect, request timeout,
    /// shutdown), neither runs — this synchronous release lets the caller's
    /// `Drop` guard clean up the latch so the next caller does not stall for
    /// the full orphan bound and the loading map does not grow without bound
    /// (F-112). Adapters without local loading latches (Redis, Disabled) no-op.
    fn release_loading(&self, _key: &ReconstructionCacheKey) {}

    /// Releases one explicitly owned reservation during cancellation.
    fn release_reservation(
        &self,
        key: &ReconstructionCacheKey,
        _reservation: &ReconstructionCacheReservation,
    ) {
        self.release_loading(key);
    }
}
