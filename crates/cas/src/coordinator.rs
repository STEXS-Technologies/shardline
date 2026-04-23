use crate::CasLimits;

/// CAS coordinator with pluggable index and object storage.
#[derive(Debug)]
pub struct CasCoordinator<I, O> {
    index: I,
    object_store: O,
    limits: CasLimits,
}

impl<I, O> CasCoordinator<I, O> {
    /// Creates a CAS coordinator.
    #[must_use]
    pub const fn new(index: I, object_store: O, limits: CasLimits) -> Self {
        Self {
            index,
            object_store,
            limits,
        }
    }

    /// Returns the metadata index adapter.
    #[must_use]
    pub const fn index(&self) -> &I {
        &self.index
    }

    /// Returns the object storage adapter.
    #[must_use]
    pub const fn object_store(&self) -> &O {
        &self.object_store
    }

    /// Returns the active coordinator limits.
    #[must_use]
    pub const fn limits(&self) -> CasLimits {
        self.limits
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::CasCoordinator;
    use crate::CasLimits;

    #[derive(Debug, PartialEq, Eq)]
    struct IndexProbe;

    #[derive(Debug, PartialEq, Eq)]
    struct ObjectStoreProbe;

    #[test]
    fn coordinator_keeps_adapters_and_limits() {
        let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN);
        let coordinator = CasCoordinator::new(IndexProbe, ObjectStoreProbe, limits);

        assert_eq!(coordinator.index(), &IndexProbe);
        assert_eq!(coordinator.object_store(), &ObjectStoreProbe);
        assert_eq!(coordinator.limits(), limits);
    }
}
