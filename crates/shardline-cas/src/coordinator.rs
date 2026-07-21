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
        let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX);
        let coordinator = CasCoordinator::new(IndexProbe, ObjectStoreProbe, limits);

        assert_eq!(coordinator.index(), &IndexProbe);
        assert_eq!(coordinator.object_store(), &ObjectStoreProbe);
        assert_eq!(coordinator.limits(), limits);
    }

    #[test]
    fn coordinator_debug_format() {
        let limits = CasLimits::new(NonZeroU64::new(1).unwrap(), NonZeroU64::new(2).unwrap());
        let coordinator = CasCoordinator::new(IndexProbe, ObjectStoreProbe, limits);
        let debug = format!("{coordinator:?}");
        assert!(debug.contains("CasCoordinator"));
        assert!(debug.contains("IndexProbe"));
        assert!(debug.contains("ObjectStoreProbe"));
        assert!(debug.contains("CasLimits"));
    }

    #[test]
    fn coordinator_with_different_type_combinations() {
        let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN);
        let coordinator = CasCoordinator::new(42_usize, String::from("store"), limits);
        assert_eq!(coordinator.index(), &42_usize);
        assert_eq!(coordinator.object_store(), &"store");
        assert_eq!(coordinator.limits(), limits);
    }

    #[test]
    fn coordinator_limits_are_independent_of_adapters() {
        let limits_a = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN);
        let limits_b = CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX);
        let coord_a = CasCoordinator::new((), (), limits_a);
        let coord_b = CasCoordinator::new((), (), limits_b);
        assert_ne!(coord_a.limits(), coord_b.limits());
    }
}
