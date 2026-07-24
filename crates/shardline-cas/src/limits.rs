use std::num::NonZeroU64;

/// Coordinator limits for untrusted protocol objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CasLimits {
    max_xorb_bytes: NonZeroU64,
    max_shard_bytes: NonZeroU64,
    max_object_bytes: NonZeroU64,
}

impl CasLimits {
    /// Creates coordinator limits.
    #[must_use]
    pub const fn new(
        max_xorb_bytes: NonZeroU64,
        max_shard_bytes: NonZeroU64,
        max_object_bytes: NonZeroU64,
    ) -> Self {
        Self {
            max_xorb_bytes,
            max_shard_bytes,
            max_object_bytes,
        }
    }

    /// Returns the maximum accepted serialized xorb size.
    #[must_use]
    pub const fn max_xorb_bytes(&self) -> NonZeroU64 {
        self.max_xorb_bytes
    }

    /// Returns the maximum accepted serialized shard size.
    #[must_use]
    pub const fn max_shard_bytes(&self) -> NonZeroU64 {
        self.max_shard_bytes
    }

    /// Returns the maximum accepted body size for content-addressed blobs.
    #[must_use]
    pub const fn max_object_bytes(&self) -> NonZeroU64 {
        self.max_object_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::CasLimits;

    #[test]
    fn limits_preserve_configured_bounds() {
        let max_xorb_bytes = NonZeroU64::MIN;
        let max_shard_bytes = NonZeroU64::MAX;
        let max_object_bytes = NonZeroU64::new(3).unwrap();
        let limits = CasLimits::new(max_xorb_bytes, max_shard_bytes, max_object_bytes);

        assert_eq!(limits.max_xorb_bytes(), max_xorb_bytes);
        assert_eq!(limits.max_shard_bytes(), max_shard_bytes);
        assert_eq!(limits.max_object_bytes(), max_object_bytes);
    }

    #[test]
    fn limits_debug_format() {
        let limits = CasLimits::new(NonZeroU64::new(1).unwrap(), NonZeroU64::new(2).unwrap(), NonZeroU64::new(3).unwrap());
        let debug = format!("{limits:?}");
        assert!(debug.contains("CasLimits"));
        assert!(debug.contains("max_xorb_bytes"));
        assert!(debug.contains("max_shard_bytes"));
        assert!(debug.contains("max_object_bytes"));
    }

    #[test]
    fn limits_clone_produces_equal_copy() {
        let limits = CasLimits::new(NonZeroU64::new(100).unwrap(), NonZeroU64::new(200).unwrap(), NonZeroU64::new(300).unwrap());
        let cloned = limits;
        assert_eq!(limits, cloned);
        assert_eq!(limits.max_xorb_bytes(), cloned.max_xorb_bytes());
        assert_eq!(limits.max_shard_bytes(), cloned.max_shard_bytes());
        assert_eq!(limits.max_object_bytes(), cloned.max_object_bytes());
    }

    #[test]
    fn limits_copy_semantics() {
        let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN, NonZeroU64::MIN);
        // Copy should not move
        let _copy1 = limits;
        let _copy2 = limits;
        // Both copies still work
        assert_eq!(_copy1.max_xorb_bytes(), _copy2.max_xorb_bytes());
    }

    #[test]
    fn limits_different_bounds_are_not_equal() {
        let a = CasLimits::new(NonZeroU64::new(1).unwrap(), NonZeroU64::new(2).unwrap(), NonZeroU64::new(3).unwrap());
        let b = CasLimits::new(NonZeroU64::new(4).unwrap(), NonZeroU64::new(5).unwrap(), NonZeroU64::new(6).unwrap());
        assert_ne!(a, b);
    }

    #[test]
    fn limits_same_bounds_are_equal() {
        let a = CasLimits::new(NonZeroU64::new(42).unwrap(), NonZeroU64::new(99).unwrap(), NonZeroU64::new(101).unwrap());
        let b = CasLimits::new(NonZeroU64::new(42).unwrap(), NonZeroU64::new(99).unwrap(), NonZeroU64::new(101).unwrap());
        assert_eq!(a, b);
    }
}
