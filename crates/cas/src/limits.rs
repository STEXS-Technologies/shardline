use std::num::NonZeroU64;

/// Coordinator limits for untrusted protocol objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CasLimits {
    max_xorb_bytes: NonZeroU64,
    max_shard_bytes: NonZeroU64,
}

impl CasLimits {
    /// Creates coordinator limits.
    #[must_use]
    pub const fn new(max_xorb_bytes: NonZeroU64, max_shard_bytes: NonZeroU64) -> Self {
        Self {
            max_xorb_bytes,
            max_shard_bytes,
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
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::CasLimits;

    #[test]
    fn limits_preserve_configured_bounds() {
        let max_xorb_bytes = NonZeroU64::MIN;
        let max_shard_bytes = NonZeroU64::MAX;
        let limits = CasLimits::new(max_xorb_bytes, max_shard_bytes);

        assert_eq!(limits.max_xorb_bytes(), max_xorb_bytes);
        assert_eq!(limits.max_shard_bytes(), max_shard_bytes);
    }
}
