use std::num::NonZeroU64;

/// Coordinator limits for untrusted protocol objects.
///
/// Enforces upper bounds on the serialized size of xorb containers, shards,
/// and content-addressed blobs before they are parsed or stored. Every
/// non-zero bound is enforced at the CAS boundary.
///
/// # Examples
///
/// ```
/// use shardline_cas::CasLimits;
/// use std::num::NonZeroU64;
///
/// let limits = CasLimits::new(
///     NonZeroU64::new(64 * 1024 * 1024).expect("64 MiB is non-zero"),
///     NonZeroU64::new(16 * 1024 * 1024).expect("16 MiB is non-zero"),
///     NonZeroU64::new(8 * 1024 * 1024).expect("8 MiB is non-zero"),
/// );
///
/// assert_eq!(limits.max_xorb_bytes().get(), 64 * 1024 * 1024);
/// assert_eq!(limits.max_shard_bytes().get(), 16 * 1024 * 1024);
/// assert_eq!(limits.max_object_bytes().get(), 8 * 1024 * 1024);
/// ```
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
        for (xorb, shard, object) in [
            (1, u64::MAX, 3),
            (4096, 8192, 16384),
            (1, 1, 1),
            (u64::MAX, u64::MAX, u64::MAX),
        ] {
            let limits = CasLimits::new(
                NonZeroU64::new(xorb).unwrap(),
                NonZeroU64::new(shard).unwrap(),
                NonZeroU64::new(object).unwrap(),
            );
            assert_eq!(limits.max_xorb_bytes().get(), xorb);
            assert_eq!(limits.max_shard_bytes().get(), shard);
            assert_eq!(limits.max_object_bytes().get(), object);
        }
    }
}
