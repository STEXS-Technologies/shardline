//! Keep bounded buffer admission attached to transport-owned bytes and slices.
struct AdmittedBytes<P> {
    bytes: Vec<u8>,
    _admission: P,
}

impl<P> AsRef<[u8]> for AdmittedBytes<P> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(crate) fn retain_bytes(bytes: Vec<u8>, admission: impl Send + 'static) -> axum::body::Bytes {
    axum::body::Bytes::from_owner(AdmittedBytes {
        bytes,
        _admission: admission,
    })
}

#[cfg(test)]
mod tests {
    use crate::admission::BoundedPool;
    use std::num::NonZeroUsize;

    #[tokio::test]
    async fn response_slices_retain_buffer_charge_without_occupying_execution() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let bytes = super::retain_bytes(vec![42; 64], pool.acquire_buffer().await.unwrap());
        assert_eq!(pool.available_buffer_permits(), 0);
        let execution = pool.acquire().await.unwrap();
        let retained = bytes.slice(0..1);
        drop(bytes);
        drop(execution);
        assert_eq!(pool.available_buffer_permits(), 0);
        assert_eq!(pool.available_permits(), 1);
        assert_eq!(retained.as_ref(), &[42]);
        drop(retained);
        assert_eq!(pool.available_buffer_permits(), 1);
    }
}
