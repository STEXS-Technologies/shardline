use super::*;

#[test]
fn synchronous_bridge_drives_timers_and_preserves_values_and_errors_in_each_runtime_context() {
    let store =
        S3ObjectStore::new(S3ObjectStoreConfig::new("bucket".into(), "region".into())).unwrap();
    let exercise = || {
        let value: Result<u64, S3ObjectStoreError> = store.block_on_result(async {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            Ok(42)
        });
        assert_eq!(value.unwrap(), 42);
        let external = store.block_on(async {
            Err::<(), _>(ExternalObjectStoreError::NotImplemented {
                operation: "fixture".into(),
                implementer: "fixture".into(),
            })
        });
        assert!(matches!(
            external,
            Err(S3ObjectStoreError::External(
                ExternalObjectStoreError::NotImplemented { .. }
            ))
        ));
        let domain_error: Result<(), S3ObjectReadError> =
            store.block_on_result(async { Err(S3ObjectReadError::ObjectTooLarge) });
        assert!(matches!(
            domain_error,
            Err(S3ObjectReadError::ObjectTooLarge)
        ));
    };
    exercise();
    for multithreaded in [false, true] {
        let mut builder = if multithreaded {
            Builder::new_multi_thread()
        } else {
            Builder::new_current_thread()
        };
        let runtime = builder.worker_threads(2).enable_all().build().unwrap();
        runtime.block_on(async {
            exercise();
        });
    }
}
