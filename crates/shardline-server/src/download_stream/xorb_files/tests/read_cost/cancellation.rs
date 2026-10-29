//! Dropping a pending backend GET must release the range reader's admission.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_footer_and_member_gets_release_admission_without_waiting_for_provider() {
    let provider = Arc::new(Mutex::new(Provider::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(request).with_state(provider.clone());
    let _server = StopServer(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    let store = ServerObjectStore::s3(
        S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some(endpoint))
            .with_allow_http(true)
            .with_credentials(
                Some(SecretString::new("fixture-access".to_owned())),
                Some(SecretString::new("fixture-secret".to_owned())),
                None,
            ),
    )
    .unwrap();
    let storage = shardline_test_support::TempStorage::new();
    let local = ServerObjectStore::local(storage.path()).unwrap();
    let payload = vec![73; 1536 * 1024];
    let chunks =
        super::super::container(&local, &payload.chunks(512 * 1024).collect::<Vec<_>>()).await;
    let key = crate::xet_adapter::xorb_object_key(&chunks[0].hash).unwrap();
    let length = local.metadata(&key).unwrap().unwrap().length();
    let bytes = local
        .read_range(&key, ByteRange::new(0, length - 1).unwrap())
        .unwrap();
    provider
        .lock()
        .unwrap()
        .objects
        .insert(format!("/fixture/{}", key.as_str()), Bytes::from(bytes));
    let record = super::super::file(chunks);
    let range = Some(ByteRange::new(0, 4095).unwrap());
    let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::new(1).unwrap());
    for phase in [1, 2, 3] {
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        {
            let mut state = provider.lock().unwrap();
            state.requests.clear();
            state.change = Some((phase, Change::Stall(entered.clone(), resume.clone())));
        }
        let task = tokio::spawn(crate::download_stream::file_record_byte_stream(
            store.clone(),
            record.clone(),
            range,
            pool.clone(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("reader must reach the selected backend phase");
        assert_eq!(
            pool.available_buffer_permits(),
            0,
            "pending GET retains its buffer charge"
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::admission::blocking_work(&pool, || Ok(())),
        )
        .await
        .expect("a stalled provider GET must not monopolize execution")
        .unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert!(
            pool.try_acquire().is_some(),
            "cancel must release admission before provider resumes"
        );
        assert_eq!(pool.available_buffer_permits(), 1);
        resume.notify_one();
        let mut output = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::download_stream::file_record_byte_stream(
                store.clone(),
                record.clone(),
                range,
                pool.clone(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let mut actual = Vec::new();
        while let Some(bytes) = output.try_next().await.unwrap() {
            actual.extend_from_slice(&bytes);
        }
        assert_eq!(actual, payload[..4096]);
        drop(output);
        assert!(pool.try_acquire().is_some());
        assert_eq!(pool.available_buffer_permits(), 1);
    }
    // The response has already yielded its first verified member. Cancellation
    // must also work while its next body poll waits for a later member GET.
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    {
        let mut state = provider.lock().unwrap();
        state.requests.clear();
        state.change = Some((4, Change::Stall(entered.clone(), resume.clone())));
    }
    let spanning = Some(ByteRange::new(0, 512 * 1024 + 4095).unwrap());
    let mut output =
        crate::download_stream::file_record_byte_stream(store, record, spanning, pool.clone())
            .await
            .unwrap();
    assert_eq!(
        output.try_next().await.unwrap().unwrap().as_ref(),
        &payload[..512 * 1024]
    );
    let task = tokio::spawn(async move { output.try_next().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("later member must reach backend");
    assert_eq!(pool.available_buffer_permits(), 0);
    assert!(pool.try_acquire().is_some());
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert!(
        pool.try_acquire().is_some(),
        "partial response cancellation releases admission"
    );
    assert_eq!(pool.available_buffer_permits(), 1);
    resume.notify_one();
}
