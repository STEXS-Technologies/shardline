//! A response assembled across provider requests must still match the trusted root.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_changes_between_footer_and_member_reads_never_expose_wrong_content() {
    let provider = Arc::new(Mutex::new(Provider::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(request).with_state(provider.clone());
    let _server = StopServer(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
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
    let mut seed = 736489_u64;
    let payload: Vec<u8> = (0..1536 * 1024)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect();
    let original =
        super::super::container(&local, &payload.chunks(512 * 1024).collect::<Vec<_>>()).await;
    let alternative: Vec<u8> = payload.iter().map(|byte| byte ^ 0xff).collect();
    let other =
        super::super::container(&local, &alternative.chunks(512 * 1024).collect::<Vec<_>>()).await;
    let load = |hash: &str| {
        let key = crate::xet_adapter::xorb_object_key(hash).unwrap();
        let length = local.metadata(&key).unwrap().unwrap().length();
        Bytes::from(
            local
                .read_range(&key, ByteRange::new(0, length - 1).unwrap())
                .unwrap(),
        )
    };
    let expected = load(&original[0].hash);
    let wrong_root = load(&other[0].hash);
    assert_eq!(
        expected.len(),
        wrong_root.len(),
        "same layout isolates root/member authentication"
    );
    let key = crate::xet_adapter::xorb_object_key(&original[0].hash).unwrap();
    let path = format!("/fixture/{}", key.as_str());
    let record = super::super::file(original);
    let range = Some(ByteRange::new(0, 4095).unwrap());
    for phase in [1, 2, 3] {
        for change in [
            Change::Replace(wrong_root.clone()),
            Change::Remove,
            Change::Deny,
        ] {
            {
                let mut state = provider.lock().unwrap();
                state.objects.insert(path.clone(), expected.clone());
                state.requests.clear();
                state.change = Some((phase, change));
            }
            assert!(
                super::super::collect(&store, &record, range).await.is_err(),
                "provider changes at GET {phase} must not produce a valid response"
            );
        }
    }
    // All failed attempts release bounded work admission and leave no read-side
    // durable state; the same identity can be read immediately after restoration.
    provider.lock().unwrap().objects.insert(path, expected);
    assert_eq!(
        super::super::collect(&store, &record, range).await.unwrap(),
        payload[..4096]
    );
}
