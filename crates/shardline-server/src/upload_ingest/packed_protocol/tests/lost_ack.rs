use super::*;

#[tokio::test]
async fn committed_container_with_failed_acknowledgement_is_verified_before_reuse() {
    let bytes: Vec<_> = (0..256 * 1024).map(|index| (index % 251) as u8).collect();
    let legacy = Provider::new(0).await;
    let (expected, _) = legacy.ingest(&bytes, false, 4093).await.unwrap();
    let provider = Provider::new(4).await;
    let (record, _) = provider.ingest(&bytes, true, 7919).await.unwrap();
    assert_eq!(record, expected);
    let serialized = provider
        .state
        .lock()
        .unwrap()
        .objects
        .values()
        .next()
        .unwrap()
        .clone();
    let hash = shardline_index::parse_xet_hash_hex(&record.chunks[0].hash).unwrap();
    let mut cursor = std::io::Cursor::new(serialized);
    let validated = crate::xet_adapter::validate_serialized_xorb(&mut cursor, hash).unwrap();
    std::io::Seek::seek(&mut cursor, std::io::SeekFrom::Start(0)).unwrap();
    let mut actual = Vec::new();
    crate::xet_adapter::try_for_each_serialized_xorb_chunk_trusted(
        &mut cursor,
        &validated,
        |chunk| {
            actual.extend_from_slice(chunk.data());
            Ok::<(), ServerError>(())
        },
    )
    .unwrap();
    assert_eq!(actual, bytes);
    let state = provider.state.lock().unwrap();
    assert!(state.objects.keys().all(|path| path.contains("/xorbs/")));
    assert!(
        state
            .requests
            .iter()
            .any(|(method, _)| *method == Method::GET)
    );
    assert!(
        state
            .requests
            .iter()
            .filter(|(method, _)| *method == Method::PUT)
            .count()
            >= 2,
        "ambiguous write must exercise conditional retry and existing-object recovery"
    );
}
