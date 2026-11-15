//! Slow source verification must release discovery locks; publication still fences deletion.
use super::*;
use crate::test_support::postgres_fixture::PostgresFixture;
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_slow_reuse_verification_releases_discovery_and_rechecks_publication() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture.migrate().await;
    let provider = Provider::new(0).await;
    let url = fixture.config.index_postgres_url().unwrap();
    let backend = crate::PostgresBackend::new_with_object_store(
        fixture.config.root_dir().to_owned(),
        "http://localhost".into(),
        NonZeroUsize::new(64 * 1024).unwrap(),
        url,
        provider.objects.clone(),
    )
    .await
    .unwrap()
    .with_single_connection_pools(url)
    .unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "repo", None).unwrap();
    let content = Bytes::from_static(b"verified remote chunks must not pin discovery transactions");
    backend
        .upload_protocol_stream(
            "source",
            crate::upload_ingest::RequestBodyReader::from_bytes(content.clone()),
            None,
            Some(&scope),
        )
        .await
        .unwrap();
    let source = backend.protocol_file_record("source").await.unwrap();
    let key = crate::xet_adapter::xorb_object_key(&source.chunks.first().unwrap().hash).unwrap();
    assert_eq!(
        provider
            .state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|(method, path)| *method == Method::GET && path.contains("/xorbs/"))
            .count(),
        1,
        "fresh container registration must share the verified read-back metadata"
    );
    provider.reads.armed.store(true, Ordering::SeqCst);
    let target = backend.clone();
    let upload = tokio::spawn(async move {
        target
            .upload_protocol_stream(
                "target",
                crate::upload_ingest::RequestBodyReader::from_bytes(content),
                None,
                Some(&scope),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), provider.reads.entered.notified())
        .await
        .unwrap();
    let connection = tokio::time::timeout(
        Duration::from_secs(2),
        backend.index_store().pool().acquire(),
    )
    .await;
    let connection_released = connection.as_ref().is_ok_and(|value| value.is_ok());
    drop(connection);
    let admission = tokio::time::timeout(
        Duration::from_secs(2),
        fixture.index().begin_remote_delete(&key),
    )
    .await;
    provider.reads.armed.store(false, Ordering::SeqCst);
    provider.reads.resume.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(10), upload)
        .await
        .unwrap()
        .unwrap();
    assert!(
        connection_released,
        "slow verification retained the only metadata connection"
    );
    let permit = admission
        .expect("slow verification retained deletion admission locks")
        .unwrap();
    assert!(
        outcome.is_err(),
        "publication must reject deletion admitted during verification"
    );
    assert!(matches!(
        backend.protocol_file_record("target").await,
        Err(ServerError::NotFound)
    ));
    assert!(fixture.index().finish_remote_delete(&permit).await.unwrap());
    backend.index_store().pool().close().await;
    fixture.finish().await;
}
