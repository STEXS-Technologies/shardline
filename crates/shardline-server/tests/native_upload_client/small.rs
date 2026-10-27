//! Real-client coverage of the bounded memory-backed Xorb publication path.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit stock hf_xet small upload and cold-client replay qualification"]
async fn stock_xet_small_upload_preserves_memory_publication_and_cold_replay() -> Result {
    let root = tempfile::tempdir()?;
    let working = tempfile::tempdir()?;
    let source = working.path().join("small.bin");
    let mut content = vec![0; 64 * 1024];
    blake3::Hasher::new().finalize_xof().fill(&mut content);
    std::fs::write(&source, &content)?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let base = format!("http://{address}");
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "small", "retry", None)?;
    let token = LocalHmacProvider::new(KEY)?.mint_token(&TokenClaims::new(
        "shardline",
        "small-native-client",
        TokenScope::Write,
        scope,
        u64::MAX,
    )?)?;
    let mut server = start_with_environment(root.path(), address, None, &[])?;
    ready(&reqwest::Client::new(), &base, &mut server).await?;
    let mut first_hash = None;
    for attempt in ["first", "cold-replay"] {
        let home = working.path().join(attempt);
        let mut process = client(&source, &base, &token, &home)?;
        let status = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Some(status) = process.0.try_wait()? {
                    return Ok::<_, std::io::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        assert!(
            status.success(),
            "small stock-client transfer failed; credential-bearing client output omitted"
        );
        let result: serde_json::Value =
            serde_json::from_slice(&std::fs::read(home.join("result.json"))?)?;
        assert_eq!(
            result.get("size").and_then(serde_json::Value::as_u64),
            Some(content.len() as u64)
        );
        assert_eq!(std::fs::read(home.join("restored.bin"))?, content);
        let current_hash = result
            .get("hash")
            .and_then(serde_json::Value::as_str)
            .ok_or("stock client omitted content hash")?;
        match &first_hash {
            Some(hash) => assert_eq!(current_hash, hash),
            None => first_hash = Some(current_hash.to_owned()),
        }
    }
    let index = shardline_index::LocalIndexStore::new(root.path().to_owned())?;
    let visible = index.intents_by_state(UploadIntentState::Visible).await?;
    let xorbs: Vec<_> = visible
        .iter()
        .filter(|intent| intent.intent_id().starts_with("xorb-v2-"))
        .collect();
    assert!(!xorbs.is_empty(), "stock client must publish native Xorbs");
    assert!(
        xorbs
            .iter()
            .all(|intent| intent.object_length() <= 256 * 1024),
        "native uploads must exercise memory staging"
    );
    Ok(())
}
