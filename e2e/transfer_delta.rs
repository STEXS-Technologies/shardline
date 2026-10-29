use reqwest::{Client, StatusCode};
use serde_json::Value;

pub(super) async fn qualify(
    base: &str,
    token: &str,
    write: &str,
    file: &str,
    stable_xorb: &str,
    root: &std::path::Path,
) {
    let client = Client::new();
    let url = format!("{base}/v2/file-chunk-hashes/{file}");
    let response = client
        .get(&url)
        .bearer_auth(token)
        .header("X-Range-Dirty", "bytes=1-2")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["fileSize"], 8);
    assert_eq!(body["totalChunks"], 2);
    assert_eq!(
        body["windows"],
        serde_json::json!([{"dirtyByteRange": [0, 4]}])
    );
    assert_eq!(body["hashRanges"].as_array().unwrap().len(), 2);
    assert!(body["hashRanges"][0].is_null());
    assert!(body["hashRanges"][1].is_object());
    assert_eq!(body["gapVerification"].as_array().unwrap().len(), 1);
    // Consume the actual HTTP proof using the upstream client representation.
    // Replacing the first chunk must compose the same root as rehashing both.
    use xet_core_structures::merklehash::{MerkleHashSubtree, compute_data_hash, file_hash};
    let gap: MerkleHashSubtree = serde_json::from_value(body["hashRanges"][1].clone()).unwrap();
    let changed = (compute_data_hash(b"cccc"), 4);
    let replacement = MerkleHashSubtree::from_chunks(true, &[changed], false);
    // Both inputs cross the wire: upstream debug-only leaf tracking is absent
    // from serialized subtrees and cannot be mixed with in-memory tracking.
    let replacement: MerkleHashSubtree =
        serde_json::from_value(serde_json::to_value(replacement).unwrap()).unwrap();
    let merged = MerkleHashSubtree::merge(&[replacement, gap]).unwrap();
    let merged_root = merged.final_hash().unwrap();
    let composed = blake3::keyed_hash(&[0; 32], merged_root.as_bytes());
    assert_eq!(
        composed.as_bytes(),
        file_hash(&[changed, (compute_data_hash(b"bbbb"), 4)]).as_bytes()
    );
    let verification =
        xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks(&[
            compute_data_hash(b"bbbb"),
        ]);
    assert_eq!(body["gapVerification"][0], verification.hex());
    // Upgrade case: old source objects remain usable without derived indexes.
    let database = rusqlite::Connection::open(root.join("metadata.sqlite3")).unwrap();
    database
        .execute("DELETE FROM shardline_xet_xorbs", [])
        .unwrap();
    let recovered = client
        .get(&url)
        .bearer_auth(token)
        .header("X-Range-Dirty", "bytes=1-2")
        .send()
        .await
        .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
    assert_eq!(recovered.json::<Value>().await.unwrap(), body);
    let (hash, json): (String, String) = database
        .query_row(
            "SELECT xorb_hash, descriptor FROM shardline_xet_xorbs LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let mut corrupted: Value = serde_json::from_str(&json).unwrap();
    corrupted["chunks"][0]["hash"] = Value::String("0".repeat(64));
    database
        .execute(
            "UPDATE shardline_xet_xorbs SET descriptor=?1 WHERE xorb_hash=?2",
            rusqlite::params![corrupted.to_string(), hash],
        )
        .unwrap();
    let rejected = client
        .get(&url)
        .bearer_auth(token)
        .header("X-Range-Dirty", "bytes=1-2")
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::INTERNAL_SERVER_ERROR);
    database
        .execute(
            "UPDATE shardline_xet_xorbs SET descriptor=?1 WHERE xorb_hash=?2",
            rusqlite::params![json, hash],
        )
        .unwrap();
    super::transfer_delta_upload::qualify(
        base,
        token,
        write,
        stable_xorb,
        *composed.as_bytes(),
        &body,
    )
    .await;
    for header in [None, Some("bytes=8-9"), Some("bytes=2-1"), Some("bytes=")] {
        let mut request = client.get(&url).bearer_auth(token);
        if let Some(header) = header {
            request = request.header("X-Range-Dirty", header);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        client
            .get(&url)
            .header("X-Range-Dirty", "bytes=0-1")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let hidden = super::bearer_token(
        "other-user",
        super::TokenScope::Read,
        super::RepositoryProvider::GitHub,
        "other-team",
        "assets",
        Some("main"),
    )
    .unwrap();
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(hidden)
            .header("X-Range-Dirty", "bytes=0-1")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}
