//! Reject contradictory native file identities before durable publication.

use super::{HubRuntime, RepositoryProvider, TestError};
use shardline_protocol::RepositoryScope;
use shardline_xet_core::{
    merklehash::{MerkleHash, compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, MDBFileInfo,
        },
        shard_in_memory::MDBInMemoryShard,
    },
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

pub(super) async fn reject_forged_identities(runtime: &HubRuntime) -> Result<(), TestError> {
    let client = reqwest::Client::new();
    let data = b"valid Xet Merkle content with a forged SHA-256 identity";
    let member = compute_data_hash(data);
    let length = u64::try_from(data.len())?;
    let xorb = xorb_hash(&[(member, length)]);
    let serialized = serialized_xorb_object_from_components(
        &xorb,
        data.to_vec(),
        vec![(member, length)],
        CompressionScheme::LZ4,
    )?;
    client
        .post(format!(
            "{}/v1/xorbs/default/{}",
            runtime.base_url,
            xorb.hex()
        ))
        .bearer_auth(&runtime.model_token)
        .body(serialized.serialized_data)
        .send()
        .await?
        .error_for_status()?;

    let forged = "a".repeat(64);
    for nonempty in [false, true] {
        let mut shard = MDBInMemoryShard::default();
        let leaves = if nonempty {
            vec![(member, length)]
        } else {
            Vec::new()
        };
        shard.add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash(&leaves),
                u64::from(nonempty),
                false,
                true,
            ),
            segments: if nonempty {
                vec![FileDataSequenceEntry::new(xorb, length, 0, 1)]
            } else {
                Vec::new()
            },
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(MerkleHash::from_hex(&forged)?)),
        })?;
        let response = client
            .post(format!("{}/v1/shards", runtime.base_url))
            .bearer_auth(&runtime.model_token)
            .header("Content-Type", "application/octet-stream")
            .body(shard.to_bytes()?)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "cli-model", None)?;
        let identity = if let Some(url) = &runtime.postgres_url {
            let pool = sqlx::PgPool::connect(url).await?;
            let records = shardline_index::PostgresRecordStore::new(pool.clone());
            let identity = records.xet_file_identity(&scope, &forged).await?;
            pool.close().await;
            identity
        } else {
            shardline_index::LocalRecordStore::open(runtime._storage.path().to_path_buf())
                .xet_file_identity(&scope, &forged)?
        };
        assert!(
            identity.is_none(),
            "rejected shard published a forged file identity"
        );
    }
    Ok(())
}
