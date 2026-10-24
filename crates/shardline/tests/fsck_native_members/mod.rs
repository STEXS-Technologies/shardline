//! Actual multi-member container publication and missing-copy scanner behavior.
use super::*;
use shardline_xet_core::{
    merklehash::{compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

pub(super) struct Published {
    members: Vec<String>,
    identity: String,
    bytes: Vec<u8>,
}

pub(super) async fn publish(client: &reqwest::Client, base: &str) -> Result<Published> {
    let mut data = Vec::new();
    let mut leaves = Vec::new();
    let mut entries = Vec::new();
    for index in 0_u8..33 {
        let member = vec![index; 64 * 1024];
        let hash = compute_data_hash(&member);
        let length = u64::try_from(member.len())?;
        entries.push(XorbChunkSequenceEntry::new(
            hash,
            length,
            u64::try_from(data.len())?,
        ));
        data.extend_from_slice(&member);
        leaves.push((hash, length));
    }
    let hash = xorb_hash(&leaves);
    let identity = shardline_server::test_fixtures::xet_hash_hex(&hash);
    let mut end = 0_u64;
    let boundaries = leaves
        .iter()
        .map(|(member, length)| {
            end = end
                .checked_add(*length)
                .ok_or("fixture boundary overflow")?;
            Ok((*member, end))
        })
        .collect::<Result<Vec<_>>>()?;
    let serialized = serialized_xorb_object_from_components(
        &hash,
        data.clone(),
        boundaries,
        CompressionScheme::None,
    )?;
    client
        .post(format!("{base}/v1/xorbs/default/{identity}"))
        .body(serialized.serialized_data.clone())
        .send()
        .await?
        .error_for_status()?;
    let mut shard = MDBInMemoryShard::default();
    shard.add_xorb_block(MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(
            hash,
            u64::try_from(leaves.len())?,
            u64::try_from(data.len())?,
        ),
        chunks: entries,
    })?;
    let file = file_hash(&leaves);
    shard.add_file_reconstruction_info(MDBFileInfo {
        metadata: FileDataSequenceHeader::new(file, 1, false, false),
        segments: vec![FileDataSequenceEntry::new(
            hash,
            u64::try_from(data.len())?,
            0,
            u64::try_from(leaves.len())?,
        )],
        verification: Vec::new(),
        metadata_ext: None,
    })?;
    client
        .post(format!("{base}/v1/shards"))
        .body(shard.to_bytes()?)
        .send()
        .await?
        .error_for_status()?;
    Ok(Published {
        members: leaves
            .iter()
            .map(|(member, _)| shardline_server::test_fixtures::xet_hash_hex(member))
            .collect(),
        identity,
        bytes: serialized.serialized_data,
    })
}

pub(super) fn qualify(
    root: &Path,
    remote: Option<&RemoteStorage>,
    published: &Published,
) -> Result<()> {
    if let Some(remote) = remote {
        let store = shardline_server_core::ServerObjectStore::s3(remote.config.clone())?;
        for member in &published.members {
            let key = shardline_server_core::chunk_object_key(member)?;
            shardline_storage::ObjectStore::delete_if_present(&store, &key)?;
        }
    } else {
        for member in &published.members {
            fs::remove_file(root.join("chunks").join(&member[..2]).join(member))?;
        }
    }
    assert_status(&scan_configured(root, remote)?, 0);
    let key = shardline_storage::ObjectKey::parse(&format!(
        "xorbs/default/{}/{}.xorb",
        &published.identity[..2],
        published.identity
    ))?;
    let store = if let Some(remote) = remote {
        shardline_server_core::ServerObjectStore::s3(remote.config.clone())?
    } else {
        shardline_server_core::ServerObjectStore::local(root.join("chunks"))?
    };
    let mut corrupt = published.bytes.clone();
    let middle = corrupt.len() / 2;
    *corrupt.get_mut(middle).ok_or("missing fixture payload")? ^= 1;
    for (bytes, status) in [(corrupt.as_slice(), 1), (published.bytes.as_slice(), 0)] {
        store.put_overwrite(
            &key,
            shardline_storage::ObjectBody::from_slice(bytes),
            &shardline_storage::ObjectIntegrity::new(
                shardline_server_core::chunk_hash(bytes),
                u64::try_from(bytes.len())?,
            ),
        )?;
        assert_status(&scan_configured(root, remote)?, status);
    }
    Ok(())
}
