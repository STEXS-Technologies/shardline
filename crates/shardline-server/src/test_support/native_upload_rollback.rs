//! Protect every new native and raw xorb checkpoint from released recovery, with and without
//! a physical request key. This tests the writer protocol, not domain publication.
use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

use crate::{
    object_store::ServerObjectStore,
    test_fixtures::{single_chunk_xorb, single_file_shard},
};

pub(crate) struct Case {
    pub(crate) intent: UploadIntent,
    pub(crate) state: UploadIntentState,
    events: serde_json::Value,
}

pub(crate) async fn seed<S: UploadIntentStore>(index: &S, objects: &ServerObjectStore) -> Vec<Case>
where
    S::Error: std::fmt::Debug,
{
    let mut cases = Vec::new();
    for present in [false, true] {
        for (ordinal, state) in [
            UploadIntentState::Created,
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
        ]
        .into_iter()
        .enumerate()
        {
            let payload = format!("native rollback request {present} {ordinal}");
            let (xorb, hash) = single_chunk_xorb(payload.as_bytes());
            let xorb_key = crate::xet_adapter::xorb_object_key(&hash).unwrap();
            if present {
                objects
                    .put_if_absent(
                        &xorb_key,
                        ObjectBody::from_vec(xorb.to_vec()),
                        &ObjectIntegrity::new(
                            crate::local_backend::chunk_hash(&xorb),
                            xorb.len() as u64,
                        ),
                    )
                    .unwrap();
            }
            let xorb_intent = UploadIntent::new(
                crate::upload_identity::xorb(&hash, xorb.len() as u64),
                xorb_key.as_str().to_owned(),
                hash.clone(),
                xorb.len() as u64,
            );
            index.create_intent(&xorb_intent).await.unwrap();
            for next in [
                UploadIntentState::Storing,
                UploadIntentState::Stored,
                UploadIntentState::MetadataCommitted,
            ]
            .into_iter()
            .take(ordinal)
            {
                assert!(
                    index
                        .transition_intent(xorb_intent.intent_id(), next)
                        .await
                        .unwrap()
                );
            }
            let events = serde_json::to_value(
                index
                    .reliability_events(xorb_intent.intent_id())
                    .await
                    .unwrap(),
            )
            .unwrap();
            cases.push(Case {
                intent: xorb_intent,
                state,
                events,
            });
            let (body, _file_hash) = single_file_shard(&[(payload.as_bytes(), &hash)]);
            let body_hash = blake3::hash(&body).to_hex().to_string();
            let key = shardline_storage::ObjectKey::parse(&format!(
                "shards/{}/{body_hash}.shard",
                body_hash.get(..2).unwrap()
            ))
            .unwrap();
            if present {
                objects
                    .put_if_absent(
                        &key,
                        ObjectBody::from_vec(body.to_vec()),
                        &ObjectIntegrity::new(
                            crate::local_backend::chunk_hash(&body),
                            body.len() as u64,
                        ),
                    )
                    .unwrap();
            }
            let scope = RepositoryScope::new(
                RepositoryProvider::GitHub,
                "rollback",
                &format!("native-{present}-{ordinal}"),
                None,
            )
            .unwrap();
            for repository_scope in [None, Some(&scope)] {
                let (id, repository) =
                    crate::upload_identity::native_shard(&body_hash, repository_scope).unwrap();
                let intent = UploadIntent::new(
                    id,
                    key.as_str().to_owned(),
                    body_hash.clone(),
                    body.len() as u64,
                );
                index
                    .create_intent_scoped(&intent, "shardline", &repository)
                    .await
                    .unwrap();
                for next in [
                    UploadIntentState::Storing,
                    UploadIntentState::Stored,
                    UploadIntentState::MetadataCommitted,
                ]
                .into_iter()
                .take(ordinal)
                {
                    assert!(
                        index
                            .transition_intent(intent.intent_id(), next)
                            .await
                            .unwrap()
                    );
                }
                let events = serde_json::to_value(
                    index.reliability_events(intent.intent_id()).await.unwrap(),
                )
                .unwrap();
                cases.push(Case {
                    intent,
                    state,
                    events,
                });
            }
        }
    }
    cases
}

pub(crate) async fn assert_preserved<S: UploadIntentStore>(index: &S, cases: &[Case], logs: &str)
where
    S::Error: std::fmt::Debug,
{
    for case in cases {
        assert!(
            logs.contains(case.intent.intent_id()),
            "released recovery must actually visit the intent"
        );
        assert_eq!(
            index
                .intent_by_id(case.intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            case.state
        );
        assert_eq!(
            serde_json::to_value(
                index
                    .reliability_events(case.intent.intent_id())
                    .await
                    .unwrap()
            )
            .unwrap(),
            case.events
        );
    }
}
