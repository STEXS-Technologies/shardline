//! Bounded deletion of already-qualified, unique temporary-upload keys.

use std::collections::HashSet;

use futures_util::{StreamExt, stream, stream::BoxStream};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

use super::S3ObjectStoreError;

pub(super) const BATCH_SIZE: usize = 1_000;

pub(in crate::s3) async fn delete_batch(
    store: &impl ObjectStore,
    single_delete_store: &impl ObjectStore,
    locations: Vec<Path>,
) -> Result<(), S3ObjectStoreError> {
    if locations.is_empty() {
        return Ok(());
    }
    let expected = locations.iter().cloned().collect();
    let results = store.delete_stream(stream::iter(locations.clone().into_iter().map(Ok)).boxed());
    if verify_results(expected, results).await.is_ok() {
        return Ok(());
    }
    // Preserve compatibility with providers lacking bulk delete. Repeating a
    // deletion is safe here: these unique generated temporary keys are never
    // reused for live content. Also handles partially successful batches.
    for location in locations {
        match single_delete_store.delete(&location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(S3ObjectStoreError::External(error)),
        }
    }
    Ok(())
}

async fn verify_results(
    mut pending: HashSet<Path>,
    mut results: BoxStream<'_, object_store::Result<Path>>,
) -> Result<(), S3ObjectStoreError> {
    while let Some(result) = results.next().await {
        let location = result.map_err(S3ObjectStoreError::External)?;
        if !pending.remove(&location) {
            return Err(invalid_response());
        }
    }
    if !pending.is_empty() {
        return Err(invalid_response());
    }
    Ok(())
}

fn invalid_response() -> S3ObjectStoreError {
    S3ObjectStoreError::External(object_store::Error::Generic {
        store: "S3 temporary cleanup",
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "temporary deletion response omitted or repeated a key, or returned an unexpected key",
        )
        .into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn partial_missing_duplicate_and_unexpected_results_fail() {
        let a = Path::from("__tmp/a");
        let b = Path::from("__tmp/b");
        for results in [
            vec![
                Ok(a.clone()),
                Err(object_store::Error::NotFound {
                    path: b.to_string(),
                    source: std::io::Error::from(std::io::ErrorKind::NotFound).into(),
                }),
            ],
            vec![Ok(a.clone())],
            vec![Ok(a.clone()), Ok(a.clone()), Ok(b.clone())],
            vec![Ok(Path::from("other")), Ok(b.clone())],
        ] {
            assert!(
                verify_results(
                    HashSet::from([a.clone(), b.clone()]),
                    stream::iter(results).boxed(),
                )
                .await
                .is_err()
            );
        }
        assert!(
            verify_results(
                HashSet::from([a.clone(), b.clone()]),
                stream::iter([Ok(b), Ok(a)]).boxed(),
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn concurrent_cleanup_and_retry_preserve_unselected_objects() {
        let store = InMemory::new();
        let keys: Vec<_> = (0..BATCH_SIZE)
            .map(|n| Path::from(format!("__tmp/{n}")))
            .collect();
        let sentinel = Path::from("live-object");
        store.put(&sentinel, "live".into()).await.unwrap();
        for key in &keys {
            store.put(key, "temp".into()).await.unwrap();
        }
        let (first, second) = tokio::join!(
            delete_batch(&store, &store, keys.clone()),
            delete_batch(&store, &store, keys.clone()),
        );
        first.unwrap();
        second.unwrap();
        delete_batch(&store, &store, keys).await.unwrap();
        delete_batch(&store, &store, Vec::new()).await.unwrap();
        assert_eq!(
            store.get(&sentinel).await.unwrap().bytes().await.unwrap(),
            "live"
        );
        assert_eq!(store.list(None).collect::<Vec<_>>().await.len(), 1);
    }
}
