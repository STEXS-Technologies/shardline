use shardline_index::{
    AsyncIndexStore, FileId, FileReconstruction, MemoryIndexStore, MemoryIndexStoreError,
    ReconstructionStore,
};
use shardline_protocol::ShardlineHash;

#[derive(Debug)]
enum VisitError {
    Adapter,
    Stop,
}

impl std::fmt::Display for VisitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for VisitError {}

impl From<MemoryIndexStoreError> for VisitError {
    fn from(_: MemoryIndexStoreError) -> Self {
        Self::Adapter
    }
}

fn populated_store() -> Result<MemoryIndexStore, MemoryIndexStoreError> {
    let store = MemoryIndexStore::new();
    for byte in [3, 1, 2] {
        store.insert_reconstruction(
            &FileId::new(ShardlineHash::from_bytes([byte; 32])),
            &FileReconstruction::new(Vec::new()),
        )?;
    }
    Ok(store)
}

#[test]
fn default_reconstruction_visitor_preserves_inventory_and_stops_on_error()
-> Result<(), Box<dyn std::error::Error>> {
    let store = populated_store()?;
    let expected = ReconstructionStore::list_reconstruction_file_ids(&store)?;
    let mut actual = Vec::new();
    ReconstructionStore::visit_reconstruction_file_ids(&store, |id| {
        actual.push(id);
        Ok::<_, VisitError>(())
    })?;
    if actual != expected {
        return Err(std::io::Error::other("visitor inventory differs from list").into());
    }
    let mut visited = 0;
    let result = ReconstructionStore::visit_reconstruction_file_ids(&store, |_| {
        visited += 1;
        Err(VisitError::Stop)
    });
    if !matches!(result, Err(VisitError::Stop)) {
        return Err(std::io::Error::other("visitor did not preserve the stop error").into());
    }
    if visited != 1 {
        return Err(std::io::Error::other("visitor continued after the stop error").into());
    }
    Ok(())
}

#[tokio::test]
async fn default_async_reconstruction_visitor_supports_borrowed_callback_and_stops_on_error()
-> Result<(), Box<dyn std::error::Error>> {
    let store = populated_store()?;
    let expected = AsyncIndexStore::list_reconstruction_file_ids(&store).await?;
    let mut actual = Vec::new();
    AsyncIndexStore::visit_reconstruction_file_ids(&store, |id| {
        actual.push(id);
        Ok::<_, VisitError>(())
    })
    .await?;
    if actual != expected {
        return Err(std::io::Error::other("visitor inventory differs from list").into());
    }
    let mut visited = 0;
    let result = AsyncIndexStore::visit_reconstruction_file_ids(&store, |_| {
        visited += 1;
        Err(VisitError::Stop)
    })
    .await;
    if !matches!(result, Err(VisitError::Stop)) {
        return Err(std::io::Error::other("visitor did not preserve the stop error").into());
    }
    if visited != 1 {
        return Err(std::io::Error::other("visitor continued after the stop error").into());
    }
    Ok(())
}
