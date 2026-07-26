use std::time::Duration;

/// Persisted state of a content-addressed upload intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadIntentState {
    /// Intent was created but no bytes have been written yet.
    Created,
    /// Bytes are being written to the object store.
    Storing,
    /// Bytes have been written, metadata commit has not started.
    Stored,
    /// Metadata write has started but not completed.
    MetadataCommitted,
    /// The upload completed successfully and the record is visible.
    Visible,
    /// The upload failed and should be reconciled or quarantined.
    Failed,
}

impl UploadIntentState {
    /// Returns the database string representation.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Storing => "storing",
            Self::Stored => "stored",
            Self::MetadataCommitted => "metadata_committed",
            Self::Visible => "visible",
            Self::Failed => "failed",
        }
    }

    /// Parses a database string.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "created" => Some(Self::Created),
            "storing" => Some(Self::Storing),
            "stored" => Some(Self::Stored),
            "metadata_committed" => Some(Self::MetadataCommitted),
            "visible" => Some(Self::Visible),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Returns whether a durable intent may move to the requested next state.
    ///
    /// Repeating a state is permitted so retries are idempotent. A failed
    /// operation is terminal and no state may skip a persistence boundary.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Created, Self::Created | Self::Storing | Self::Failed)
                | (Self::Storing, Self::Storing | Self::Stored | Self::Failed)
                | (
                    Self::Stored,
                    Self::Stored | Self::MetadataCommitted | Self::Failed
                )
                | (
                    Self::MetadataCommitted,
                    Self::MetadataCommitted | Self::Visible | Self::Failed
                )
                | (Self::Visible, Self::Visible)
                | (Self::Failed, Self::Failed)
        )
    }
}

/// A durable upload intent record.
#[derive(Debug, Clone)]
pub struct UploadIntent {
    intent_id: String,
    object_key: String,
    object_hash: String,
    object_length: u64,
    state: UploadIntentState,
    created_at: Duration,
    updated_at: Duration,
}

impl UploadIntent {
    /// Creates a new upload intent record.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(
        intent_id: String,
        object_key: String,
        object_hash: String,
        object_length: u64,
    ) -> Self {
        Self {
            intent_id,
            object_key,
            object_hash,
            object_length,
            state: UploadIntentState::Created,
            created_at: Duration::ZERO,
            updated_at: Duration::ZERO,
        }
    }

    /// Returns the unique intent identifier.
    #[must_use]
    pub fn intent_id(&self) -> &str {
        &self.intent_id
    }

    /// Returns the object key targeted by this upload.
    #[must_use]
    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    /// Returns the content hash of the object.
    #[must_use]
    pub fn object_hash(&self) -> &str {
        &self.object_hash
    }

    /// Returns the object length in bytes.
    #[must_use]
    pub const fn object_length(&self) -> u64 {
        self.object_length
    }

    /// Returns the current intent state.
    #[must_use]
    pub const fn state(&self) -> UploadIntentState {
        self.state
    }

    /// Returns the creation timestamp.
    #[must_use]
    pub const fn created_at(&self) -> Duration {
        self.created_at
    }

    /// Returns the last-updated timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> Duration {
        self.updated_at
    }

    /// Constructs an `UploadIntent` from raw parts, including timestamps.
    ///
    /// This is intended for store implementations that reconstruct intents
    /// from database rows.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub(crate) fn from_parts(
        intent_id: String,
        object_key: String,
        object_hash: String,
        object_length: u64,
        state: UploadIntentState,
        created_at: Duration,
        updated_at: Duration,
    ) -> Self {
        Self {
            intent_id,
            object_key,
            object_hash,
            object_length,
            state,
            created_at,
            updated_at,
        }
    }
}

/// Durable storage for upload intent records.
#[async_trait::async_trait]
pub trait UploadIntentStore: Send + Sync {
    /// Adapter-specific error type.
    type Error: Send + Sync;

    /// Persists a new upload intent in `Created` state.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the persistence operation fails.
    async fn create_intent(&self, intent: &UploadIntent) -> Result<(), Self::Error>;

    /// Transitions an intent to a new state.
    ///
    /// Returns `false` if the intent does not exist or the transition is invalid.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the persistence operation fails.
    async fn transition_intent(
        &self,
        intent_id: &str,
        new_state: UploadIntentState,
    ) -> Result<bool, Self::Error>;

    /// Loads an intent by ID.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the lookup fails.
    async fn intent_by_id(&self, intent_id: &str) -> Result<Option<UploadIntent>, Self::Error>;

    /// Lists all intents in a given state.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the query fails.
    async fn intents_by_state(
        &self,
        state: UploadIntentState,
    ) -> Result<Vec<UploadIntent>, Self::Error>;

    /// Lists all intents that have been in a non-terminal state for longer than
    /// the given duration.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the query fails.
    async fn stale_intents(
        &self,
        state: UploadIntentState,
        older_than: Duration,
    ) -> Result<Vec<UploadIntent>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_STATES: &[UploadIntentState] = &[
        UploadIntentState::Created,
        UploadIntentState::Storing,
        UploadIntentState::Stored,
        UploadIntentState::MetadataCommitted,
        UploadIntentState::Visible,
        UploadIntentState::Failed,
    ];

    #[test]
    fn state_as_str_and_parse_round_trip_all_variants() {
        for state in ALL_STATES {
            let s = state.as_str();
            let parsed = UploadIntentState::parse(s);
            assert_eq!(
                parsed,
                Some(*state),
                "round-trip failed for state {state:?}"
            );
        }
    }

    #[test]
    fn parse_returns_none_for_invalid_strings() {
        assert_eq!(UploadIntentState::parse(""), None);
        assert_eq!(UploadIntentState::parse("unknown"), None);
        assert_eq!(UploadIntentState::parse("CREATED"), None);
        assert_eq!(UploadIntentState::parse(" "), None);
    }

    #[test]
    fn new_creates_intent_with_correct_initial_values() {
        let intent = UploadIntent::new(
            "intent-1".to_owned(),
            "objects/my-file".to_owned(),
            "abcdef0123456789".to_owned(),
            12345,
        );
        assert_eq!(intent.state(), UploadIntentState::Created);
        assert_eq!(intent.created_at(), Duration::ZERO);
        assert_eq!(intent.updated_at(), Duration::ZERO);
        assert_eq!(intent.intent_id(), "intent-1");
        assert_eq!(intent.object_key(), "objects/my-file");
        assert_eq!(intent.object_hash(), "abcdef0123456789");
        assert_eq!(intent.object_length(), 12345);
    }

    #[test]
    fn accessors_return_expected_values() {
        let intent = UploadIntent::new(
            "test-intent".to_owned(),
            "key/obj".to_owned(),
            "hash123".to_owned(),
            999,
        );
        assert_eq!(intent.intent_id(), "test-intent");
        assert_eq!(intent.object_key(), "key/obj");
        assert_eq!(intent.object_hash(), "hash123");
        assert_eq!(intent.object_length(), 999);
    }
}
