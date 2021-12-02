use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;
mod hummock;
pub use hummock::*;
mod memory;
pub use memory::*;

#[async_trait]
pub trait StateStore {
    /// Point get a value from the state store.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Ingest a batch of data into the state store.
    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()>;
}

/// Provides API to read key-value pairs of a prefix in the storage backend.
pub struct Keyspace<S: StateStore> {
    store: S,
    prefix: Vec<u8>,
}

impl<S: StateStore> Keyspace<S> {
    pub fn new(store: S, prefix: Vec<u8>) -> Self {
        Self { store, prefix }
    }

    /// Get the key from the keyspace
    pub async fn get(&self) -> Result<Option<Bytes>> {
        // TODO: make the HummockError into an I/O Error.
        self.store.get(&self.prefix).await
    }

    pub fn prefix(&self) -> &[u8] {
        &self.prefix[..]
    }
}
