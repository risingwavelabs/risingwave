use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;
mod hummock;
pub use hummock::*;
mod memory;
pub use memory::*;
mod panic_store;
pub use panic_store::*;

#[async_trait]
pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter: StateStoreIter<Item = (Bytes, Bytes)>;

    /// Point get a value from the state store.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Scan `limit` number of keys from the keyspace. If `limit` is `None`, scan all elements.
    ///
    /// TODO: this interface should be refactored to return an iterator in the future. And in some
    /// cases, the scan can be optimized into a `multi_get` request.
    async fn scan(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>>;

    /// Ingest a batch of data into the state store. One write batch should never contain operation
    /// on the same key. e.g. Put(233, x) then Delete(233).
    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()>;

    fn iter(&self, prefix: &[u8]) -> Self::Iter;
}

#[async_trait]
pub trait StateStoreIter: Send + Sync + 'static {
    type Item;

    async fn open(&mut self) -> Result<()>;

    async fn next(&mut self) -> Result<Option<Self::Item>>;
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
        self.store.get(&self.prefix).await
    }

    /// Scan `limit` keys from the keyspace. If `limit` is None, all keys of the given prefix will
    /// be returned.
    pub async fn scan(&self, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        self.store.scan(&self.prefix, limit).await
    }

    pub fn prefix(&self) -> &[u8] {
        &self.prefix[..]
    }

    /// Get the underlying state store.
    pub fn state_store(&self) -> S {
        self.store.clone()
    }

    /// Get a sub-keyspace.
    pub fn keyspace(&self, sub_prefix: &[u8]) -> Self {
        Self {
            store: self.store.clone(),
            prefix: [&self.prefix[..], sub_prefix].concat(),
        }
    }
}
