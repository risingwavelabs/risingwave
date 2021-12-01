use risingwave_common::error::{Result, ToRwResult};

use crate::storage::hummock::HummockStorage;

/// Provides API to read key-value pairs of a prefix in the storage backend.
pub struct Keyspace {
    storage: HummockStorage,
    prefix: Vec<u8>,
}

impl Keyspace {
    /// Get the key from the keyspace
    pub async fn get(&self) -> Result<Option<Vec<u8>>> {
        // TODO: make the HummockError into an I/O Error.
        Ok(self
            .storage
            .get(&self.prefix)
            .await
            .map_err(anyhow::Error::new)
            .to_rw_result()?)
    }

    pub fn prefix(&self) -> &[u8] {
        &self.prefix[..]
    }
}
