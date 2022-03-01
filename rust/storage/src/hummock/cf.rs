use std::collections::BTreeSet;

use risingwave_common::error::Result;

use crate::hummock::HummockStorage;

/// `HummockColumnFamily` provides physical isolation of storage files.
#[derive(Clone, Debug)]
pub struct HummockColumnFamily {
    /// Use HummockStorage to store data.
    storage: HummockStorage,

    /// Encoded representation for all segments.
    cf_name: Vec<u8>,
}

impl HummockColumnFamily {
    pub fn new(storage: HummockStorage, cf_name: Vec<u8>) -> Self {
        Self { storage, cf_name }
    }

    pub fn name(&self) -> &[u8] {
        &self.cf_name
    }

    /// Put a batch to a column family.
    pub async fn put_batch(
        &self,
        batch: BTreeSet<(Vec<u8>, Option<Vec<u8>>)>,
        epoch: u64,
    ) -> Result<()> {
        let cf_batch = batch.into_iter().map(|(k, v)| (k, v.into()));
        self.storage.write_batch(cf_batch, epoch).await?;
        Ok(())
    }
}
