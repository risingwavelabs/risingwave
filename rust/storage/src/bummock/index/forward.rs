use std::collections::BTreeSet;

use parking_lot::Mutex;
use risingwave_common::error::Result;

use crate::hummock::cf::HummockColumnFamily;
use crate::hummock::HummockStorage;

type IndexEntry = (Vec<u8>, Option<Vec<u8>>);

#[derive(Debug)]
pub struct HummockForwardIndexer {
    /// The column family to store the inverted index.
    cf: HummockColumnFamily,

    /// Indexing is based on batches.
    wbuffer: Mutex<BTreeSet<IndexEntry>>,
}

/// Use Hummock storage as forward indexing backend.
/// Usually it's used for primary key index.
/// A key would be encoded to `| pk | epoch |`.
/// A value would be encoded to a byte stream (e.g. jsonb encoded).
impl HummockForwardIndexer {
    pub fn new(storage: HummockStorage, cf_name: Vec<u8>) -> Self {
        let cf = HummockColumnFamily::new(storage, cf_name);

        Self {
            cf,
            wbuffer: Mutex::new(BTreeSet::new()),
        }
    }

    /// TODO(xiangyhu) multithreaded indexing should be supported.
    pub fn index_entry(&self, k: &[u8], v: &[u8]) -> Result<()> {
        self.wbuffer.lock().insert((k.to_vec(), Some(v.to_vec())));
        Ok(())
    }

    /// Stamp an epoch and persist the forward index.
    pub async fn commit(&self, epoch: u64) -> Result<()> {
        self.cf
            .put_batch(self.wbuffer.lock().to_owned(), epoch)
            .await?;

        Ok(())
    }
}
