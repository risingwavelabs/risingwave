//! Hummock is the state store of the streaming system.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::hummock::iterator::HummockIterator;
use crate::hummock::table::format::user_key;

mod table;
pub use table::*;
mod cloud;
mod error;
mod iterator;
mod key_range;
mod value;
mod version_manager;
use cloud::gen_remote_table;
pub use error::*;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use value::*;
use version_manager::VersionManager;

use self::iterator::SortedIterator;
use self::table::format::key_with_ts;
use crate::object::ObjectStore;

pub static REMOTE_DIR: &str = "/test/";

#[derive(Default, Debug, Clone)]
pub struct HummockOptions {
    /// target size of the table
    pub table_size: u32,
    /// size of each block in bytes in SST
    pub block_size: u32,
    /// false positive probability of Bloom filter
    pub bloom_false_positive: f64,
    /// remote directory for storing data and metadata objects
    pub remote_dir: String,
    /// checksum algorithm
    pub checksum_algo: ChecksumAlg,
}

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    options: Arc<HummockOptions>,
    unique_id: Arc<AtomicU64>,
    version_manager: Arc<VersionManager>,
    obj_client: Arc<dyn ObjectStore>,
}

impl HummockStorage {
    pub fn new(obj_client: Arc<dyn ObjectStore>, options: HummockOptions) -> Self {
        Self {
            options: Arc::new(options),
            unique_id: Arc::new(AtomicU64::new(0)),
            version_manager: Arc::new(VersionManager::new()),
            obj_client,
        }
    }

    /// Get the latest value of a specified `key`.
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get(&self, key: &[u8]) -> HummockResult<Option<Vec<u8>>> {
        let mut table_iters: Vec<Box<dyn HummockIterator + Send + Sync>> = Vec::new();

        for table in self.version_manager.tables().unwrap().iter() {
            // bloom filter tells us the key could possibly exist, go get it
            if !table.surely_not_have(key) {
                let iter = Box::new(TableIterator::new(table.clone()));
                table_iters.push(iter);
            }
        }

        let mut it = SortedIterator::new(table_iters);

        // Use `SortedIterator` to seek for they key with latest version to
        // get the latest key. `Err(EOF)` will be directly transformed to `Ok(None)`.
        if let Err(err) = it.seek(&key_with_ts(key.to_vec(), u64::MAX)).await {
            match err {
                HummockError::EOF => return Ok(None),
                _ => return Err(err),
            }
        }

        // Iterator has seeked passed the borders.
        if !it.is_valid() {
            return Ok(None);
        }

        // Iterator gets us the key, we tell if it's the key we want
        // or key next to it.
        match user_key(it.key()?) == key {
            true => Ok(it.value()?.into_put_value().map(|x| x.to_vec())),
            false => Ok(None),
        }
    }

    /// Write batch to storage. The batch should be:
    /// * Ordered. KV pairs will be directly written to the table, so it must be ordered.
    /// * Locally unique. There should not be two or more operations on the same key in one write
    ///   batch.
    /// * Globally unique. The streaming operators should ensure that different operators won't
    ///   operate on the same key. The operator operating on one keyspace should always wait for all
    ///   changes to be committed before reading and writing new keys to the engine. That is because
    ///   that the table with lower epoch might be committed after a table with higher epoch has
    ///   been committed. If such case happens, the outcome is non-predictable.
    pub async fn write_batch(
        &self,
        kv_pairs: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
    ) -> HummockResult<()> {
        let get_builder = |options: &HummockOptions| {
            TableBuilder::new(TableBuilderOptions {
                table_capacity: options.table_size,
                block_size: options.block_size,
                bloom_false_positive: options.bloom_false_positive,
                checksum_algo: options.checksum_algo,
            })
        };

        let mut table_builder = get_builder(&self.options);
        let table_id = self.unique_id.fetch_add(1, Ordering::SeqCst);
        for (k, v) in kv_pairs {
            let k = key_with_ts(k, table_id);
            table_builder.add(k.as_slice(), v);
        }

        // Producing only one table regardless of capacity for now.
        // TODO: update kv pairs to multi tables when size of the kv pairs is larger than
        // TODO: the capacity of a single table.
        let (blocks, meta) = table_builder.finish();
        let remote_dir = Some(self.options.remote_dir.as_str());
        let table =
            gen_remote_table(self.obj_client.clone(), table_id, blocks, meta, remote_dir).await?;

        self.version_manager.add_l0_sst(table).await?;
        Ok(())
    }
}

/// `assert_eq` two `Vec<u8>` with human-readable format.
#[macro_export]
macro_rules! assert_bytes_eq {
    ($left:expr, $right:expr) => {{
        use bytes::Bytes;
        assert_eq!(
            Bytes::copy_from_slice(&$left),
            Bytes::copy_from_slice(&$right)
        )
    }};
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::{HummockOptions, HummockStorage};
    use crate::object::InMemObjectStore;

    #[tokio::test]
    async fn test_basic() {
        let hummock_storage =
            HummockStorage::new(Arc::new(InMemObjectStore::new()), HummockOptions::default());
        let anchor = Bytes::from("aa");

        // First batch inserts the anchor and others.
        let mut batch1 = vec![
            (anchor.clone(), Some(Bytes::from("111"))),
            (Bytes::from("bb"), Some(Bytes::from("222"))),
        ];

        // Make sure the batch is sorted.
        batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Second batch modifies the anchor.
        let mut batch2 = vec![
            (Bytes::from("cc"), Some(Bytes::from("333"))),
            (anchor.clone(), Some(Bytes::from("111111"))),
        ];

        // Make sure the batch is sorted.
        batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Third batch deletes the anchor
        let mut batch3 = vec![
            (Bytes::from("dd"), Some(Bytes::from("444"))),
            (Bytes::from("ee"), Some(Bytes::from("555"))),
            (anchor.clone(), None),
        ];

        // Make sure the batch is sorted.
        batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Write first batch.
        hummock_storage
            .write_batch(
                batch1
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await
            .unwrap();

        // Get the value after flushing to remote.
        let value = hummock_storage.get(&anchor).await.unwrap().unwrap();
        assert_eq!(Bytes::from(value), Bytes::from("111"));

        // Test looking for a nonexistent key. `next()` would return the next key.
        let value = hummock_storage.get(&Bytes::from("ab")).await.unwrap();
        assert_eq!(value, None);

        // Write second batch.
        hummock_storage
            .write_batch(
                batch2
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await
            .unwrap();

        // Get the value after flushing to remote.
        let value = hummock_storage.get(&anchor).await.unwrap().unwrap();
        assert_eq!(Bytes::from(value), Bytes::from("111111"));

        // Write second batch.
        hummock_storage
            .write_batch(
                batch3
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await
            .unwrap();

        // Get the value after flushing to remote.
        let value = hummock_storage.get(&anchor).await.unwrap();
        assert_eq!(value, None);

        // Get non-existent maximum key.
        let value = hummock_storage.get(&Bytes::from("ff")).await.unwrap();
        assert_eq!(value, None);
    }
}
