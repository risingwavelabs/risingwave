//! Hummock is the state store of the streaming system.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::{Result, ToRwResult};

use crate::hummock::iterator::HummockIterator;

mod table;
pub use table::*;
mod cloud;
mod compactor;
mod error;
mod iterator;
mod key;
mod key_range;
mod level_handler;
mod value;
mod version_cmp;
mod version_manager;

use cloud::gen_remote_table;
use compactor::Compactor;
pub use error::*;
use parking_lot::Mutex as PLMutex;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use tokio::select;
use tokio::sync::mpsc;
use value::*;
use version_manager::VersionManager;

use self::iterator::{BoxedHummockIterator, SortedIterator, UserKeyIterator};
use self::key::{key_with_ts, user_key, FullKey};
use crate::object::ObjectStore;
use crate::{StateStore, StateStoreIter};

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

    /// Notify the compactor to compact after every write_batch().
    tx: mpsc::UnboundedSender<()>,

    /// Receiver of the compactor.
    rx: Arc<PLMutex<Option<mpsc::UnboundedReceiver<()>>>>,
}

impl HummockStorage {
    pub fn new(obj_client: Arc<dyn ObjectStore>, options: HummockOptions) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            options: Arc::new(options),
            unique_id: Arc::new(AtomicU64::new(0)),
            version_manager: Arc::new(VersionManager::new()),
            obj_client,
            tx,
            rx: Arc::new(PLMutex::new(Some(rx))),
        }
    }

    /// Get the latest value of a specified `key`.
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get(&self, key: &[u8]) -> HummockResult<Option<Vec<u8>>> {
        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();

        for table in &self.version_manager.tables().unwrap() {
            // bloom filter tells us the key could possibly exist, go get it
            if !table.surely_not_have(&key_with_ts(key.to_vec(), u64::MAX)) {
                let iter = Box::new(TableIterator::new(table.clone()));
                table_iters.push(iter);
            }
        }

        let mut it = SortedIterator::new(table_iters);

        // Use `SortedIterator` to seek for they key with latest version to
        // get the latest key.
        it.seek(&key_with_ts(key.to_vec(), u64::MAX)).await?;

        // Iterator has seeked passed the borders.
        if !it.is_valid() {
            return Ok(None);
        }

        // Iterator gets us the key, we tell if it's the key we want
        // or key next to it.
        match user_key(it.key()) == key {
            true => Ok(it.value().into_put_value().map(|x| x.to_vec())),
            false => Ok(None),
        }
    }

    /// Return an iterator that scan from the begin key to the end key
    pub async fn range_scan(
        &self,
        begin_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> HummockResult<UserKeyIterator> {
        if begin_key.is_some() && end_key.is_some() {
            assert!(begin_key.clone().unwrap() <= end_key.clone().unwrap());
        }

        let begin_key_copy = match &begin_key {
            Some(begin_key) => key_with_ts(begin_key.clone(), u64::MAX),
            None => Vec::new(),
        };
        let begin_fk = FullKey::from_slice(begin_key_copy.as_slice());

        let end_key_copy = match &end_key {
            Some(end_key) => key_with_ts(end_key.clone(), u64::MIN),
            None => Vec::new(),
        };
        let end_fk = FullKey::from_slice(end_key_copy.as_slice());

        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();
        for table in self.version_manager.tables()? {
            let tlk = FullKey::from_slice(table.meta.largest_key.as_slice());
            let table_too_left = begin_key.is_some() && tlk < begin_fk;

            let tsk = FullKey::from_slice(table.meta.smallest_key.as_slice());
            let table_too_right = end_key.is_some() && tsk > end_fk;

            // decide whether the two ranges have common sub-range
            if !(table_too_left || table_too_right) {
                let iter = Box::new(TableIterator::new(table.clone()));
                table_iters.push(iter);
            }
        }

        let si = SortedIterator::new(table_iters);
        Ok(UserKeyIterator::new(si, begin_key, end_key))
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
            // do not allow empty key
            assert!(!k.is_empty());

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
        // TODO: should we use unwrap() ?
        self.tx.send(()).unwrap();

        Ok(())
    }

    fn get_builder(options: &HummockOptions) -> TableBuilder {
        // TODO: avoid repeating code in write_batch()
        // TODO: use different option values (especially table_size) for compaction
        TableBuilder::new(TableBuilderOptions {
            table_capacity: options.table_size,
            block_size: options.block_size,
            bloom_false_positive: options.bloom_false_positive,
            checksum_algo: options.checksum_algo,
        })
    }
    pub async fn start_compactor(
        self: &Arc<Self>,
        mut stop: mpsc::UnboundedReceiver<()>,
    ) -> HummockResult<()> {
        let mut compact_notifier = self.rx.lock().take().unwrap();
        loop {
            select! {
                Some(_) = compact_notifier.recv() => Compactor::compact_tasking(self).await?,
                Some(_) = stop.recv() => break
            }
        }
        Ok(())
    }
}

/// A wrapper over [`HummockStorage`] as a state store.
///
/// TODO: this wrapper introduces extra overhead of async trait, may be turned into an enum if
/// possible.
#[derive(Clone)]
pub struct HummockStateStore {
    storage: HummockStorage,
}

// Note(eric): How about removing HummockStateStore and just impl StateStore for HummockStorage?
#[async_trait]
impl StateStore for HummockStateStore {
    type Iter = HummockStateStoreIter;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.storage
            .get(key)
            .await
            .map(|x| x.map(Bytes::from))
            // TODO: make the HummockError into an I/O Error.
            .map_err(anyhow::Error::new)
            .to_rw_result()
    }

    async fn scan(&self, _prefix: &[u8], _limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        todo!()
    }

    async fn ingest_batch(&self, mut kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        // TODO: reduce the redundant vec clone
        kv_pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        self.storage
            .write_batch(
                kv_pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await
            .map_err(anyhow::Error::new)
            .to_rw_result()
    }

    fn iter(&self, _prefix: &[u8]) -> Self::Iter {
        todo!()
    }
}

pub struct HummockStateStoreIter {}

#[async_trait]
impl StateStoreIter for HummockStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn open(&mut self) -> Result<()> {
        todo!()
    }

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        todo!()
    }
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
