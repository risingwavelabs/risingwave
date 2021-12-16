//! Hummock is the state store of the streaming system.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::hummock::iterator::HummockIterator;
use crate::hummock::key::FullKey;
use crate::hummock::table::format::user_key;

mod table;
pub use table::*;
mod cloud;
mod error;
mod iterator;
mod key_range;
mod level_handler;
mod value;
mod version_manager;
use cloud::gen_remote_table;
pub use error::*;
use parking_lot::Mutex as PLMutex;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use tokio::select;
use tokio::sync::mpsc;
use value::*;
mod key;
use version_manager::{CompactTask, Level, LevelEntry, VersionManager};

use self::iterator::{BoxedHummockIterator, SortedIterator, UserKeyIterator};
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
            if !table.surely_not_have(key) {
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
    pub async fn range_scan(&self, begin: Vec<u8>, end: Vec<u8>) -> HummockResult<UserKeyIterator> {
        if begin > end {
            return Err(HummockError::DecodeError("invalid range".to_string()));
        }

        // TODO: not a good implementation, consider change the type of smallest_key and largest_key
        // in table meta
        let begin_fk_clone = key_with_ts(begin.clone(), u64::MAX);
        let begin_fk = FullKey::from_slice(begin_fk_clone.as_slice());
        let end_fk_clone = key_with_ts(end.clone(), u64::MIN);
        let end_fk = FullKey::from_slice(end_fk_clone.as_slice());

        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();
        for table in self.version_manager.tables()? {
            // TODO: change the type of smallest_key and largest_key
            let tsk = FullKey::from_slice(table.meta.smallest_key.as_slice());
            let tlk = FullKey::from_slice(table.meta.largest_key.as_slice());

            // decide whether the two ranges have common sub-range
            if !(tsk > end_fk || tlk < begin_fk) {
                let iter = Box::new(TableIterator::new(table.clone()));
                table_iters.push(iter);
            }
        }

        let si = SortedIterator::new(table_iters);
        Ok(UserKeyIterator::new(si, Some((begin, end))))
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

    async fn run_compact(self: &Arc<Self>, compact_task: &mut CompactTask) -> HummockResult<()> {
        let mut iters = vec![];
        for LevelEntry { level, .. } in &compact_task.input_ssts {
            match level {
                Level::Tiering(input_sst_ids) => {
                    let tables = self.version_manager.pick_few_tables(input_sst_ids)?;
                    iters.extend(tables.into_iter().map(
                        |table| -> Box<dyn HummockIterator + Send> {
                            Box::new(TableIterator::new(table))
                        },
                    ));
                }
                Level::Leveling(_) => {
                    unimplemented!();
                }
            }
        }
        todo!();
        // let mut iter = SortedIterator::new(iters);
        //
        // compact_task
        // .sorted_output_ssts
        // .reserve(compact_task.splits.len());
        //
        // TODO: avoid repeating code in write_batch()
        // TODO: use different option values (especially table_size) for compaction
        // let get_builder = |options: &HummockOptions| {
        // TableBuilder::new(TableBuilderOptions {
        // table_capacity: options.table_size,
        // block_size: options.block_size,
        // bloom_false_positive: options.bloom_false_positive,
        // })
        // };
        //
        // TODO: we can speed up by parallelling compaction (each with different kr)
        // let mut skip_key = BytesMut::new();
        // for kr in &compact_task.splits {
        // TODO: purge tombstone if possible
        // NOTICE: should be user_key overlap, NOT full_key overlap!
        // let _has_overlap = true;
        //
        // if !kr.left.is_empty() {
        // iter.seek(&kr.left).await?;
        // } else {
        // iter.rewind().await?;
        // }
        //
        // skip_key.clear();
        // let mut last_key = BytesMut::new();
        // let mut _num_versions = 0;
        //
        // let mut ky;
        // let mut val;
        // let mut is_valid;
        //
        // is_valid = if let Ok(Some((tmp_ky, tmp_val))) = iter.next().await {
        // ky = tmp_ky;
        // val = tmp_val;
        // true
        // } else {
        // ky = b"";
        // val = HummockValue::Delete;
        // false
        // };
        //
        // while is_valid {
        // if !kr.right.is_empty()
        // && VersionComparator::compare_key(ky, &kr.right) != std::cmp::Ordering::Less
        // {
        // break;
        // }
        //
        // let mut table_builder = get_builder(&self.options);
        //
        // while is_valid {
        // let iter_key = Bytes::copy_from_slice(ky);
        //
        // if !skip_key.is_empty() {
        // if VersionComparator::same_user_key(&iter_key, &skip_key) {
        // is_valid = if let Ok(Some((tmp_ky, tmp_val))) = iter.next().await {
        // ky = tmp_ky;
        // val = tmp_val;
        // true
        // } else {
        // ky = b"";
        // val = HummockValue::Delete;
        // false
        // };
        // continue;
        // } else {
        // skip_key.clear();
        // }
        // }
        //
        // if !VersionComparator::same_user_key(&iter_key, &last_key) {
        // if !kr.right.is_empty()
        // && VersionComparator::compare_key(&iter_key, &kr.right) != std::cmp::Ordering::Less
        // {
        // break;
        // }
        //
        // if table_builder.reach_capacity() {
        // break;
        // }
        //
        // last_key.clear();
        // last_key.extend_from_slice(ky);
        //
        // _num_versions = 0;
        // }
        //
        // TODO: discard_ts logic
        //
        // table_builder.add(
        // &iter_key,
        // match val {
        // HummockValue::Put(slice_val) => HummockValue::Put(Vec::from(slice_val)),
        // HummockValue::Delete => HummockValue::Delete,
        // },
        // );
        //
        // is_valid = if let Ok(Some((tmp_ky, tmp_val))) = iter.next().await {
        // ky = tmp_ky;
        // val = tmp_val;
        // true
        // } else {
        // ky = b"";
        // val = HummockValue::Delete;
        // false
        // };
        // }
        //
        // if table_builder.is_empty() {
        // continue;
        // }
        //
        // TODO: avoid repeating code in write_batch()
        // let (blocks, meta) = table_builder.finish();
        // let table_id = self.unique_id.fetch_add(1, Ordering::SeqCst);
        // let remote_dir = Some(self.options.remote_dir.as_str());
        // let table =
        // gen_remote_table(self.obj_client.clone(), table_id, blocks, meta, remote_dir).await?;
        //
        // compact_task.sorted_output_ssts.push(table);
        // }
        // }
        // Ok(())
    }

    async fn compact_tasking(self: &Arc<Self>) -> HummockResult<()> {
        let mut compact_task = match self.version_manager.get_compact_task().await {
            Ok(task) => task,
            Err(HummockError::OK) => {
                return Ok(());
            }
            Err(err) => {
                return Err(err);
            }
        };

        compact_task.result = self.run_compact(&mut compact_task).await;
        if compact_task.result.is_err() {
            for _sst_to_delete in &compact_task.sorted_output_ssts {
                // TODO: delete these tables in (S3) storage
                // However, if we request a table_id from hummock storage service every time we
                // generate a table, we would not delete here, or we should notify
                // hummock storage service to delete them.
            }
            compact_task.sorted_output_ssts.clear();
        }
        let is_task_ok = compact_task.result.is_ok();

        self.version_manager.report_compact_task(compact_task).await;

        if is_task_ok {
            Ok(())
        } else {
            Err(HummockError::ObjectIoError(String::from(
                "compaction failed.",
            )))
        }
    }

    async fn start_compactor(
        self: &Arc<Self>,
        mut stop: mpsc::UnboundedReceiver<()>,
    ) -> HummockResult<()> {
        let mut compact_notifier = self.rx.lock().take().unwrap();
        loop {
            select! {
                Some(_) = compact_notifier.recv() => self.compact_tasking().await?,
                Some(_) = stop.recv() => break
            }
        }
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
