use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use crate::monitor::{MonitoredStateStore, StateStoreStats};
use crate::write_batch::WriteBatch;

#[async_trait]
pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter<'a>: StateStoreIter<Item = (Bytes, Bytes)>;

    /// Point get a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    async fn get(&self, key: &[u8], epoch: u64) -> Result<Option<Bytes>>;

    /// Scan `limit` number of keys from a key range. If `limit` is `None`, scan all elements.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    ///
    /// By default, this simply calls `StateStore::iter` to fetch elements.
    ///
    /// TODO: in some cases, the scan can be optimized into a `multi_get` request.
    async fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        collect_from_iter(self.iter(key_range, epoch).await?, limit).await
    }

    async fn reverse_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        collect_from_iter(self.reverse_iter(key_range, epoch).await?, limit).await
    }

    /// Ingest a batch of data into the state store. One write batch should never contain operation
    /// on the same key. e.g. Put(233, x) then Delete(233).
    /// A epoch should be provided to ingest a write batch. It is served as:
    /// - A handle to represent an atomic write session. All ingested write batches associated with
    ///   the same `Epoch` have the all-or-nothing semantics, meaning that partial changes are not
    ///   queryable and will be rollbacked if instructed.
    /// - A version of a kv pair. kv pair associated with larger `Epoch` is guaranteed to be newer
    ///   then kv pair with smaller `Epoch`. Currently this version is only used to derive the
    ///   per-key modification history (e.g. in compaction), not across different keys.
    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, epoch: u64) -> Result<()>;

    /// Functions the same as `ingest_batch`, except that data won't be persisted.
    async fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        epoch: u64,
    ) -> Result<()>;

    /// Open and return an iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`.
    async fn iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>;

    /// Open and return a reversed iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`
    async fn reverse_iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>;

    /// Create a `WriteBatch` associated with this state store.
    fn start_write_batch(&self) -> WriteBatch<Self> {
        WriteBatch::new(self.clone())
    }

    /// Wait until the epoch is committed and its data is ready to read.
    async fn wait_epoch(&self, epoch: u64);

    /// Sync buffered data to S3.
    /// If epoch is None, all buffered data will be synced.
    /// Otherwise, only data of the provided epoch will be synced.
    async fn sync(&self, epoch: Option<u64>) -> Result<()>;

    /// Create a [`MonitoredStateStore`] from this state store, with given `stats`.
    fn monitored(self, stats: Arc<StateStoreStats>) -> MonitoredStateStore<Self> {
        MonitoredStateStore::new(self, stats)
    }
}

#[async_trait]
pub trait StateStoreIter: Send {
    type Item;

    async fn next(&mut self) -> Result<Option<Self::Item>>;
}

async fn collect_from_iter<'a, I>(mut iter: I, limit: Option<usize>) -> Result<Vec<I::Item>>
where
    I: StateStoreIter,
{
    let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

    for _ in 0..limit.unwrap_or(usize::MAX) {
        match iter.next().await? {
            Some(kv) => kvs.push(kv),
            None => break,
        }
    }

    Ok(kvs)
}
