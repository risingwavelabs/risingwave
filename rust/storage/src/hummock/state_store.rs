use std::ops::RangeBounds;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::HummockStorage;
use crate::hummock::iterator::DirectedUserIterator;
use crate::{StateStore, StateStoreIter};

/// A wrapper over [`HummockStorage`] as a state store.
///
/// TODO: this wrapper introduces extra overhead of async trait, may be turned into an enum if
/// possible.
#[derive(Clone)]
pub struct HummockStateStore {
    pub storage: HummockStorage,
}

impl HummockStateStore {
    pub fn new(storage: HummockStorage) -> Self {
        Self { storage }
    }

    pub fn storage(&self) -> HummockStorage {
        self.storage.clone()
    }
}

// Note(eric): How about removing HummockStateStore and just impl StateStore for HummockStorage?
#[async_trait]
impl StateStore for HummockStateStore {
    type Iter<'a> = HummockStateStoreIter<'a>;

    async fn get(&self, key: &[u8], epoch: u64) -> Result<Option<Bytes>> {
        let value = self.storage.get(key, epoch).await?;
        let value = value.map(Bytes::from);
        Ok(value)
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, epoch: u64) -> Result<()> {
        self.storage
            .write_batch(
                kv_pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
                epoch,
            )
            .await?;

        Ok(())
    }

    async fn iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        let timer = self.storage.stats.iter_seek_latency.start_timer();
        let inner = self.storage.range_scan(key_range, epoch).await?;
        self.storage.stats.iter_counts.inc();
        let mut res = DirectedUserIterator::Forward(inner);
        res.rewind().await?;
        timer.observe_duration();
        Ok(HummockStateStoreIter(res))
    }

    async fn reverse_iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        let mut res = self.storage.reverse_range_scan(key_range, epoch).await?;
        res.rewind().await?;
        Ok(HummockStateStoreIter(DirectedUserIterator::Backward(res)))
    }

    async fn wait_epoch(&self, epoch: u64) {
        self.storage.wait_epoch(epoch).await;
    }
}

pub struct HummockStateStoreIter<'a>(DirectedUserIterator<'a>);

#[async_trait]
impl<'a> StateStoreIter for HummockStateStoreIter<'a> {
    // TODO: directly return `&[u8]` to user instead of `Bytes`.
    type Item = (Bytes, Bytes);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let iter = &mut self.0;

        if iter.is_valid() {
            let kv = (
                Bytes::copy_from_slice(iter.key()),
                Bytes::copy_from_slice(iter.value()),
            );
            iter.next().await?;
            Ok(Some(kv))
        } else {
            Ok(None)
        }
    }
}
