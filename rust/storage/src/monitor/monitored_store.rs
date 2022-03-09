use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use risingwave_common::error::Result;

use super::StateStoreStats;
use crate::{StateStore, StateStoreIter};

/// A state store wrapper for monitoring metrics.
#[derive(Clone)]
pub struct MonitoredStateStore<S> {
    inner: S,

    stats: Arc<StateStoreStats>,
}

impl<S> MonitoredStateStore<S> {
    pub fn new(inner: S, stats: Arc<StateStoreStats>) -> Self {
        Self { inner, stats }
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S> MonitoredStateStore<S>
where
    S: StateStore,
{
    async fn monitored_iter<'a, I>(
        &self,
        iter: I,
    ) -> Result<<MonitoredStateStore<S> as StateStore>::Iter<'a>>
    where
        I: Future<Output = Result<S::Iter<'a>>>,
    {
        self.stats.iter_counts.inc();

        let timer = self.stats.iter_seek_latency.start_timer();
        let iter = iter.await?;
        timer.observe_duration();

        let monitored = MonitoredStateStoreIter {
            inner: iter,
            stats: self.stats.clone(),
        };
        Ok(monitored)
    }
}

#[async_trait]
impl<S> StateStore for MonitoredStateStore<S>
where
    S: StateStore,
{
    type Iter<'a> = MonitoredStateStoreIter<S::Iter<'a>>;

    async fn get(&self, key: &[u8], epoch: u64) -> Result<Option<Bytes>> {
        self.stats.get_counts.inc();

        let timer = self.stats.get_latency.start_timer();
        let value = self.inner.get(key, epoch).await?;
        timer.observe_duration();

        self.stats.get_key_size.observe(key.len() as _);
        if let Some(value) = value.as_ref() {
            self.stats.get_value_size.observe(value.len() as _);
        }

        Ok(value)
    }

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
        self.stats.range_scan_counts.inc();

        let timer = self.stats.range_scan_latency.start_timer();
        let result = self.inner.scan(key_range, limit, epoch).await?;
        timer.observe_duration();

        self.stats
            .range_scan_size
            .observe(result.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as _);

        Ok(result)
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
        self.stats.reverse_range_scan_counts.inc();

        let timer = self.stats.range_scan_latency.start_timer();
        let result = self.inner.scan(key_range, limit, epoch).await?;
        timer.observe_duration();

        self.stats
            .range_scan_size
            .observe(result.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as _);

        Ok(result)
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, epoch: u64) -> Result<()> {
        if kv_pairs.is_empty() {
            return Ok(());
        }

        self.stats.batched_write_counts.inc();
        self.stats
            .batch_write_tuple_counts
            .inc_by(kv_pairs.len() as _);

        let total_size = kv_pairs
            .iter()
            .map(|(k, v)| k.len() + v.as_ref().map(|v| v.len()).unwrap_or_default())
            .sum::<usize>();

        let timer = self.stats.batch_write_latency.start_timer();
        self.inner.ingest_batch(kv_pairs, epoch).await?;
        timer.observe_duration();

        self.stats.batch_write_size.observe(total_size as _);

        Ok(())
    }

    async fn iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.monitored_iter(self.inner.iter(key_range, epoch)).await
    }

    async fn reverse_iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.monitored_iter(self.inner.reverse_iter(key_range, epoch))
            .await
    }

    async fn wait_epoch(&self, epoch: u64) {
        self.inner.wait_epoch(epoch).await
    }

    async fn sync(&self, epoch: Option<u64>) -> Result<()> {
        self.inner.sync(epoch).await
    }

    fn monitored(self, _stats: Arc<StateStoreStats>) -> MonitoredStateStore<Self> {
        panic!("the state store is already monitored")
    }

    async fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        epoch: u64,
    ) -> Result<()> {
        self.inner.replicate_batch(kv_pairs, epoch).await
    }
}

/// A state store iterator wrapper for monitoring metrics.
pub struct MonitoredStateStoreIter<I> {
    inner: I,

    stats: Arc<StateStoreStats>,
}

#[async_trait]
impl<I> StateStoreIter for MonitoredStateStoreIter<I>
where
    I: StateStoreIter<Item = (Bytes, Bytes)>,
{
    type Item = I::Item;

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let timer = self.stats.iter_next_latency.start_timer();
        let pair = self.inner.next().await?;
        timer.observe_duration();

        if let Some((key, value)) = pair.as_ref() {
            self.stats
                .iter_next_size
                .observe((key.len() + value.len()) as _);
        }

        Ok(pair)
    }
}
