use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::StateStoreStats;
use crate::{StateStore, StateStoreIter};

#[derive(Clone)]
pub struct MonitoredStateStore<S> {
    inner: S,

    stats: Arc<StateStoreStats>,
}

impl<S> MonitoredStateStore<S> {
    pub fn new(inner: S, stats: Arc<StateStoreStats>) -> Self {
        Self { inner, stats }
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
        self.stats.iter_counts.inc();

        let timer = self.stats.iter_seek_latency.start_timer();
        let iter = self.inner.iter(key_range, epoch).await?;
        timer.observe_duration();

        let monitored = MonitoredStateStoreIter {
            inner: iter,
            stats: self.stats.clone(),
        };
        Ok(monitored)
    }
}

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
