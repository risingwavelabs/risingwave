// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use futures::Future;

use super::StateStoreMetrics;
use crate::error::StorageResult;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

/// A state store wrapper for monitoring metrics.
#[derive(Clone)]
pub struct MonitoredStateStore<S> {
    inner: S,

    stats: Arc<StateStoreMetrics>,
}

impl<S> MonitoredStateStore<S> {
    pub fn new(inner: S, stats: Arc<StateStoreMetrics>) -> Self {
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
    ) -> StorageResult<<MonitoredStateStore<S> as StateStore>::Iter<'a>>
    where
        I: Future<Output = StorageResult<S::Iter<'a>>>,
    {
        let iter = iter.await?;

        let monitored = MonitoredStateStoreIter { inner: iter };
        Ok(monitored)
    }

    pub fn stats(&self) -> Arc<StateStoreMetrics> {
        self.stats.clone()
    }
}

impl<S> StateStore for MonitoredStateStore<S>
where
    S: StateStore,
{
    type Iter<'a> = MonitoredStateStoreIter<S::Iter<'a>> where Self: 'a;
    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'_> {
        async move {
            let timer = self.stats.get_duration.start_timer();
            let value = self.inner.get(key, epoch).await?;
            timer.observe_duration();

            self.stats.get_key_size.observe(key.len() as _);
            if let Some(value) = value.as_ref() {
                self.stats.get_value_size.observe(value.len() as _);
            }

            Ok(value)
        }
    }

    fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let timer = self.stats.range_scan_duration.start_timer();
            let result = self.inner.scan(key_range, limit, epoch).await?;
            timer.observe_duration();

            self.stats
                .range_scan_size
                .observe(result.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as _);

            Ok(result)
        }
    }

    fn reverse_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ReverseScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let timer = self.stats.range_reverse_scan_duration.start_timer();
            let result = self.inner.scan(key_range, limit, epoch).await?;
            timer.observe_duration();

            self.stats
                .range_reverse_scan_size
                .observe(result.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as _);

            Ok(result)
        }
    }

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            if kv_pairs.is_empty() {
                return Ok(());
            }

            self.stats
                .write_batch_tuple_counts
                .inc_by(kv_pairs.len() as _);

            let total_size = kv_pairs
                .iter()
                .map(|(k, v)| k.len() + v.size())
                .sum::<usize>();

            let timer = self.stats.write_batch_duration.start_timer();
            self.inner.ingest_batch(kv_pairs, epoch).await?;
            timer.observe_duration();

            self.stats.write_batch_size.observe(total_size as _);

            Ok(())
        }
    }

    fn iter<R, B>(&self, key_range: R, epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { self.monitored_iter(self.inner.iter(key_range, epoch)).await }
    }

    fn reverse_iter<R, B>(&self, key_range: R, epoch: u64) -> Self::ReverseIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            self.monitored_iter(self.inner.reverse_iter(key_range, epoch))
                .await
        }
    }

    fn wait_epoch(&self, epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { self.inner.wait_epoch(epoch).await }
    }

    fn sync(&self, epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move {
            let timer = self.stats.shared_buffer_to_l0_duration.start_timer();
            self.inner.sync(epoch).await?;
            timer.observe_duration();
            Ok(())
        }
    }

    fn monitored(self, _stats: Arc<StateStoreMetrics>) -> MonitoredStateStore<Self> {
        panic!("the state store is already monitored")
    }

    fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move { self.inner.replicate_batch(kv_pairs, epoch).await }
    }
}

/// A state store iterator wrapper for monitoring metrics.
pub struct MonitoredStateStoreIter<I> {
    inner: I,
}

impl<I> StateStoreIter for MonitoredStateStoreIter<I>
where
    I: StateStoreIter<Item = (Bytes, Bytes)>,
{
    type Item = (Bytes, Bytes);
    type NextFuture<'a> = impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> where Self: 'a;
    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            let pair = self.inner.next().await?;

            Ok(pair)
        }
    }
}
