// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use bytes::Bytes;
use futures::{Future, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockReadEpoch;
use tracing::error;

use super::StateStoreMetrics;
use crate::error::{StorageError, StorageResult};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockStorage, SstableIdManagerRef};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{
    define_state_store_associated_type, define_state_store_read_associated_type,
    define_state_store_write_associated_type,
};

/// A state store wrapper for monitoring metrics.
#[derive(Clone)]
pub struct MonitoredStateStore<S> {
    inner: Box<S>,

    stats: Arc<StateStoreMetrics>,
}

impl<S> MonitoredStateStore<S> {
    pub fn new(inner: S, stats: Arc<StateStoreMetrics>) -> Self {
        Self {
            inner: Box::new(inner),
            stats,
        }
    }
}

pub type MonitoredStateStoreIterStream<S: StateStoreRead> = impl StateStoreReadIterStream;
impl<S: StateStoreRead> MonitoredStateStore<S> {
    async fn monitored_iter(
        &self,
        iter_stream_future: impl Future<Output = StorageResult<S::IterStream>>,
    ) -> StorageResult<MonitoredStateStoreIterStream<S>> {
        // start time takes iterator build time into account
        let start_time = minstant::Instant::now();

        // wait for iterator creation (e.g. seek)
        let iter_stream = iter_stream_future
            .verbose_stack_trace("store_create_iter")
            .await
            .inspect_err(|e| error!("Failed in iter: {:?}", e))?;

        // statistics of iter in process count to estimate the read ops in the same time
        self.stats.iter_in_process_counts.inc();

        // create a monitored iterator to collect metrics
        let monitored = MonitoredStateStoreIter {
            inner: iter_stream,
            stats: MonitoredStateStoreIterStats {
                total_items: 0,
                total_size: 0,
                start_time,
                scan_time: minstant::Instant::now(),
                stats: self.stats.clone(),
            },
        };
        Ok(monitored.into_stream())
    }

    pub fn stats(&self) -> Arc<StateStoreMetrics> {
        self.stats.clone()
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: StateStoreRead> StateStoreRead for MonitoredStateStore<S> {
    type IterStream = MonitoredStateStoreIterStream<S>;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move {
            let timer = self.stats.get_duration.start_timer();
            let value = self
                .inner
                .get(key, epoch, read_options)
                .verbose_stack_trace("store_get")
                .await
                .inspect_err(|e| error!("Failed in get: {:?}", e))?;
            timer.observe_duration();

            self.stats.get_key_size.observe(key.len() as _);
            if let Some(value) = value.as_ref() {
                self.stats.get_value_size.observe(value.len() as _);
            }

            Ok(value)
        }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        self.monitored_iter(self.inner.iter(key_range, epoch, read_options))
    }
}

impl<S: StateStoreWrite> StateStoreWrite for MonitoredStateStore<S> {
    define_state_store_write_associated_type!();

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            self.stats
                .write_batch_tuple_counts
                .inc_by(kv_pairs.len() as _);
            let timer = self.stats.write_batch_duration.start_timer();
            let batch_size = self
                .inner
                .ingest_batch(kv_pairs, delete_ranges, write_options)
                .verbose_stack_trace("store_ingest_batch")
                .await
                .inspect_err(|e| error!("Failed in ingest_batch: {:?}", e))?;
            timer.observe_duration();

            self.stats.write_batch_size.observe(batch_size as _);
            Ok(batch_size)
        }
    }
}

impl<S: LocalStateStore> LocalStateStore for MonitoredStateStore<S> {}

impl<S: StateStore> StateStore for MonitoredStateStore<S> {
    type Local = MonitoredStateStore<S::Local>;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move {
            self.inner
                .try_wait_epoch(epoch)
                .verbose_stack_trace("store_wait_epoch")
                .await
                .inspect_err(|e| error!("Failed in wait_epoch: {:?}", e))
        }
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            // TODO: this metrics may not be accurate if we start syncing after `seal_epoch`. We may
            // move this metrics to inside uploader
            let timer = self.stats.shared_buffer_to_l0_duration.start_timer();
            let sync_result = self
                .inner
                .sync(epoch)
                .verbose_stack_trace("store_await_sync")
                .await
                .inspect_err(|e| error!("Failed in sync: {:?}", e))?;
            timer.observe_duration();
            if sync_result.sync_size != 0 {
                self.stats
                    .write_l0_size_per_epoch
                    .observe(sync_result.sync_size as _);
            }
            Ok(sync_result)
        }
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        self.inner.seal_epoch(epoch, is_checkpoint);
    }

    fn monitored(self, _stats: Arc<StateStoreMetrics>) -> MonitoredStateStore<Self> {
        panic!("the state store is already monitored")
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move {
            self.inner
                .clear_shared_buffer()
                .verbose_stack_trace("store_clear_shared_buffer")
                .await
                .inspect_err(|e| error!("Failed in clear_shared_buffer: {:?}", e))
        }
    }

    fn new_local(&self, table_id: TableId) -> Self::NewLocalFuture<'_> {
        async move { MonitoredStateStore::new(self.inner.new_local(table_id).await, self.stats.clone()) }
    }
}

impl MonitoredStateStore<HummockStorage> {
    pub fn sstable_store(&self) -> SstableStoreRef {
        self.inner.sstable_store()
    }

    pub fn sstable_id_manager(&self) -> SstableIdManagerRef {
        self.inner.sstable_id_manager().clone()
    }
}

/// A state store iterator wrapper for monitoring metrics.
pub struct MonitoredStateStoreIter<S> {
    inner: S,
    stats: MonitoredStateStoreIterStats,
}

struct MonitoredStateStoreIterStats {
    total_items: usize,
    total_size: usize,
    start_time: minstant::Instant,
    scan_time: minstant::Instant,
    stats: Arc<StateStoreMetrics>,
}

impl<S: StateStoreIterItemStream> MonitoredStateStoreIter<S> {
    #[try_stream(ok = StateStoreIterItem, error = StorageError)]
    async fn into_stream_inner(mut self) {
        let inner = self.inner;
        futures::pin_mut!(inner);
        while let Some((key, value)) = inner
            .try_next()
            .await
            .inspect_err(|e| error!("Failed in next: {:?}", e))?
        {
            self.stats.total_items += 1;
            self.stats.total_size += key.encoded_len() + value.len();
            yield (key, value);
        }
    }

    fn into_stream(self) -> impl StateStoreIterItemStream {
        Self::into_stream_inner(self)
    }
}

impl Drop for MonitoredStateStoreIterStats {
    fn drop(&mut self) {
        self.stats
            .iter_duration
            .observe(self.start_time.elapsed().as_secs_f64());
        self.stats
            .iter_scan_duration
            .observe(self.scan_time.elapsed().as_secs_f64());
        self.stats.iter_item.observe(self.total_items as f64);
        self.stats.iter_size.observe(self.total_size as f64);
    }
}
