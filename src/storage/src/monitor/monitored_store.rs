// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use futures::{Future, TryFutureExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};
use risingwave_hummock_sdk::HummockReadEpoch;
use thiserror_ext::AsReport;
use tokio::time::Instant;
use tracing::error;

#[cfg(all(not(madsim), feature = "hm-trace"))]
use super::traced_store::TracedStateStore;
use super::{MonitoredStateStoreGetStats, MonitoredStateStoreIterStats, MonitoredStorageMetrics};
use crate::error::{StorageError, StorageResult};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockStorage, SstableObjectIdManagerRef};
use crate::store::*;
/// A state store wrapper for monitoring metrics.
#[derive(Clone)]
pub struct MonitoredStateStore<S> {
    #[cfg(not(all(not(madsim), feature = "hm-trace")))]
    inner: Box<S>,

    #[cfg(all(not(madsim), feature = "hm-trace"))]
    inner: Box<TracedStateStore<S>>,

    storage_metrics: Arc<MonitoredStorageMetrics>,
}

impl<S> MonitoredStateStore<S> {
    pub fn new(inner: S, storage_metrics: Arc<MonitoredStorageMetrics>) -> Self {
        #[cfg(all(not(madsim), feature = "hm-trace"))]
        let inner = TracedStateStore::new_global(inner);
        Self {
            inner: Box::new(inner),
            storage_metrics,
        }
    }

    #[cfg(all(not(madsim), feature = "hm-trace"))]
    pub fn new_from_local(
        inner: TracedStateStore<S>,
        storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            storage_metrics,
        }
    }

    #[cfg(not(all(not(madsim), feature = "hm-trace")))]
    pub fn new_from_local(inner: S, storage_metrics: Arc<MonitoredStorageMetrics>) -> Self {
        Self {
            inner: Box::new(inner),
            storage_metrics,
        }
    }
}

/// A util function to break the type connection between two opaque return types defined by `impl`.
pub(crate) fn identity(input: impl StateStoreIterItemStream) -> impl StateStoreIterItemStream {
    input
}

pub type MonitoredStateStoreIterStream<S: StateStoreIterItemStream> = impl StateStoreIterItemStream;

// Note: it is important to define the `MonitoredStateStoreIterStream` type alias, as it marks that
// the return type of `monitored_iter` only captures the lifetime `'s` and has nothing to do with
// `'a`. If we simply use `impl StateStoreIterItemStream + 's`, the rust compiler will also capture
// the lifetime `'a` in the scope defined in the scope.
impl<S> MonitoredStateStore<S> {
    async fn monitored_iter<'a, St: StateStoreIterItemStream + 'a>(
        &'a self,
        table_id: TableId,
        iter_stream_future: impl Future<Output = StorageResult<St>> + 'a,
    ) -> StorageResult<MonitoredStateStoreIterStream<St>> {
        // start time takes iterator build time into account
        // wait for iterator creation (e.g. seek)
        let start_time = Instant::now();
        let iter_stream = iter_stream_future
            .await
            .inspect_err(|e| error!(error = %e.as_report(), "Failed in iter"))?;
        let iter_init_duration = start_time.elapsed();

        // create a monitored iterator to collect metrics
        let monitored = MonitoredStateStoreIter {
            inner: iter_stream,
            stats: MonitoredStateStoreIterStats::new(
                table_id.table_id,
                iter_init_duration,
                self.storage_metrics.clone(),
            ),
        };
        Ok(monitored.into_stream())
    }

    pub fn inner(&self) -> &S {
        #[cfg(all(not(madsim), feature = "hm-trace"))]
        {
            self.inner.inner()
        }
        #[cfg(not(all(not(madsim), feature = "hm-trace")))]
        &self.inner
    }

    async fn monitored_get(
        &self,
        get_future: impl Future<Output = StorageResult<Option<Bytes>>>,
        table_id: TableId,
        key_len: usize,
    ) -> StorageResult<Option<Bytes>> {
        use tracing::Instrument;

        let mut stats =
            MonitoredStateStoreGetStats::new(table_id.table_id, self.storage_metrics.clone());

        let value = get_future
            .verbose_instrument_await("store_get")
            .instrument(tracing::trace_span!("store_get"))
            .await
            .inspect_err(|e| error!(error = %e.as_report(), "Failed in get"))?;

        stats.get_key_size = key_len;
        if let Some(value) = value.as_ref() {
            stats.get_value_size = value.len();
        }
        stats.report();

        Ok(value)
    }
}

impl<S: StateStoreRead> StateStoreRead for MonitoredStateStore<S> {
    type IterStream = impl StateStoreReadIterStream;

    fn get(
        &self,
        key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Option<Bytes>>> + '_ {
        let table_id = read_options.table_id;
        let key_len = key.len();
        self.monitored_get(self.inner.get(key, epoch, read_options), table_id, key_len)
    }

    fn iter(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::IterStream>> + '_ {
        self.monitored_iter(
            read_options.table_id,
            self.inner.iter(key_range, epoch, read_options),
        )
        .map_ok(identity)
    }
}

impl<S: LocalStateStore> LocalStateStore for MonitoredStateStore<S> {
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;

    async fn may_exist(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<bool> {
        let table_id_label = read_options.table_id.to_string();
        let timer = self
            .storage_metrics
            .may_exist_duration
            .with_label_values(&[table_id_label.as_str()])
            .start_timer();
        let res = self
            .inner
            .may_exist(key_range, read_options)
            .verbose_instrument_await("store_may_exist")
            .await;
        timer.observe_duration();
        res
    }

    fn get(
        &self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Option<Bytes>>> + Send + '_ {
        let table_id = read_options.table_id;
        let key_len = key.len();
        // TODO: may collect the metrics as local
        self.monitored_get(self.inner.get(key, read_options), table_id, key_len)
    }

    fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::IterStream<'_>>> + Send + '_ {
        let table_id = read_options.table_id;
        // TODO: may collect the metrics as local
        self.monitored_iter(table_id, self.inner.iter(key_range, read_options))
            .map_ok(identity)
    }

    fn insert(
        &mut self,
        key: TableKey<Bytes>,
        new_val: Bytes,
        old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        // TODO: collect metrics
        self.inner.insert(key, new_val, old_val)
    }

    fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
        // TODO: collect metrics
        self.inner.delete(key, old_val)
    }

    fn flush(&mut self) -> impl Future<Output = StorageResult<usize>> + Send + '_ {
        self.inner.flush().verbose_instrument_await("store_flush")
    }

    fn epoch(&self) -> u64 {
        self.inner.epoch()
    }

    fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }

    async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
        self.inner.init(options).await
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
        // TODO: may collect metrics
        self.inner.seal_current_epoch(next_epoch, opts)
    }

    fn try_flush(&mut self) -> impl Future<Output = StorageResult<()>> + Send + '_ {
        self.inner
            .try_flush()
            .verbose_instrument_await("store_try_flush")
    }

    fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        self.inner.update_vnode_bitmap(vnodes)
    }
}

impl<S: StateStore> StateStore for MonitoredStateStore<S> {
    type Local = MonitoredStateStore<S::Local>;

    fn try_wait_epoch(
        &self,
        epoch: HummockReadEpoch,
    ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
        self.inner
            .try_wait_epoch(epoch)
            .verbose_instrument_await("store_wait_epoch")
            .inspect_err(|e| error!(error = %e.as_report(), "Failed in wait_epoch"))
    }

    async fn sync(&self, epoch: u64) -> StorageResult<SyncResult> {
        // TODO: this metrics may not be accurate if we start syncing after `seal_epoch`. We may
        // move this metrics to inside uploader
        let timer = self.storage_metrics.sync_duration.start_timer();
        let sync_result = self
            .inner
            .sync(epoch)
            .instrument_await("store_sync")
            .await
            .inspect_err(|e| error!(error = %e.as_report(), "Failed in sync"))?;
        timer.observe_duration();
        if sync_result.sync_size != 0 {
            self.storage_metrics
                .sync_size
                .observe(sync_result.sync_size as _);
        }
        Ok(sync_result)
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        self.inner.seal_epoch(epoch, is_checkpoint);
    }

    fn monitored(
        self,
        _storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> MonitoredStateStore<Self> {
        panic!("the state store is already monitored")
    }

    fn clear_shared_buffer(&self, prev_epoch: u64) -> impl Future<Output = ()> + Send + '_ {
        self.inner
            .clear_shared_buffer(prev_epoch)
            .verbose_instrument_await("store_clear_shared_buffer")
    }

    async fn new_local(&self, option: NewLocalOptions) -> Self::Local {
        MonitoredStateStore::new_from_local(
            self.inner
                .new_local(option)
                .instrument_await("store_new_local")
                .await,
            self.storage_metrics.clone(),
        )
    }

    fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
        self.inner.validate_read_epoch(epoch)
    }
}

impl MonitoredStateStore<HummockStorage> {
    pub fn sstable_store(&self) -> SstableStoreRef {
        self.inner.sstable_store()
    }

    pub fn sstable_object_id_manager(&self) -> SstableObjectIdManagerRef {
        self.inner.sstable_object_id_manager().clone()
    }
}

/// A state store iterator wrapper for monitoring metrics.
pub struct MonitoredStateStoreIter<S> {
    inner: S,
    stats: MonitoredStateStoreIterStats,
}

impl<S: StateStoreIterItemStream> MonitoredStateStoreIter<S> {
    #[try_stream(ok = StateStoreIterItem, error = StorageError)]
    async fn into_stream_inner(self) {
        let inner = self.inner;

        let mut stats = self.stats;
        futures::pin_mut!(inner);
        while let Some((key, value)) = inner
            .try_next()
            .await
            .inspect_err(|e| error!(error = %e.as_report(), "Failed in next"))?
        {
            stats.total_items += 1;
            stats.total_size += key.encoded_len() + value.len();
            yield (key, value);
        }
        drop(stats);
    }

    fn into_stream(self) -> MonitoredStateStoreIterStream<S> {
        Self::into_stream_inner(self)
    }
}
