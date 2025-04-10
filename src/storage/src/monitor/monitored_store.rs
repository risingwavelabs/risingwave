// Copyright 2025 RisingWave Labs
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

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{Future, FutureExt, TryFutureExt};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch, SyncResult};
use thiserror_ext::AsReport;
use tokio::time::Instant;
use tracing::{Instrument, error};

use super::{MonitoredStateStoreGetStats, MonitoredStateStoreIterStats, MonitoredStorageMetrics};
use crate::error::StorageResult;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockStorage, SstableObjectIdManagerRef};
use crate::monitor::monitored_storage_metrics::StateStoreIterStats;
use crate::monitor::{StateStoreIterLogStats, StateStoreIterStatsTrait};
use crate::store::*;
use crate::store_impl::AsHummock;

/// A state store wrapper for monitoring metrics.
#[derive(Clone)]
pub struct MonitoredStateStore<S, E = ()> {
    inner: Box<S>,
    storage_metrics: Arc<MonitoredStorageMetrics>,
    extra: E,
}

type MonitoredTableStateStore<S> = MonitoredStateStore<S, TableId>;

impl<S> MonitoredStateStore<S> {
    pub fn new(inner: S, storage_metrics: Arc<MonitoredStorageMetrics>) -> Self {
        Self {
            inner: Box::new(inner),
            storage_metrics,
            extra: (),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S> MonitoredTableStateStore<S> {
    fn new(inner: S, storage_metrics: Arc<MonitoredStorageMetrics>, table_id: TableId) -> Self {
        Self {
            inner: Box::new(inner),
            storage_metrics,
            extra: table_id,
        }
    }

    fn table_id(&self) -> TableId {
        self.extra
    }
}

// Note: it is important to define the `MonitoredStateStoreIter` type alias, as it marks that
// the return type of `monitored_iter` only captures the lifetime `'s` and has nothing to do with
// `'a`. If we simply use `impl StateStoreIter + 's`, the rust compiler will also capture
// the lifetime `'a` in the scope defined in the scope.
impl<S, E> MonitoredStateStore<S, E> {
    async fn monitored_iter<
        'a,
        Item: IterItem,
        I: StateStoreIter<Item> + 'a,
        Stat: StateStoreIterStatsTrait<Item = Item>,
    >(
        &'a self,
        table_id: TableId,
        iter_stream_future: impl Future<Output = StorageResult<I>> + 'a,
    ) -> StorageResult<MonitoredStateStoreIter<Item, I, Stat>> {
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
            stats: MonitoredStateStoreIterStats {
                inner: Stat::new(table_id.table_id, &self.storage_metrics, iter_init_duration),
                table_id: table_id.table_id,
                metrics: self.storage_metrics.clone(),
            },
            _phantom: PhantomData,
        };
        Ok(monitored)
    }

    async fn monitored_on_key_value<O>(
        &self,
        on_key_value_future: impl Future<Output = StorageResult<Option<(O, usize)>>>,
        table_id: TableId,
        key_len: usize,
    ) -> StorageResult<Option<O>> {
        let mut stats =
            MonitoredStateStoreGetStats::new(table_id.table_id, self.storage_metrics.clone());

        let value = on_key_value_future
            .instrument_await("store_on_key_value".verbose())
            .instrument(tracing::trace_span!("store_on_key_value"))
            .await
            .inspect_err(|e| error!(error = %e.as_report(), "Failed in get"))?;

        stats.get_key_size = key_len;
        let value = value.map(|(value, value_len)| {
            stats.get_value_size = value_len;
            value
        });
        stats.report();

        Ok(value)
    }
}

impl<S: StateStoreGet> StateStoreGet for MonitoredTableStateStore<S> {
    fn on_key_value<O: Send + 'static>(
        &self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
        on_key_value_fn: impl KeyValueFn<O>,
    ) -> impl StorageFuture<'_, Option<O>> {
        let table_id = self.table_id();
        let key_len = key.len();
        self.monitored_on_key_value(
            self.inner
                .on_key_value(key, read_options, move |key, value| {
                    let result = on_key_value_fn(key, value);
                    result.map(|output| (output, value.len()))
                }),
            table_id,
            key_len,
        )
    }
}

impl<S: StateStoreRead> StateStoreRead for MonitoredTableStateStore<S> {
    type Iter = impl StateStoreReadIter;
    type RevIter = impl StateStoreReadIter;

    fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
        self.monitored_iter::<'_, _, _, StateStoreIterStats>(
            self.table_id(),
            self.inner.iter(key_range, read_options),
        )
    }

    fn rev_iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::RevIter>> + '_ {
        self.monitored_iter::<'_, _, _, StateStoreIterStats>(
            self.table_id(),
            self.inner.rev_iter(key_range, read_options),
        )
    }
}

impl<S: StateStoreReadLog> StateStoreReadLog for MonitoredStateStore<S> {
    type ChangeLogIter = impl StateStoreReadChangeLogIter;

    fn next_epoch(&self, epoch: u64, options: NextEpochOptions) -> impl StorageFuture<'_, u64> {
        self.inner.next_epoch(epoch, options)
    }

    fn iter_log(
        &self,
        epoch_range: (u64, u64),
        key_range: TableKeyRange,
        options: ReadLogOptions,
    ) -> impl Future<Output = StorageResult<Self::ChangeLogIter>> + Send + '_ {
        self.monitored_iter::<'_, _, _, StateStoreIterLogStats>(
            options.table_id,
            self.inner.iter_log(epoch_range, key_range, options),
        )
    }
}

impl<S: StateStoreReadVector> StateStoreReadVector for MonitoredTableStateStore<S> {
    fn nearest<O: Send + 'static>(
        &self,
        vec: Vector,
        options: VectorNearestOptions,
        on_nearest_item_fn: impl OnNearestItem<O>,
    ) -> impl StorageFuture<'_, Vec<O>> {
        // TODO: monitor
        self.inner.nearest(vec, options, on_nearest_item_fn)
    }
}

impl<S: LocalStateStore> LocalStateStore for MonitoredTableStateStore<S> {
    type FlushedSnapshotReader = MonitoredTableStateStore<S::FlushedSnapshotReader>;

    type Iter<'a> = impl StateStoreIter + 'a;
    type RevIter<'a> = impl StateStoreIter + 'a;

    fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::Iter<'_>>> + Send + '_ {
        self.monitored_iter::<'_, _, _, StateStoreIterStats>(
            self.table_id(),
            self.inner.iter(key_range, read_options),
        )
    }

    fn rev_iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::RevIter<'_>>> + Send + '_ {
        self.monitored_iter::<'_, _, _, StateStoreIterStats>(
            self.table_id(),
            self.inner.rev_iter(key_range, read_options),
        )
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

    fn get_table_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
        self.inner.get_table_watermark(vnode)
    }

    fn new_flushed_snapshot_reader(&self) -> Self::FlushedSnapshotReader {
        MonitoredTableStateStore::new(
            self.inner.new_flushed_snapshot_reader(),
            self.storage_metrics.clone(),
            self.table_id(),
        )
    }

    async fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> StorageResult<Arc<Bitmap>> {
        self.inner.update_vnode_bitmap(vnodes).await
    }
}

impl<S: StateStoreWriteEpochControl> StateStoreWriteEpochControl for MonitoredTableStateStore<S> {
    fn flush(&mut self) -> impl Future<Output = StorageResult<usize>> + Send + '_ {
        self.inner.flush().instrument_await("store_flush".verbose())
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
            .instrument_await("store_try_flush".verbose())
    }
}

impl<S: StateStoreWriteVector> StateStoreWriteVector for MonitoredTableStateStore<S> {
    fn insert(&mut self, vec: Vector, info: Bytes) -> StorageResult<()> {
        // TODO: monitor
        self.inner.insert(vec, info)
    }
}

impl<S: StateStore> StateStore for MonitoredStateStore<S> {
    type Local = MonitoredTableStateStore<S::Local>;
    type ReadSnapshot = MonitoredTableStateStore<S::ReadSnapshot>;
    type VectorWriter = MonitoredTableStateStore<S::VectorWriter>;

    fn try_wait_epoch(
        &self,
        epoch: HummockReadEpoch,
        options: TryWaitEpochOptions,
    ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
        self.inner
            .try_wait_epoch(epoch, options)
            .instrument_await("store_wait_epoch".verbose())
            .inspect_err(|e| error!(error = %e.as_report(), "Failed in wait_epoch"))
    }

    fn monitored(
        self,
        _storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> MonitoredStateStore<Self> {
        panic!("the state store is already monitored")
    }

    async fn new_local(&self, option: NewLocalOptions) -> Self::Local {
        let table_id = option.table_id;
        MonitoredTableStateStore::new(
            self.inner
                .new_local(option)
                .instrument_await("store_new_local")
                .await,
            self.storage_metrics.clone(),
            table_id,
        )
    }

    async fn new_read_snapshot(
        &self,
        epoch: HummockReadEpoch,
        options: NewReadSnapshotOptions,
    ) -> StorageResult<Self::ReadSnapshot> {
        Ok(MonitoredTableStateStore::new(
            self.inner.new_read_snapshot(epoch, options).await?,
            self.storage_metrics.clone(),
            options.table_id,
        ))
    }

    async fn new_vector_writer(&self, options: NewVectorWriterOptions) -> Self::VectorWriter {
        let table_id = options.table_id;
        MonitoredTableStateStore::new(
            self.inner.new_vector_writer(options).await,
            self.storage_metrics.clone(),
            table_id,
        )
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

impl<S: AsHummock> AsHummock for MonitoredStateStore<S> {
    fn as_hummock(&self) -> Option<&HummockStorage> {
        self.inner.as_hummock()
    }

    fn sync(
        &self,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    ) -> BoxFuture<'_, StorageResult<SyncResult>> {
        async move {
            let future = self
                .inner
                .sync(sync_table_epochs)
                .instrument_await("store_sync");
            let timer = self.storage_metrics.sync_duration.start_timer();
            let sync_size = self.storage_metrics.sync_size.clone();
            let sync_result = future
                .await
                .inspect_err(|e| error!(error = %e.as_report(), "Failed in sync"))?;
            timer.observe_duration();
            if sync_result.sync_size != 0 {
                sync_size.observe(sync_result.sync_size as _);
            }
            Ok(sync_result)
        }
        .boxed()
    }
}

/// A state store iterator wrapper for monitoring metrics.
pub(crate) struct MonitoredStateStoreIter<
    Item: IterItem,
    I,
    S: StateStoreIterStatsTrait<Item = Item>,
> {
    inner: I,
    stats: MonitoredStateStoreIterStats<S>,
    _phantom: PhantomData<Item>,
}

impl<Item: IterItem, I: StateStoreIter<Item>, S: StateStoreIterStatsTrait<Item = Item>>
    StateStoreIter<Item> for MonitoredStateStoreIter<Item, I, S>
{
    async fn try_next(&mut self) -> StorageResult<Option<Item::ItemRef<'_>>> {
        if let Some(item) = self
            .inner
            .try_next()
            .instrument(tracing::trace_span!("store_iter_try_next"))
            .await
            .inspect_err(|e| error!(error = %e.as_report(), "Failed in next"))?
        {
            self.stats.inner.observe(item);
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }
}
