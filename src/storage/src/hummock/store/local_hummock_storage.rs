// Copyright 2023 RisingWave Labs
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

use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use parking_lot::RwLock;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::util::epoch::MAX_SPILL_TIMES;
use risingwave_hummock_sdk::key::{is_empty_key_range, TableKey, TableKeyRange};
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch};
use tokio::sync::mpsc;
use tracing::{warn, Instrument};

use super::version::{HummockReadVersion, StagingData, VersionUpdate};
use crate::error::StorageResult;
use crate::hummock::event_handler::{HummockEvent, LocalInstanceGuard};
use crate::hummock::iterator::{
    ConcatIteratorInner, Forward, HummockIteratorUnion, OrderedMergeIteratorInner,
    SkipWatermarkIterator, UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator,
};
use crate::hummock::store::version::{read_filter_for_local, HummockVersionReader};
use crate::hummock::utils::{
    cmp_delete_range_left_bounds, do_delete_sanity_check, do_insert_sanity_check,
    do_update_sanity_check, filter_with_delete_range, wait_for_epoch, ENABLE_SANITY_CHECK,
};
use crate::hummock::write_limiter::WriteLimiterRef;
use crate::hummock::{MemoryLimiter, SstableIterator};
use crate::mem_table::{KeyOp, MemTable, MemTableHummockIterator};
use crate::monitor::{HummockStateStoreMetrics, IterLocalMetricsGuard, StoreLocalStatistic};
use crate::storage_value::StorageValue;
use crate::store::*;

/// `LocalHummockStorage` is a handle for a state table shard to access data from and write data to
/// the hummock state backend. It is created via `HummockStorage::new_local`.

pub struct LocalHummockStorage {
    mem_table: MemTable,

    spill_offset: u16,
    epoch: Option<u64>,

    table_id: TableId,
    is_consistent_op: bool,
    table_option: TableOption,

    instance_guard: LocalInstanceGuard,

    /// Read handle.
    read_version: Arc<RwLock<HummockReadVersion>>,

    /// This indicates that this `LocalHummockStorage` replicates another `LocalHummockStorage`.
    /// It's used by executors in different CNs to synchronize states.
    ///
    /// Within `LocalHummockStorage` we use this flag to avoid uploading local state to be
    /// persisted, so we won't have duplicate data.
    ///
    /// This also handles a corner case where an executor doing replication
    /// is scheduled to the same CN as its Upstream executor.
    /// In that case, we use this flag to avoid reading the same data twice,
    /// by ignoring the replicated ReadVersion.
    is_replicated: bool,

    /// Event sender.
    event_sender: mpsc::UnboundedSender<HummockEvent>,

    memory_limiter: Arc<MemoryLimiter>,

    hummock_version_reader: HummockVersionReader,

    stats: Arc<HummockStateStoreMetrics>,

    write_limiter: WriteLimiterRef,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,

    mem_table_spill_threshold: usize,
}

impl LocalHummockStorage {
    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.read_version.write().update(info)
    }

    pub async fn get_inner(
        &self,
        table_key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let table_key_range = (
            Bound::Included(table_key.clone()),
            Bound::Included(table_key.clone()),
        );

        let (table_key_range, read_snapshot) = read_filter_for_local(
            epoch,
            read_options.table_id,
            table_key_range,
            &self.read_version,
        )?;

        if is_empty_key_range(&table_key_range) {
            return Ok(None);
        }

        self.hummock_version_reader
            .get(table_key, epoch, read_options, read_snapshot)
            .await
    }

    pub async fn wait_for_epoch(&self, wait_epoch: u64) -> StorageResult<()> {
        wait_for_epoch(&self.version_update_notifier_tx, wait_epoch).await
    }

    pub async fn iter_flushed(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<StreamTypeOfIter<HummockStorageIterator>> {
        let (table_key_range, read_snapshot) = read_filter_for_local(
            epoch,
            read_options.table_id,
            table_key_range,
            &self.read_version,
        )?;

        let table_key_range = table_key_range;

        self.hummock_version_reader
            .iter(table_key_range, epoch, read_options, read_snapshot)
            .await
    }

    fn mem_table_iter(&self) -> MemTableHummockIterator<'_> {
        MemTableHummockIterator::new(
            &self.mem_table.buffer,
            EpochWithGap::new(self.epoch(), self.spill_offset),
            self.table_id,
        )
    }

    pub async fn iter_all(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<StreamTypeOfIter<LocalHummockStorageIterator<'_>>> {
        let (table_key_range, read_snapshot) = read_filter_for_local(
            epoch,
            read_options.table_id,
            table_key_range,
            &self.read_version,
        )?;

        self.hummock_version_reader
            .iter_with_memtable(
                table_key_range,
                epoch,
                read_options,
                read_snapshot,
                self.mem_table_iter(),
            )
            .await
    }

    pub async fn may_exist_inner(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<bool> {
        if self.mem_table.iter(key_range.clone()).next().is_some() {
            return Ok(true);
        }

        let (key_range, read_snapshot) = read_filter_for_local(
            HummockEpoch::MAX, // Use MAX epoch to make sure we read from latest
            read_options.table_id,
            key_range,
            &self.read_version,
        )?;

        self.hummock_version_reader
            .may_exist(key_range, read_options, read_snapshot)
            .await
    }
}

impl StateStoreRead for LocalHummockStorage {
    type IterStream = StreamTypeOfIter<HummockStorageIterator>;

    fn get(
        &self,
        key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Option<Bytes>>> + '_ {
        assert!(epoch <= self.epoch());
        self.get_inner(key, epoch, read_options)
    }

    fn iter(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::IterStream>> + '_ {
        assert!(epoch <= self.epoch());
        self.iter_flushed(key_range, epoch, read_options)
            .instrument(tracing::trace_span!("hummock_iter"))
    }
}

impl LocalStateStore for LocalHummockStorage {
    type IterStream<'a> = StreamTypeOfIter<LocalHummockStorageIterator<'a>>;

    fn may_exist(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<bool>> + Send + '_ {
        self.may_exist_inner(key_range, read_options)
    }

    async fn get(
        &self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        match self.mem_table.buffer.get(&key) {
            None => self.get_inner(key, self.epoch(), read_options).await,
            Some(op) => match op {
                KeyOp::Insert(value) | KeyOp::Update((_, value)) => Ok(Some(value.clone())),
                KeyOp::Delete(_) => Ok(None),
            },
        }
    }

    async fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<Self::IterStream<'_>> {
        self.iter_all(key_range.clone(), self.epoch(), read_options)
            .await
    }

    fn insert(
        &mut self,
        key: TableKey<Bytes>,
        new_val: Bytes,
        old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, old_val, new_val)?,
        };

        Ok(())
    }

    fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
        self.mem_table.delete(key, old_val)?;

        Ok(())
    }

    async fn flush(
        &mut self,
        delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
    ) -> StorageResult<usize> {
        debug_assert!(delete_ranges
            .iter()
            .map(|(key, _)| key)
            .is_sorted_by(|a, b| Some(cmp_delete_range_left_bounds(a.as_ref(), b.as_ref()))));
        let buffer = self.mem_table.drain().into_parts();
        let mut kv_pairs = Vec::with_capacity(buffer.len());
        for (key, key_op) in filter_with_delete_range(buffer.into_iter(), delete_ranges.iter()) {
            match key_op {
                // Currently, some executors do not strictly comply with these semantics. As
                // a workaround you may call disable the check by initializing the
                // state store with `is_consistent_op=false`.
                KeyOp::Insert(value) => {
                    if ENABLE_SANITY_CHECK && self.is_consistent_op {
                        do_insert_sanity_check(
                            key.clone(),
                            value.clone(),
                            self,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_put(value)));
                }
                KeyOp::Delete(old_value) => {
                    if ENABLE_SANITY_CHECK && self.is_consistent_op {
                        do_delete_sanity_check(
                            key.clone(),
                            old_value,
                            self,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_delete()));
                }
                KeyOp::Update((old_value, new_value)) => {
                    if ENABLE_SANITY_CHECK && self.is_consistent_op {
                        do_update_sanity_check(
                            key.clone(),
                            old_value,
                            new_value.clone(),
                            self,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_put(new_value)));
                }
            }
        }
        self.flush_inner(
            kv_pairs,
            delete_ranges,
            WriteOptions {
                epoch: self.epoch(),
                table_id: self.table_id,
            },
        )
        .await
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        if self.mem_table.kv_size.size() > self.mem_table_spill_threshold {
            if self.spill_offset < MAX_SPILL_TIMES {
                let table_id_label = self.table_id.table_id().to_string();
                self.flush(vec![]).await?;
                self.stats
                    .mem_table_spill_counts
                    .with_label_values(&[table_id_label.as_str()])
                    .inc();
            } else {
                tracing::warn!("No mem table spill occurs, the gap epoch exceeds available range.");
            }
        }

        Ok(())
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch")
    }

    fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
        let epoch = options.epoch;
        if self.is_replicated {
            self.wait_for_epoch(epoch.prev).await?;
        }
        assert!(
            self.epoch.replace(epoch.curr).is_none(),
            "local state store of table id {:?} is init for more than once",
            self.table_id
        );

        Ok(())
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
        assert!(!self.is_dirty());
        let prev_epoch = self
            .epoch
            .replace(next_epoch)
            .expect("should have init epoch before seal the first epoch");
        self.spill_offset = 0;
        assert!(
            next_epoch > prev_epoch,
            "new epoch {} should be greater than current epoch: {}",
            next_epoch,
            prev_epoch
        );
        self.event_sender
            .send(HummockEvent::LocalSealEpoch {
                instance_id: self.instance_id(),
                table_id: self.table_id,
                epoch: prev_epoch,
                opts,
            })
            .expect("should be able to send")
    }
}

impl LocalHummockStorage {
    async fn flush_inner(
        &mut self,
        kv_pairs: Vec<(TableKey<Bytes>, StorageValue)>,
        delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
        write_options: WriteOptions,
    ) -> StorageResult<usize> {
        let epoch = write_options.epoch;
        let table_id = write_options.table_id;

        let table_id_label = table_id.to_string();
        self.stats
            .write_batch_tuple_counts
            .with_label_values(&[table_id_label.as_str()])
            .inc_by(kv_pairs.len() as _);
        let timer = self
            .stats
            .write_batch_duration
            .with_label_values(&[table_id_label.as_str()])
            .start_timer();

        let imm_size = if !kv_pairs.is_empty() || !delete_ranges.is_empty() {
            let sorted_items = SharedBufferBatch::build_shared_buffer_item_batches(kv_pairs);
            let size = SharedBufferBatch::measure_batch_size(&sorted_items)
                + SharedBufferBatch::measure_delete_range_size(&delete_ranges);
            self.write_limiter.wait_permission(self.table_id).await;
            let limiter = self.memory_limiter.as_ref();
            let tracker = if let Some(tracker) = limiter.try_require_memory(size as u64) {
                tracker
            } else {
                warn!(
                    "blocked at requiring memory: {}, current {}",
                    size,
                    limiter.get_memory_usage()
                );
                self.event_sender
                    .send(HummockEvent::BufferMayFlush)
                    .expect("should be able to send");
                let tracker = limiter
                    .require_memory(size as u64)
                    .verbose_instrument_await("hummock_require_memory")
                    .await;
                warn!(
                    "successfully requiring memory: {}, current {}",
                    size,
                    limiter.get_memory_usage()
                );
                tracker
            };

            let instance_id = self.instance_guard.instance_id;
            let imm = SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                self.spill_offset,
                sorted_items,
                size,
                delete_ranges,
                table_id,
                Some(instance_id),
                Some(tracker),
            );
            self.spill_offset += 1;
            let imm_size = imm.size();
            self.update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())));

            // insert imm to uploader
            if !self.is_replicated {
                self.event_sender
                    .send(HummockEvent::ImmToUploader(imm))
                    .unwrap();
            }
            imm_size
        } else {
            0
        };

        timer.observe_duration();

        self.stats
            .write_batch_size
            .with_label_values(&[table_id_label.as_str()])
            .observe(imm_size as _);
        Ok(imm_size)
    }
}

impl LocalHummockStorage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance_guard: LocalInstanceGuard,
        read_version: Arc<RwLock<HummockReadVersion>>,
        hummock_version_reader: HummockVersionReader,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        memory_limiter: Arc<MemoryLimiter>,
        write_limiter: WriteLimiterRef,
        option: NewLocalOptions,
        version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,
        mem_table_spill_threshold: usize,
    ) -> Self {
        let stats = hummock_version_reader.stats().clone();
        Self {
            mem_table: MemTable::new(option.is_consistent_op),
            spill_offset: 0,
            epoch: None,
            table_id: option.table_id,
            is_consistent_op: option.is_consistent_op,
            table_option: option.table_option,
            is_replicated: option.is_replicated,
            instance_guard,
            read_version,
            event_sender,
            memory_limiter,
            hummock_version_reader,
            stats,
            write_limiter,
            version_update_notifier_tx,
            mem_table_spill_threshold,
        }
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn read_version(&self) -> Arc<RwLock<HummockReadVersion>> {
        self.read_version.clone()
    }

    pub fn table_id(&self) -> TableId {
        self.instance_guard.table_id
    }

    pub fn instance_id(&self) -> u64 {
        self.instance_guard.instance_id
    }
}

pub type StagingDataIterator = OrderedMergeIteratorInner<
    HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>,
>;
pub type HummockStorageIteratorPayloadInner<'a> = SkipWatermarkIterator<
    UnorderedMergeIteratorInner<
        HummockIteratorUnion<
            Forward,
            StagingDataIterator,
            SstableIterator,
            ConcatIteratorInner<SstableIterator>,
            MemTableHummockIterator<'a>,
        >,
    >,
>;

pub type HummockStorageIterator = HummockStorageIteratorInner<'static>;
pub type LocalHummockStorageIterator<'a> = HummockStorageIteratorInner<'a>;

pub struct HummockStorageIteratorInner<'a> {
    inner: UserIterator<HummockStorageIteratorPayloadInner<'a>>,
    stats_guard: IterLocalMetricsGuard,
}

impl<'a> StateStoreIter for HummockStorageIteratorInner<'a> {
    type Item = StateStoreIterItem;

    async fn next(&mut self) -> StorageResult<Option<Self::Item>> {
        let iter = &mut self.inner;

        if iter.is_valid() {
            let kv = (iter.key().clone(), iter.value().clone());
            iter.next().await?;
            Ok(Some(kv))
        } else {
            Ok(None)
        }
    }
}

impl<'a> HummockStorageIteratorInner<'a> {
    pub fn new(
        inner: UserIterator<HummockStorageIteratorPayloadInner<'a>>,
        metrics: Arc<HummockStateStoreMetrics>,
        table_id: TableId,
        local_stats: StoreLocalStatistic,
    ) -> Self {
        Self {
            inner,
            stats_guard: IterLocalMetricsGuard::new(metrics, table_id, local_stats),
        }
    }
}

impl<'a> Drop for HummockStorageIteratorInner<'a> {
    fn drop(&mut self) {
        self.inner
            .collect_local_statistic(&mut self.stats_guard.local_stats);
    }
}
