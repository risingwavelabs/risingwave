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

use async_stack_trace::StackTrace;
use bytes::Bytes;
use minitrace::future::FutureExt;
use parking_lot::RwLock;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_hummock_sdk::key::{map_table_key_range, TableKey, TableKeyRange};
use risingwave_hummock_sdk::HummockEpoch;
use tokio::sync::mpsc;
use tracing::warn;

use super::version::{HummockReadVersion, StagingData, VersionUpdate};
use crate::error::StorageResult;
use crate::hummock::event_handler::{HummockEvent, LocalInstanceGuard};
use crate::hummock::iterator::{
    ConcatIteratorInner, Forward, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator,
};
use crate::hummock::store::version::{read_filter_for_local, HummockVersionReader};
use crate::hummock::utils::{
    do_delete_sanity_check, do_insert_sanity_check, do_update_sanity_check,
    filter_with_delete_range, ENABLE_SANITY_CHECK,
};
use crate::hummock::{MemoryLimiter, SstableIterator};
use crate::mem_table::{merge_stream, KeyOp, MemTable};
use crate::monitor::{HummockStateStoreMetrics, IterLocalMetricsGuard, StoreLocalStatistic};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{
    define_local_state_store_associated_type, define_state_store_read_associated_type,
    StateStoreIter,
};

pub struct LocalHummockStorage {
    mem_table: MemTable,

    epoch: Option<u64>,

    table_id: TableId,
    is_consistent_op: bool,
    table_option: TableOption,

    instance_guard: LocalInstanceGuard,

    /// Read handle.
    read_version: Arc<RwLock<HummockReadVersion>>,

    /// Event sender.
    event_sender: mpsc::UnboundedSender<HummockEvent>,

    memory_limiter: Arc<MemoryLimiter>,

    hummock_version_reader: HummockVersionReader,

    tracing: Arc<risingwave_tracing::RwTracingService>,

    stats: Arc<HummockStateStoreMetrics>,
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

        let read_snapshot = read_filter_for_local(
            epoch,
            read_options.table_id,
            &table_key_range,
            self.read_version.clone(),
        )?;

        self.hummock_version_reader
            .get(table_key, epoch, read_options, read_snapshot)
            .await
    }

    pub async fn iter_inner(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<StreamTypeOfIter<HummockStorageIterator>> {
        let read_snapshot = read_filter_for_local(
            epoch,
            read_options.table_id,
            &table_key_range,
            self.read_version.clone(),
        )?;

        self.hummock_version_reader
            .iter(table_key_range, epoch, read_options, read_snapshot)
            .await
    }

    pub async fn may_exist_inner(
        &self,
        key_range: IterKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<bool> {
        if self.mem_table.iter(key_range.clone()).next().is_some() {
            return Ok(true);
        }

        let table_key_range = map_table_key_range(key_range);

        let read_snapshot = read_filter_for_local(
            HummockEpoch::MAX, // Use MAX epoch to make sure we read from latest
            read_options.table_id,
            &table_key_range,
            self.read_version.clone(),
        )?;

        self.hummock_version_reader
            .may_exist(table_key_range, read_options, read_snapshot)
            .await
    }
}

impl StateStoreRead for LocalHummockStorage {
    type IterStream = StreamTypeOfIter<HummockStorageIterator>;

    define_state_store_read_associated_type!();

    fn get(&self, key: Bytes, epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
        assert!(epoch <= self.epoch());
        self.get_inner(TableKey(key), epoch, read_options)
    }

    fn iter(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        assert!(epoch <= self.epoch());
        self.iter_inner(map_table_key_range(key_range), epoch, read_options)
            .in_span(self.tracing.new_tracer("hummock_iter"))
    }
}

impl LocalStateStore for LocalHummockStorage {
    type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
    type GetFuture<'a> = impl GetFutureTrait<'a>;
    type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;

    define_local_state_store_associated_type!();

    fn may_exist(
        &self,
        key_range: IterKeyRange,
        read_options: ReadOptions,
    ) -> Self::MayExistFuture<'_> {
        self.may_exist_inner(key_range, read_options)
    }

    fn get(&self, key: Bytes, read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move {
            match self.mem_table.buffer.get(&key) {
                None => {
                    self.get_inner(TableKey(key), self.epoch(), read_options)
                        .await
                }
                Some(op) => match op {
                    KeyOp::Insert(value) | KeyOp::Update((_, value)) => Ok(Some(value.clone())),
                    KeyOp::Delete(_) => Ok(None),
                },
            }
        }
    }

    fn iter(&self, key_range: IterKeyRange, read_options: ReadOptions) -> Self::IterFuture<'_> {
        async move {
            let stream = self
                .iter_inner(
                    map_table_key_range(key_range.clone()),
                    self.epoch(),
                    read_options,
                )
                .await?;
            let (l, r) = key_range;
            let key_range = (l.map(Bytes::from), r.map(Bytes::from));
            Ok(merge_stream(
                self.mem_table.iter(key_range),
                stream,
                self.table_id,
                self.epoch(),
            ))
        }
    }

    fn insert(&mut self, key: Bytes, new_val: Bytes, old_val: Option<Bytes>) -> StorageResult<()> {
        match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, old_val, new_val)?,
        };
        Ok(())
    }

    fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()> {
        Ok(self.mem_table.delete(key, old_val)?)
    }

    fn flush(&mut self, delete_ranges: Vec<(Bytes, Bytes)>) -> Self::FlushFuture<'_> {
        async move {
            debug_assert!(delete_ranges.iter().map(|(key, _)| key).is_sorted());
            let buffer = self.mem_table.drain().into_parts();
            let mut kv_pairs = Vec::with_capacity(buffer.len());
            for (key, key_op) in filter_with_delete_range(buffer.into_iter(), delete_ranges.iter())
            {
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
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch")
    }

    fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    fn init(&mut self, epoch: u64) {
        assert!(
            self.epoch.replace(epoch).is_none(),
            "local state store of table id {:?} is init for more than once",
            self.table_id
        );
    }

    fn seal_current_epoch(&mut self, next_epoch: u64) {
        assert!(!self.is_dirty());
        let prev_epoch = self
            .epoch
            .replace(next_epoch)
            .expect("should have init epoch before seal the first epoch");
        assert!(
            next_epoch > prev_epoch,
            "new epoch {} should be greater than current epoch: {}",
            next_epoch,
            prev_epoch
        );
    }
}

impl LocalHummockStorage {
    async fn flush_inner(
        &mut self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> StorageResult<usize> {
        if kv_pairs.is_empty() && delete_ranges.is_empty() {
            return Ok(0);
        }

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

        let sorted_items = SharedBufferBatch::build_shared_buffer_item_batches(kv_pairs);
        let size = SharedBufferBatch::measure_batch_size(&sorted_items);
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
                .verbose_stack_trace("hummock_require_memory")
                .await;
            warn!(
                "successfully requiring memory: {}, current {}",
                size,
                limiter.get_memory_usage()
            );
            tracker
        };

        let imm = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            sorted_items,
            size,
            delete_ranges,
            table_id,
            Some(tracker),
        );
        let imm_size = imm.size();
        self.update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())));

        // insert imm to uploader
        self.event_sender
            .send(HummockEvent::ImmToUploader(imm))
            .unwrap();

        timer.observe_duration();

        self.stats
            .write_batch_size
            .with_label_values(&[table_id_label.as_str()])
            .observe(imm_size as _);
        Ok(imm_size)
    }
}

impl LocalHummockStorage {
    pub fn new(
        instance_guard: LocalInstanceGuard,
        read_version: Arc<RwLock<HummockReadVersion>>,
        hummock_version_reader: HummockVersionReader,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        memory_limiter: Arc<MemoryLimiter>,
        tracing: Arc<risingwave_tracing::RwTracingService>,
        option: NewLocalOptions,
    ) -> Self {
        let stats = hummock_version_reader.stats().clone();
        Self {
            mem_table: MemTable::new(option.is_consistent_op),
            epoch: None,
            table_id: option.table_id,
            is_consistent_op: option.is_consistent_op,
            table_option: option.table_option,
            instance_guard,
            read_version,
            event_sender,
            memory_limiter,
            hummock_version_reader,
            tracing,
            stats,
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
type HummockStorageIteratorPayload = UnorderedMergeIteratorInner<
    HummockIteratorUnion<
        Forward,
        StagingDataIterator,
        OrderedMergeIteratorInner<SstableIterator>,
        ConcatIteratorInner<SstableIterator>,
    >,
>;

pub struct HummockStorageIterator {
    inner: UserIterator<HummockStorageIteratorPayload>,
    stats_guard: IterLocalMetricsGuard,
}

impl StateStoreIter for HummockStorageIterator {
    type Item = StateStoreIterItem;

    type NextFuture<'a> = impl StateStoreIterNextFutureTrait<'a>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
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
}

impl HummockStorageIterator {
    pub fn new(
        inner: UserIterator<HummockStorageIteratorPayload>,
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

impl Drop for HummockStorageIterator {
    fn drop(&mut self) {
        self.inner
            .collect_local_statistic(&mut self.stats_guard.local_stats);
    }
}
