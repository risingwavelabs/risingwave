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

use std::cmp::Ordering;
use std::future::Future;
use std::iter::once;
use std::ops::Bound::{Excluded, Included};
use std::ops::{Bound, Deref, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use minitrace::future::FutureExt;
use minitrace::Span;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::key::{user_key_from_table_id_and_table_key, FullKey};
use risingwave_hummock_sdk::{can_concat, key};
use risingwave_pb::hummock::LevelType;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc;

use super::memtable::ImmutableMemtable;
use super::version::{HummockReadVersion, StagingData, VersionUpdate};
use super::{
    gen_min_epoch, GetFutureTrait, IngestKVBatchFutureTrait, IterFutureTrait, ReadOptions,
    StateStore, WriteOptions,
};
use crate::error::StorageResult;
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::iterator::{
    ConcatIterator, ConcatIteratorInner, Forward, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator,
};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::{prune_ssts, search_sst_idx, validate_epoch};
use crate::hummock::{
    get_from_batch, get_from_sstable_info, hit_sstable_bloom_filter, HummockResult, MemoryLimiter,
    SstableIdManager, SstableIdManagerRef, SstableIterator,
};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::storage_value::StorageValue;
use crate::{define_local_state_store_associated_type, StateStoreIter};

#[expect(dead_code)]
pub struct HummockStorageCore {
    /// Mutable memtable.
    // memtable: Memtable,

    /// Read handle.
    read_version: Arc<RwLock<HummockReadVersion>>,

    /// Event sender.
    event_sender: mpsc::UnboundedSender<HummockEvent>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    /// Statistics
    stats: Arc<StateStoreMetrics>,

    options: Arc<StorageConfig>,

    sstable_id_manager: SstableIdManagerRef,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,

    memory_limiter: Arc<MemoryLimiter>,
}

#[derive(Clone)]
pub struct HummockStorage {
    core: Arc<HummockStorageCore>,
}

impl HummockStorageCore {
    #[cfg(any(test, feature = "test"))]
    pub fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            Arc::new(StateStoreMetrics::unused()),
            read_version,
            event_sender,
            MemoryLimiter::unlimit(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        memory_limiter: Arc<MemoryLimiter>,
    ) -> HummockResult<Self> {
        let sstable_id_manager = Arc::new(SstableIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));

        let instance = Self {
            options,
            read_version,
            hummock_meta_client,
            sstable_store,
            stats,
            sstable_id_manager,
            #[cfg(not(madsim))]
            tracing: Arc::new(risingwave_tracing::RwTracingService::new()),
            event_sender,
            memory_limiter,
        };
        Ok(instance)
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.read_version.write().update(info)
    }

    pub async fn get_inner<'a>(
        &'a self,
        table_key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        use parking_lot::RwLockReadGuard;

        // TODO: remove option
        let table_key_range = (
            Bound::Included(table_key.to_vec()),
            Bound::Included(table_key.to_vec()),
        );
        let user_key = user_key_from_table_id_and_table_key(&read_options.table_id, table_key);
        let full_key = FullKey::new(read_options.table_id, table_key, epoch).into_inner();

        let (staging_imm, staging_sst, committed_version) = {
            let read_version = self.read_version.read();
            validate_epoch(read_version.committed().safe_epoch(), epoch)?;

            let (staging_imm_iter, staging_sst_iter) = read_version.staging().prune_overlap(
                epoch,
                read_options.table_id,
                &table_key_range,
            );

            let staging_imm = staging_imm_iter
                .cloned()
                .collect::<Vec<ImmutableMemtable>>();

            let staging_sst = staging_sst_iter.cloned().collect_vec();
            let committed_version = read_version.committed().clone();

            RwLockReadGuard::unlock_fair(read_version);
            (staging_imm, staging_sst, committed_version)
        };

        let mut table_counts = 0;
        let mut local_stats = StoreLocalStatistic::default();

        // 1. read staging data
        for imm in staging_imm {
            if let Some(data) = get_from_batch(&imm, table_key, &mut local_stats) {
                return Ok(data.into_user_value());
            }
        }

        // 2. order guarantee: imm -> sst
        for local_sst in staging_sst {
            table_counts += 1;

            if let Some(data) = get_from_sstable_info(
                self.sstable_store.clone(),
                &local_sst,
                &full_key,
                read_options.check_bloom_filter,
                &mut local_stats,
            )
            .await?
            {
                return Ok(data.into_user_value());
            }
        }

        // 3. read from committed_version sst file
        assert!(committed_version.is_valid());
        for level in committed_version.levels(read_options.table_id) {
            if level.table_infos.is_empty() {
                continue;
            }
            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let sstable_infos = prune_ssts(
                        level.table_infos.iter(),
                        read_options.table_id,
                        &(user_key.as_slice()..=user_key.as_slice()),
                    );
                    for sstable_info in sstable_infos {
                        table_counts += 1;
                        if let Some(v) = get_from_sstable_info(
                            self.sstable_store.clone(),
                            sstable_info,
                            &full_key,
                            read_options.check_bloom_filter,
                            &mut local_stats,
                        )
                        .await?
                        {
                            // todo add global stat to report
                            local_stats.report(self.stats.as_ref());
                            return Ok(v.into_user_value());
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let mut table_info_idx = level.table_infos.partition_point(|table| {
                        let ord = key::user_key(&table.key_range.as_ref().unwrap().left)
                            .cmp(user_key.as_ref());
                        ord == Ordering::Less || ord == Ordering::Equal
                    });
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let ord = key::user_key(
                        &level.table_infos[table_info_idx]
                            .key_range
                            .as_ref()
                            .unwrap()
                            .right,
                    )
                    .cmp(user_key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        continue;
                    }

                    table_counts += 1;
                    if let Some(v) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        &level.table_infos[table_info_idx],
                        &full_key,
                        read_options.check_bloom_filter,
                        &mut local_stats,
                    )
                    .await?
                    {
                        local_stats.report(self.stats.as_ref());
                        return Ok(v.into_user_value());
                    }
                }
            }
        }

        local_stats.report(self.stats.as_ref());
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(table_counts as f64);

        Ok(None)
    }

    pub async fn iter_inner(
        &self,
        table_key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageIterator> {
        let user_key_range = (
            table_key_range.0.clone().map(|table_key| {
                user_key_from_table_id_and_table_key(&read_options.table_id, table_key.as_ref())
            }),
            table_key_range.1.clone().map(|table_key| {
                user_key_from_table_id_and_table_key(&read_options.table_id, table_key.as_ref())
            }),
        );

        // 1. build iterator from staging data
        let (imms, uncommitted_ssts, committed) = {
            let read_guard = self.read_version.read();
            validate_epoch(read_guard.committed().safe_epoch(), epoch)?;

            let (imm_iter, sstable_info_iter) =
                read_guard
                    .staging()
                    .prune_overlap(epoch, read_options.table_id, &table_key_range);
            (
                imm_iter.cloned().collect_vec(),
                sstable_info_iter.cloned().collect_vec(),
                read_guard.committed().clone(),
            )
        };

        let mut local_stats = StoreLocalStatistic::default();
        let mut staging_iters = Vec::with_capacity(imms.len() + uncommitted_ssts.len());
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["staging-imm-iter"])
            .observe(imms.len() as f64);
        staging_iters.extend(
            imms.into_iter()
                .map(|imm| HummockIteratorUnion::First(imm.into_forward_iter())),
        );
        let mut staging_sst_iter_count = 0;
        for sstable_info in uncommitted_ssts {
            let table_holder = self
                .sstable_store
                .sstable(&sstable_info, &mut local_stats)
                .in_span(Span::enter_with_local_parent("get_sstable"))
                .await?;
            if let Some(prefix) = read_options.prefix_hint.as_ref() {
                if !hit_sstable_bloom_filter(table_holder.value(), prefix, &mut local_stats) {
                    continue;
                }
            }
            staging_sst_iter_count += 1;
            staging_iters.push(HummockIteratorUnion::Second(SstableIterator::new(
                table_holder,
                self.sstable_store.clone(),
                Arc::new(SstableIteratorReadOptions::default()),
            )));
        }
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["staging-sst-iter"])
            .observe(staging_sst_iter_count as f64);
        let staging_iter: StagingDataIterator = OrderedMergeIteratorInner::new(staging_iters);

        // 2. build iterator from committed
        let mut non_overlapping_iters = Vec::new();
        let mut overlapping_iters = Vec::new();
        let mut overlapping_iter_count = 0;
        for level in committed.levels(read_options.table_id) {
            let table_infos = prune_ssts(
                level.table_infos.iter(),
                read_options.table_id,
                &user_key_range,
            );
            if table_infos.is_empty() {
                continue;
            }

            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&table_infos));
                let start_table_idx = match user_key_range.start_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => 0,
                };
                let end_table_idx = match user_key_range.end_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => table_infos.len().saturating_sub(1),
                };
                assert!(start_table_idx < table_infos.len() && end_table_idx < table_infos.len());
                let matched_table_infos = &table_infos[start_table_idx..=end_table_idx];

                let mut sstables = vec![];
                for sstable_info in matched_table_infos {
                    if let Some(bloom_filter_key) = read_options.prefix_hint.as_ref() {
                        let sstable = self
                            .sstable_store
                            .sstable(sstable_info, &mut local_stats)
                            .in_span(Span::enter_with_local_parent("get_sstable"))
                            .await?;

                        if hit_sstable_bloom_filter(
                            sstable.value(),
                            bloom_filter_key,
                            &mut local_stats,
                        ) {
                            sstables.push((*sstable_info).clone());
                        }
                    } else {
                        sstables.push((*sstable_info).clone());
                    }
                }

                non_overlapping_iters.push(ConcatIterator::new(
                    sstables,
                    self.sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                ));
            } else {
                // Overlapping
                let mut iters = Vec::new();
                for table_info in table_infos.into_iter().rev() {
                    let sstable = self
                        .sstable_store
                        .sstable(table_info, &mut local_stats)
                        .in_span(Span::enter_with_local_parent("get_sstable"))
                        .await?;
                    if let Some(bloom_filter_key) = read_options.prefix_hint.as_ref() {
                        if !hit_sstable_bloom_filter(
                            sstable.value(),
                            bloom_filter_key,
                            &mut local_stats,
                        ) {
                            continue;
                        }
                    }

                    iters.push(SstableIterator::new(
                        sstable,
                        self.sstable_store.clone(),
                        Arc::new(SstableIteratorReadOptions::default()),
                    ));
                    overlapping_iter_count += 1;
                }
                overlapping_iters.push(OrderedMergeIteratorInner::new(iters));
            }
        }
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["committed-overlapping-iter"])
            .observe(overlapping_iter_count as f64);
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["committed-non-overlapping-iter"])
            .observe(non_overlapping_iters.len() as f64);

        // 3. build user_iterator
        let merge_iter = UnorderedMergeIteratorInner::new(
            once(HummockIteratorUnion::First(staging_iter))
                .chain(
                    overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Second),
                )
                .chain(
                    non_overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Third),
                ),
        );

        // the epoch_range left bound for iterator read
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        let mut user_iter = UserIterator::new(
            merge_iter,
            user_key_range,
            epoch,
            min_epoch,
            Some(committed),
        );
        user_iter
            .rewind()
            .in_span(Span::enter_with_local_parent("rewind"))
            .await?;
        local_stats.report(self.stats.deref());
        Ok(HummockStorageIterator {
            inner: user_iter,
            metrics: self.stats.clone(),
        })
    }
}

#[expect(unused_variables)]
impl StateStore for HummockStorage {
    type Iter = HummockStorageIterator;

    define_local_state_store_associated_type!();

    fn insert(&self, key: Bytes, val: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    fn delete(&self, key: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    fn get<'a>(
        &'a self,
        table_key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move { self.core.get_inner(table_key, epoch, read_options).await }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move { self.core.iter_inner(key_range, epoch, read_options).await }
    }

    fn flush(&self) -> StorageResult<usize> {
        unimplemented!()
    }

    fn advance_write_epoch(&mut self, new_epoch: u64) -> StorageResult<()> {
        unimplemented!()
    }

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::IngestKVBatchFuture<'_> {
        async move {
            let epoch = write_options.epoch;
            let table_id = write_options.table_id;

            let imm = SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                kv_pairs,
                table_id,
                Some(self.core.memory_limiter.as_ref()),
            )
            .await;
            let imm_size = imm.size();
            self.core
                .update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())));

            // insert imm to uploader
            self.core
                .event_sender
                .send(HummockEvent::ImmToUploader(imm))
                .unwrap();

            Ok(imm_size)
        }
    }
}

impl HummockStorage {
    #[cfg(any(test, feature = "test"))]
    pub fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::for_test(
            options,
            sstable_store,
            hummock_meta_client,
            read_version,
            event_sender,
        )?;

        let instance = Self {
            core: Arc::new(storage_core),
        };
        Ok(instance)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        memory_limiter: Arc<MemoryLimiter>,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::new(
            options,
            sstable_store,
            hummock_meta_client,
            stats,
            read_version,
            event_sender,
            memory_limiter,
        )?;

        let instance = Self {
            core: Arc::new(storage_core),
        };
        Ok(instance)
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.core.update(info)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn read_version(&self) -> Arc<RwLock<HummockReadVersion>> {
        self.core.read_version.clone()
    }
}

type StagingDataIterator = OrderedMergeIteratorInner<
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
    metrics: Arc<StateStoreMetrics>,
}

impl StateStoreIter for HummockStorageIterator {
    type Item = (Bytes, Bytes);

    type NextFuture<'a> = impl Future<Output = StorageResult<Option<Self::Item>>> + Send;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
            let iter = &mut self.inner;

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
}

impl HummockStorageIterator {
    pub async fn collect(mut self, limit: Option<usize>) -> StorageResult<Vec<(Bytes, Bytes)>> {
        let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

        for _ in 0..limit.unwrap_or(usize::MAX) {
            match self.next().await? {
                Some(kv) => kvs.push(kv),
                None => break,
            }
        }

        Ok(kvs)
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.inner.collect_local_statistic(stats);
    }
}

impl Drop for HummockStorageIterator {
    fn drop(&mut self) {
        let mut stats = StoreLocalStatistic::default();
        self.collect_local_statistic(&mut stats);
        stats.report(&self.metrics);
    }
}
