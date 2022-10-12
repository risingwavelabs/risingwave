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
use risingwave_common::catalog::TableId;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::key::{key_with_epoch, user_key};
use risingwave_hummock_sdk::{can_concat, CompactionGroupId};
use risingwave_pb::hummock::LevelType;
use risingwave_rpc_client::HummockMetaClient;

use super::version::{HummockReadVersion, ImmutableMemtable, StagingData, VersionUpdate};
use super::{
    GetFutureTrait, IngestKVBatchFutureTrait, IterFutureTrait, ReadOptions, StateStore,
    WriteOptions,
};
use crate::error::StorageResult;
use crate::hummock::compaction_group_client::CompactionGroupClientImpl;
use crate::hummock::iterator::{
    ConcatIterator, ConcatIteratorInner, Forward, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::local_version::local_version_manager::LocalVersionManagerRef;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchIterator;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{
    get_from_batch, get_from_sstable_info, SstableIdManager, SstableIdManagerRef,
};
use crate::storage_value::StorageValue;

pub type UploaderRef = LocalVersionManagerRef;
use crate::hummock::utils::{prune_ssts, search_sst_idx};
use crate::hummock::{hit_sstable_bloom_filter, HummockResult, SstableIterator};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::{define_local_state_store_associated_type, StateStoreIter};

#[expect(dead_code)]
pub struct HummockStorageCore {
    /// Mutable memtable.
    // memtable: Memtable,

    /// Read handle.
    read_version: RwLock<HummockReadVersion>,

    /// Event sender.
    // event_sender: mpsc::UnboundedSender<HummockEvent>,

    // TODO: use a dedicated uploader implementation to replace `LocalVersionManager`
    uploader: UploaderRef,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    compaction_group_client: Arc<CompactionGroupClientImpl>,

    /// Statistics
    stats: Arc<StateStoreMetrics>,

    options: Arc<StorageConfig>,

    sstable_id_manager: SstableIdManagerRef,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,
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
        uploader: UploaderRef,
    ) -> HummockResult<Self> {
        use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;

        use crate::hummock::compaction_group_client::DummyCompactionGroupClient;

        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            Arc::new(StateStoreMetrics::unused()),
            Arc::new(CompactionGroupClientImpl::Dummy(
                DummyCompactionGroupClient::new(StaticCompactionGroupId::StateDefault.into()),
            )),
            uploader,
        )
    }

    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        compaction_group_client: Arc<CompactionGroupClientImpl>,
        uploader: UploaderRef,
    ) -> HummockResult<Self> {
        // For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to
        // true in `StorageConfig`
        let sstable_id_manager = Arc::new(SstableIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));

        let read_version = HummockReadVersion::new(uploader.get_pinned_version());

        let instance = Self {
            options,
            read_version: RwLock::new(read_version),
            uploader,
            hummock_meta_client,
            sstable_store,
            stats,
            compaction_group_client,
            sstable_id_manager,
            #[cfg(not(madsim))]
            tracing: Arc::new(risingwave_tracing::RwTracingService::new()),
        };
        Ok(instance)
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.read_version.write().update(info)
    }

    pub async fn get_inner<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        use parking_lot::RwLockReadGuard;

        // TODO: remove option
        let compaction_group_id = self.get_compaction_group_id(read_options.table_id).await?;
        let key_range = (Bound::Included(key.to_vec()), Bound::Included(key.to_vec()));

        let (staging_imm, staging_sst, committed_version) = {
            let read_version = self.read_version.read();

            let (staging_imm_iter, staging_sst_iter) =
                read_version
                    .staging()
                    .prune_overlap(epoch, compaction_group_id, &key_range);

            let staging_imm = staging_imm_iter
                .cloned()
                .collect::<Vec<ImmutableMemtable>>();

            let staging_sst = staging_sst_iter.cloned().collect_vec();
            let committed_version = read_version.committed().clone();

            RwLockReadGuard::unlock_fair(read_version);
            (staging_imm, staging_sst, committed_version)
        };

        let mut table_counts = 0;
        let internal_key = key_with_epoch(key.to_vec(), epoch);
        let mut local_stats = StoreLocalStatistic::default();

        // 1. read staging data
        // 2. order guarantee: imm -> sst
        for imm in staging_imm {
            if let Some(data) = get_from_batch(&imm, key, &mut local_stats) {
                return Ok(data.into_user_value());
            }
        }

        for local_sst in staging_sst {
            table_counts += 1;

            if let Some(data) = get_from_sstable_info(
                self.sstable_store.clone(),
                &local_sst,
                &internal_key,
                read_options.check_bloom_filter,
                &mut local_stats,
            )
            .await?
            {
                return Ok(data.into_user_value());
            }
        }

        // 2. read from committed_version sst file
        assert!(committed_version.is_valid());
        for level in committed_version.levels(compaction_group_id) {
            if level.table_infos.is_empty() {
                continue;
            }
            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let sstable_infos = prune_ssts(level.table_infos.iter(), &(key..=key));
                    for sstable_info in sstable_infos {
                        table_counts += 1;
                        if let Some(v) = get_from_sstable_info(
                            self.sstable_store.clone(),
                            sstable_info,
                            &internal_key,
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
                        let ord =
                            user_key(&table.key_range.as_ref().unwrap().left).cmp(key.as_ref());
                        ord == Ordering::Less || ord == Ordering::Equal
                    });
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let ord = user_key(
                        &level.table_infos[table_info_idx]
                            .key_range
                            .as_ref()
                            .unwrap()
                            .right,
                    )
                    .cmp(key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        continue;
                    }

                    table_counts += 1;
                    if let Some(v) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        &level.table_infos[table_info_idx],
                        &internal_key,
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
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageIterator> {
        let compaction_group_id = self.get_compaction_group_id(read_options.table_id).await?;
        // 1. build iterator from staging data
        let (imms, uncommitted_ssts, committed) = {
            let read_guard = self.read_version.read();
            let (imm_iter, sstable_info_iter) =
                read_guard
                    .staging()
                    .prune_overlap(epoch, compaction_group_id, &key_range);
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
        for level in committed.levels(compaction_group_id) {
            let table_infos = prune_ssts(level.table_infos.iter(), &key_range);
            if table_infos.is_empty() {
                continue;
            }

            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&table_infos));
                let start_table_idx = match key_range.start_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => 0,
                };
                let end_table_idx = match key_range.end_bound() {
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
        // TODO: may want to set `min_epoch` by retention time.
        let mut user_iter = UserIterator::new(merge_iter, key_range, epoch, 0, Some(committed));
        user_iter
            .rewind()
            .in_span(Span::enter_with_local_parent("rewind"))
            .await?;
        local_stats.report(self.stats.deref());
        Ok(HummockStorageIterator { inner: user_iter })
    }

    async fn get_compaction_group_id(&self, table_id: TableId) -> HummockResult<CompactionGroupId> {
        self.compaction_group_client
            .get_compaction_group_id(table_id.table_id)
            .await
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
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move { self.core.get_inner(key, epoch, read_options).await }
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
            let uploader = self.core.uploader.clone();
            let compaction_group_id = self
                .core
                .get_compaction_group_id(write_options.table_id)
                .await?;

            let imm = uploader
                .build_shared_buffer_batch(epoch, compaction_group_id, kv_pairs, table_id)
                .await;
            let imm_size = imm.size();
            self.core
                .update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())));

            // insert imm to uploader
            uploader.write_shared_buffer_batch(imm);
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
        uploader: UploaderRef,
    ) -> HummockResult<Self> {
        let storage_core =
            HummockStorageCore::for_test(options, sstable_store, hummock_meta_client, uploader)?;

        let instance = Self {
            core: Arc::new(storage_core),
        };
        Ok(instance)
    }

    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        compaction_group_client: Arc<CompactionGroupClientImpl>,
        uploader: UploaderRef,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::new(
            options,
            sstable_store,
            hummock_meta_client,
            stats,
            compaction_group_client,
            uploader,
        )?;

        let instance = Self {
            core: Arc::new(storage_core),
        };
        Ok(instance)
    }

    pub fn uploader(&self) -> &UploaderRef {
        &self.core.uploader
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.core.update(info)
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
