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
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_hummock_sdk::key::{key_with_epoch, user_key};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::LevelType;
use risingwave_rpc_client::HummockMetaClient;

use super::version::{HummockReadVersion, ImmutableMemtable, StagingData, VersionUpdate};
use super::{
    GetFutureTrait, IngestKVBatchFutureTrait, IterFutureTrait, ReadOptions, StateStore,
    WriteOptions,
};
use crate::define_local_state_store_associated_type;
use crate::error::StorageResult;
use crate::hummock::compaction_group_client::CompactionGroupClientImpl;
// use super::monitor::StateStoreMetrics;
use crate::hummock::compaction_group_client::DummyCompactionGroupClient;
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::local_version::local_version_manager::{
    LocalVersionManager, LocalVersionManagerRef,
};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::prune_ssts;
use crate::hummock::{
    get_from_batch, get_from_sstable_info, HummockResult, HummockStateStoreIter, SstableIdManager,
    SstableIdManagerRef,
};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::storage_value::StorageValue;

#[expect(dead_code)]
pub struct HummockStorageCore {
    /// Mutable memtable.
    // memtable: Memtable,

    /// Read handle.
    read_version: RwLock<HummockReadVersion>,

    /// Event sender.
    // event_sender: mpsc::UnboundedSender<HummockEvent>,

    // TODO: use a dedicated uploader implementation to replace `LocalVersionManager`
    uploader: LocalVersionManagerRef,

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
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            Arc::new(StateStoreMetrics::unused()),
            Arc::new(CompactionGroupClientImpl::Dummy(
                DummyCompactionGroupClient::new(StaticCompactionGroupId::StateDefault.into()),
            )),
            filter_key_extractor_manager,
        )
    }

    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        compaction_group_client: Arc<CompactionGroupClientImpl>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> HummockResult<Self> {
        // For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to
        // true in `StorageConfig`
        let write_conflict_detector = ConflictDetector::new_from_config(options.clone());
        let sstable_id_manager = Arc::new(SstableIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));
        let local_version_manager = LocalVersionManager::new(
            options.clone(),
            sstable_store.clone(),
            stats.clone(),
            hummock_meta_client.clone(),
            write_conflict_detector,
            sstable_id_manager.clone(),
            filter_key_extractor_manager,
        );

        let read_version = HummockReadVersion::default();

        let instance = Self {
            options,
            read_version: RwLock::new(read_version),
            uploader: local_version_manager,
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
    pub fn update(&self, info: VersionUpdate) -> HummockResult<()> {
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
        let compaction_group_id = Some(self.get_compaction_group_id(read_options.table_id).await?);
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
        if committed_version.is_valid() {
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
        }

        local_stats.report(self.stats.as_ref());
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(table_counts as f64);

        Ok(None)
    }

    async fn get_compaction_group_id(&self, table_id: TableId) -> HummockResult<CompactionGroupId> {
        self.compaction_group_client
            .get_compaction_group_id(table_id.table_id)
            .await
    }
}

#[expect(unused_variables)]
impl StateStore for HummockStorage {
    type Iter = HummockStateStoreIter;

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

    fn iter<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
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

            let imm =
                uploader.build_shared_buffer_batch(epoch, compaction_group_id, kv_pairs, table_id);
            let imm_size = imm.size();
            self.core
                .update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())))?;

            // insert imm to uploader
            uploader.blocking_write_shared_buffer_batch(imm).await;
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
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::for_test(
            options,
            sstable_store,
            hummock_meta_client,
            filter_key_extractor_manager,
        )?;

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
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::new(
            options,
            sstable_store,
            hummock_meta_client,
            stats,
            compaction_group_client,
            filter_key_extractor_manager,
        )?;

        let instance = Self {
            core: Arc::new(storage_core),
        };
        Ok(instance)
    }
}
