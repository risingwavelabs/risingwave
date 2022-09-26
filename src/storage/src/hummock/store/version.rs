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
use std::collections::VecDeque;
use std::ops::RangeBounds;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::key::{key_with_epoch, next_key, user_key};
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta, LevelType};

// use super::memtable::Memtable;
use super::{GetFutureTrait, IterFutureTrait, ReadOptions};
use crate::hummock::local_version::{PinnedVersion, PinnedVersionGuard};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::{filter_single_sst, prune_ssts, range_overlap};
use crate::hummock::{get_from_batch, get_from_table, HummockResult, HummockStateStoreIter};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

type ImmutableMemtable = SharedBufferBatch;
type ImmIdVec = Vec<usize>;

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.

#[derive(Clone)]
pub enum StagingData {
    // ImmMem(Arc<Memtable>),
    ImmMem(Arc<ImmutableMemtable>),
    Sst((LocalSstableInfo, ImmIdVec)),
}

pub enum VersionUpdate {
    /// a new staging data entry will be added.
    Staging(StagingData),
    CommittedDelta(HummockVersionDelta),
    CommittedSnapshot(HummockVersion),
}

pub struct StagingVersion {
    imm: VecDeque<Arc<ImmutableMemtable>>,
    sst: VecDeque<LocalSstableInfo>,
}

// TODO: use a custom data structure to allow in-place update instead of proto
// pub type CommittedVersion = HummockVersion;

pub type CommittedVersion = PinnedVersion;

/// A container of information required for reading from hummock.
#[allow(unused)]
pub struct HummockReadVersion {
    /// Local version for staging data.
    staging: StagingVersion,

    /// Remote version for committed data.
    committed: CommittedVersion,

    sstable_store: SstableStoreRef,
}

#[allow(unused)]
impl HummockReadVersion {
    /// Updates the read version with `VersionUpdate`.
    /// A `OrderIdx` that can uniquely identify the newly added entry will be returned.
    pub fn update(&mut self, info: VersionUpdate) -> HummockResult<()> {
        match info {
            VersionUpdate::Staging(staging) => {
                match staging {
                    StagingData::ImmMem(imm) => self.staging.imm.push_back(imm),
                    StagingData::Sst((sst, clear_id_vec)) => {
                        // TODO  need to clear the order_index data included in SST file
                        self.staging.sst.push_back(sst);
                    }
                }
            }

            VersionUpdate::CommittedDelta(_) => {
                unimplemented!()
            }
            VersionUpdate::CommittedSnapshot(_) => {
                unimplemented!()
            }
        }

        Ok(())
    }

    /// Point gets a value from the state store based on the read version.
    pub fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl GetFutureTrait<'_> {
        async move {
            let compaction_group_id = Some(2);
            let mut table_counts = 0;
            let internal_key = key_with_epoch(key.to_vec(), epoch);
            let mut local_stats = StoreLocalStatistic::default();
            let internal_key = key_with_epoch(key.to_vec(), epoch);

            // 1. read staging data
            for imm in &self.staging.imm {
                if imm.epoch() > epoch {
                    break;
                }

                if imm.epoch() != epoch {
                    continue;
                }

                if !range_overlap(&(key..=key), imm.start_user_key(), imm.end_user_key()) {
                    continue;
                }

                if let Some(data) = get_from_batch(imm, key, &mut local_stats) {
                    return Ok(data.into_user_value());
                }
            }

            for (compaction_group_id, local_sst) in &self.staging.sst {
                let table = self
                    .sstable_store
                    .sstable(local_sst, &mut local_stats)
                    .await?;

                if let Some(data) = get_from_table(
                    self.sstable_store.clone(),
                    table,
                    &internal_key,
                    read_options.check_bloom_filter,
                    &mut local_stats,
                )
                .await?
                {
                    return Ok(data.into_user_value());
                }
            }

            // // 2. read from commited version
            let commited_version = self.committed.version();
            let levels = match compaction_group_id {
                None => commited_version.get_combined_levels(),
                Some(compaction_group_id) => {
                    let levels = commited_version.get_compaction_group_levels(compaction_group_id);
                    let mut ret = vec![];
                    ret.extend(levels.l0.as_ref().unwrap().sub_levels.iter().rev());
                    ret.extend(levels.levels.iter());
                    ret
                }
            };

            for level in self.committed.levels(compaction_group_id) {
                if level.table_infos.is_empty() {
                    continue;
                }
                match level.level_type() {
                    LevelType::Overlapping | LevelType::Unspecified => {
                        let table_infos = prune_ssts(level.table_infos.iter(), &(key..=key));
                        for table_info in table_infos {
                            let table = self
                                .sstable_store
                                .sstable(table_info, &mut local_stats)
                                .await?;
                            table_counts += 1;
                            if let Some(v) = get_from_table(
                                self.sstable_store.clone(),
                                table,
                                &internal_key,
                                read_options.check_bloom_filter,
                                &mut local_stats,
                            )
                            .await?
                            {
                                // todo add global stat to report
                                // local_stats.report(self.stats.as_ref());
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

                        let table = self
                            .sstable_store
                            .sstable(&level.table_infos[table_info_idx], &mut local_stats)
                            .await?;
                        table_counts += 1;
                        if let Some(v) = get_from_table(
                            self.sstable_store.clone(),
                            table,
                            &internal_key,
                            read_options.check_bloom_filter,
                            &mut local_stats,
                        )
                        .await?
                        {
                            // local_stats.report(self.stats.as_ref());
                            return Ok(v.into_user_value());
                        }
                    }
                }
            }

            Ok(None)
        }
    }

    pub fn read_filter<R, B>(
        &self,
        epoch: u64,
        key_range: &R,
        read_options: &ReadOptions,
    ) -> HummockReadVersion
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let mut local_stats = StoreLocalStatistic::default();

        let (staging_imm, staging_sst) = {
            let mut filter_imm = VecDeque::default();
            // TODO: to impl read filter imm
            for imm in &self.staging.imm {
                if imm.epoch() > epoch {
                    break;
                }

                if imm.epoch() != epoch {
                    continue;
                }

                if !range_overlap(key_range, imm.start_user_key(), imm.end_user_key()) {
                    continue;
                }

                filter_imm.push_back(imm.clone());
            }

            let filter_sst: VecDeque<LocalSstableInfo> = self
                .staging
                .sst
                .iter()
                .filter(|(_, local_sst)| filter_single_sst(local_sst, key_range))
                .cloned()
                .collect();

            (filter_imm, filter_sst)
        };

        HummockReadVersion {
            staging: StagingVersion {
                imm: staging_imm,
                sst: staging_sst,
            },
            committed: self.committed.clone(),
            sstable_store: self.sstable_store.clone(),
        }
    }

    /// Opens and returns an iterator for a given `key_range` based on the read version.
    pub fn iter<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl IterFutureTrait<'_, HummockStateStoreIter, R, B>
    where
        R: 'static + Send + RangeBounds<B>,
        B: 'static + Send + AsRef<[u8]>,
    {
        async move { unimplemented!() }
    }
}
