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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::skiplist::Skiplist;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    level_delete_ssts, level_insert_ssts, split_base_levels, summarize_group_deltas,
    GroupDeltasSummary,
};
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::{CompactionGroupId, HummockSstableId};
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    HummockVersion, HummockVersionDelta, Level, LevelType, OverlappingLevel, SstableInfo,
};

use super::{LevelZeroCache, LevelZeroData};
use crate::hummock::iterator::HummockIterator;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockResult, SstableIterator};
use crate::monitor::StoreLocalStatistic;

#[derive(Clone, Debug, PartialEq)]
pub struct LocalGroup {
    pub levels: Vec<Level>,
    pub l0: LevelZeroData,
}

impl LocalGroup {
    pub fn new(max_level: u32) -> Self {
        let mut levels = Vec::with_capacity(max_level as usize);
        for level_idx in 1..=max_level {
            levels.push(Level {
                level_idx,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
            });
        }
        Self {
            levels,
            l0: LevelZeroData {
                caches: vec![],
                sub_levels: vec![],
            },
        }
    }

    fn apply_compact_ssts(&mut self, summary: GroupDeltasSummary) {
        let GroupDeltasSummary {
            delete_sst_levels,
            delete_sst_ids_set,
            insert_sst_level_id,
            insert_sub_level_id,
            insert_table_infos,
            ..
        } = summary;
        let mut deleted = false;
        for level_idx in &delete_sst_levels {
            if *level_idx == 0 {
                for level in &mut self.l0.caches {
                    deleted = level_delete_ssts(&mut level.level, &delete_sst_ids_set) || deleted;
                }
                for level in &mut self.l0.sub_levels {
                    deleted = level_delete_ssts(level, &delete_sst_ids_set) || deleted;
                }
            } else {
                let idx = *level_idx as usize - 1;
                deleted = level_delete_ssts(&mut self.levels[idx], &delete_sst_ids_set) || deleted;
            }
        }
        if !insert_table_infos.is_empty() {
            if insert_sst_level_id == 0 {
                let index = self
                    .l0
                    .sub_levels
                    .partition_point(|level| level.sub_level_id < insert_sub_level_id);
                assert!(
                    index < self.l0.sub_levels.len() && self.l0.sub_levels[index].sub_level_id == insert_sub_level_id,
                    "should find the level to insert into when applying compaction generated delta. sub level idx: {}",
                    insert_sub_level_id
                );
                level_insert_ssts(&mut self.l0.sub_levels[index], insert_table_infos);
            } else {
                let idx = insert_sst_level_id as usize - 1;
                level_insert_ssts(&mut self.levels[idx], insert_table_infos);
            }
        }
        if delete_sst_levels.iter().any(|level_id| *level_id == 0) {
            self.l0
                .caches
                .retain(|cache| !cache.level.table_infos.is_empty());
            self.l0
                .sub_levels
                .retain(|level| !level.table_infos.is_empty());
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LocalHummockVersion {
    pub id: u64,
    pub groups: HashMap<u64, LocalGroup>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
}

impl LocalHummockVersion {
    pub fn new(
        id: u64,
        group: HashMap<u64, LocalGroup>,
        max_committed_epoch: u64,
        safe_epoch: u64,
    ) -> Self {
        Self {
            id,
            groups: group,
            max_committed_epoch,
            safe_epoch,
        }
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_compaction_group_levels(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> &LocalGroup {
        self.groups
            .get(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} does not exist", compaction_group_id))
    }

    pub fn get_compaction_levels(&self) -> Vec<LocalGroup> {
        self.groups.values().cloned().collect_vec()
    }

    pub fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut LocalGroup {
        self.groups
            .get_mut(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} does not exist", compaction_group_id))
    }

    pub fn num_levels(&self, compaction_group_id: CompactionGroupId) -> usize {
        // l0 is currently separated from all levels
        self.groups
            .get(&compaction_group_id)
            .map(|group| group.levels.len() + 1)
            .unwrap_or(0)
    }

    fn init_with_parent_group(
        &mut self,
        parent_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        member_table_ids: &HashSet<StateTableId>,
    ) -> Vec<(HummockSstableId, u64, u32)> {
        let mut split_id_vers = vec![];
        if parent_group_id == StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId
            || !self.groups.contains_key(&parent_group_id)
        {
            return split_id_vers;
        }
        let [parent_levels, cur_levels] = self
            .groups
            .get_many_mut([&parent_group_id, &group_id])
            .unwrap();
        for sub_level in &mut parent_levels.l0.caches {
            let mut insert_table_infos = vec![];
            for table_info in &mut sub_level.level.table_infos {
                if table_info
                    .table_ids
                    .iter()
                    .any(|table_id| member_table_ids.contains(table_id))
                {
                    table_info.divide_version += 1;
                    split_id_vers.push((table_info.get_id(), table_info.get_divide_version(), 0));
                    let mut branch_table_info = table_info.clone();
                    branch_table_info.table_ids = table_info
                        .table_ids
                        .drain_filter(|table_id| member_table_ids.contains(table_id))
                        .collect_vec();
                    insert_table_infos.push(branch_table_info);
                }
            }
            if !insert_table_infos.is_empty() {
                add_new_sub_level(
                    &mut cur_levels.l0,
                    sub_level.level.sub_level_id,
                    sub_level.level.level_type(),
                    insert_table_infos,
                );
            }
        }
        for sub_level in &mut parent_levels.l0.sub_levels {
            let mut insert_table_infos = vec![];
            for table_info in &mut sub_level.table_infos {
                if table_info
                    .table_ids
                    .iter()
                    .any(|table_id| member_table_ids.contains(table_id))
                {
                    table_info.divide_version += 1;
                    split_id_vers.push((table_info.get_id(), table_info.get_divide_version(), 0));
                    let mut branch_table_info = table_info.clone();
                    branch_table_info.table_ids = table_info
                        .table_ids
                        .drain_filter(|table_id| member_table_ids.contains(table_id))
                        .collect_vec();
                    insert_table_infos.push(branch_table_info);
                }
            }
            if !insert_table_infos.is_empty() {
                let level = new_sub_level(
                    sub_level.sub_level_id,
                    sub_level.level_type(),
                    insert_table_infos,
                );
                cur_levels.l0.sub_levels.push(level);
            }
        }
        split_id_vers.extend(split_base_levels(
            member_table_ids,
            &mut parent_levels.levels,
            &mut cur_levels.levels,
        ));
        split_id_vers
    }

    pub fn build_compaction_group_info(&self) -> HashMap<TableId, CompactionGroupId> {
        let mut ret = HashMap::new();
        for (compaction_group_id, levels) in &self.groups {
            for sub_level in &levels.l0.sub_levels {
                for table_info in &sub_level.table_infos {
                    table_info.table_ids.iter().for_each(|table_id| {
                        ret.insert(TableId::new(*table_id), *compaction_group_id);
                    });
                }
            }
            for level in &levels.levels {
                for table_info in &level.table_infos {
                    table_info.table_ids.iter().for_each(|table_id| {
                        ret.insert(TableId::new(*table_id), *compaction_group_id);
                    });
                }
            }
        }
        ret
    }

    pub async fn fill_cache(
        &self,
        version_delta: &HummockVersionDelta,
        sstable_store: &SstableStoreRef,
    ) -> HummockResult<()> {
        for (compaction_group_id, group_deltas) in &version_delta.group_deltas {
            let summary = summarize_group_deltas(group_deltas);
            let GroupDeltasSummary {
                delete_sst_ids_set,
                insert_sst_level_id,
                insert_table_infos,
                ..
            } = summary;
            if insert_sst_level_id == 0
                && !insert_table_infos.is_empty()
                && delete_sst_ids_set.is_empty()
            {
                // `max_committed_epoch` increases. It must be a `commit_epoch`
                let group = self
                    .groups
                    .get(compaction_group_id)
                    .expect("compaction group should exist");
                // TODO: do not pre-load data to cache if the size of this delta is enough large.
                let mut stats = StoreLocalStatistic::default();
                for sstable_info in insert_table_infos {
                    if let Some(cache) = group.l0.caches.last() {
                        if cache
                            .level
                            .table_infos
                            .iter()
                            .any(|sst| sst.id == sstable_info.id)
                        {
                            // TODO: read data from shared-buffer-batch directly to speed up.
                            let data = sstable_store.sstable(&sstable_info, &mut stats).await?;
                            let mut iter = SstableIterator::new(
                                data,
                                sstable_store.clone(),
                                Arc::new(SstableIteratorReadOptions::default()),
                            );
                            iter.rewind().await?;
                            while iter.is_valid() {
                                let key = iter.key().to_vec();
                                let value = iter.value().to_bytes();
                                cache.cache.put(key, value);
                                iter.next().await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta) {
        for (compaction_group_id, group_deltas) in &version_delta.group_deltas {
            let summary = summarize_group_deltas(group_deltas);
            if let Some(group_construct) = &summary.group_construct {
                self.groups.insert(
                    *compaction_group_id,
                    LocalGroup::new(
                        group_construct.group_config.as_ref().unwrap().max_level as u32,
                    ),
                );
                let parent_group_id = group_construct.parent_group_id;
                self.init_with_parent_group(
                    parent_group_id,
                    *compaction_group_id,
                    &HashSet::from_iter(group_construct.get_table_ids().iter().cloned()),
                );
            }
            let has_destroy = summary.group_destroy.is_some();
            let levels = self
                .groups
                .get_mut(compaction_group_id)
                .expect("compaction group should exist");

            assert!(
                self.max_committed_epoch <= version_delta.max_committed_epoch,
                "new max commit epoch {} is older than the current max commit epoch {}",
                version_delta.max_committed_epoch,
                self.max_committed_epoch
            );
            if self.max_committed_epoch < version_delta.max_committed_epoch {
                // `max_committed_epoch` increases. It must be a `commit_epoch`
                let GroupDeltasSummary {
                    delete_sst_levels,
                    delete_sst_ids_set,
                    insert_sst_level_id,
                    insert_sub_level_id,
                    insert_table_infos,
                    ..
                } = summary;
                assert!(
                    insert_sst_level_id == 0 || insert_table_infos.is_empty(),
                    "we should only add to L0 when we commit an epoch. Inserting into {} {:?}",
                    insert_sst_level_id,
                    insert_table_infos
                );
                assert!(
                    delete_sst_levels.is_empty() && delete_sst_ids_set.is_empty() || has_destroy,
                    "no sst should be deleted when committing an epoch"
                );
                add_new_sub_level(
                    &mut levels.l0,
                    insert_sub_level_id,
                    LevelType::Overlapping,
                    insert_table_infos,
                );
            } else {
                // `max_committed_epoch` is not changed. The delta is caused by compaction.
                levels.apply_compact_ssts(summary);
            }
            if has_destroy {
                self.groups.remove(compaction_group_id);
            }
        }
        self.id = version_delta.id;
        self.max_committed_epoch = version_delta.max_committed_epoch;
        self.safe_epoch = version_delta.safe_epoch;
    }
}

impl From<HummockVersion> for LocalHummockVersion {
    fn from(version: HummockVersion) -> LocalHummockVersion {
        let mut groups = HashMap::with_capacity(version.levels.len());
        for (group_id, levels) in version.levels {
            groups.insert(
                group_id,
                LocalGroup {
                    levels: levels.levels,
                    l0: LevelZeroData {
                        caches: vec![],
                        sub_levels: levels.l0.unwrap().sub_levels,
                    },
                },
            );
        }
        Self {
            id: version.id,
            groups,
            max_committed_epoch: version.max_committed_epoch,
            safe_epoch: version.safe_epoch,
        }
    }
}

impl From<&LocalHummockVersion> for HummockVersion {
    fn from(version: &LocalHummockVersion) -> Self {
        let mut levels = HashMap::default();
        for (group_id, group) in &version.groups {
            let mut sub_levels = vec![];
            for sub_level in &group.l0.caches {
                sub_levels.push(sub_level.level.clone());
            }
            for sub_level in &group.l0.sub_levels {
                sub_levels.push(sub_level.clone());
            }
            levels.insert(
                *group_id,
                Levels {
                    levels: group.levels.clone(),
                    l0: Some(OverlappingLevel {
                        sub_levels,
                        total_file_size: 0,
                    }),
                },
            );
        }

        Self {
            id: version.id,
            levels,
            max_committed_epoch: version.max_committed_epoch,
            safe_epoch: version.safe_epoch,
        }
    }
}

pub fn add_new_sub_level(
    l0: &mut LevelZeroData,
    insert_sub_level_id: u64,
    level_type: LevelType,
    insert_table_infos: Vec<SstableInfo>,
) {
    if insert_sub_level_id == u64::MAX {
        return;
    }
    if let Some(newest_level) = l0.caches.last_mut() {
        if newest_level.level.sub_level_id == insert_sub_level_id {
            newest_level.level.table_infos.extend(insert_table_infos);
            return;
        }
    }
    // All files will be committed in one new Overlapping sub-level and become
    // Nonoverlapping  after at least one compaction.
    let level = new_sub_level(insert_sub_level_id, level_type, insert_table_infos);
    l0.caches.push(LevelZeroCache {
        cache: Skiplist::new(true),
        level,
    });
}

fn new_sub_level(sub_level_id: u64, level_type: LevelType, table_infos: Vec<SstableInfo>) -> Level {
    let total_file_size = table_infos.iter().map(|table| table.file_size).sum();
    Level {
        level_idx: 0,
        level_type: level_type as i32,
        table_infos,
        total_file_size,
        sub_level_id,
    }
}
