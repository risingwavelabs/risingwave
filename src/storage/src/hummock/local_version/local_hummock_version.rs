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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    add_new_sub_level, summarize_group_deltas, GroupDeltasSummary, HummockLevelsExt,
};
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    CompactionConfig, HummockVersion, HummockVersionDelta, LevelType, SstableInfo,
};

use crate::hummock::Sstable;

#[derive(Clone, Debug)]
pub struct SubLevelCache {
    pub sub_level_id: u64,
    pub data: Vec<Arc<Sstable>>,
}

impl SubLevelCache {
    pub fn new(sub_level_id: u64) -> Self {
        Self {
            sub_level_id,
            data: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalGroup {
    pub raw_group_meta: Levels,
    pub base_level_cache: Vec<Vec<Arc<Sstable>>>,
    pub sub_level_cache: Vec<SubLevelCache>,
}

impl PartialEq for LocalGroup {
    fn eq(&self, other: &LocalGroup) -> bool {
        self.raw_group_meta.eq(&other.raw_group_meta)
    }
}

impl LocalGroup {
    pub fn new(config: &CompactionConfig) -> Self {
        Self {
            raw_group_meta: Levels::build_initial_levels(config),
            base_level_cache: vec![vec![]; config.max_level as usize],
            sub_level_cache: vec![],
        }
    }

    fn apply_compact_ssts(&mut self, summary: GroupDeltasSummary) {
        self.raw_group_meta.apply_compact_ssts(summary);
        self.sub_level_cache.retain(|sub_level| {
            self.raw_group_meta
                .l0
                .as_ref()
                .unwrap()
                .sub_levels
                .iter()
                .any(|level| level.sub_level_id == sub_level.sub_level_id)
        });
    }

    pub fn add_new_sub_level(
        &mut self,
        insert_sub_level_id: u64,
        level_type: LevelType,
        insert_table_infos: Vec<SstableInfo>,
    ) {
        add_new_sub_level(
            self.raw_group_meta.l0.as_mut().unwrap(),
            insert_sub_level_id,
            level_type,
            insert_table_infos,
        );
        self.sub_level_cache
            .push(SubLevelCache::new(insert_sub_level_id));
    }

    fn init_with_parent_group(
        &mut self,
        parent_group: &mut LocalGroup,
        member_table_ids: &HashSet<StateTableId>,
    ) {
        self.raw_group_meta
            .init_with_parent_group(&mut parent_group.raw_group_meta, member_table_ids);
        for (idx, sub_level) in self
            .raw_group_meta
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .iter()
            .enumerate()
        {
            if sub_level.table_infos.is_empty() {
                self.sub_level_cache
                    .push(SubLevelCache::new(sub_level.sub_level_id));
                continue;
            }
            let data = parent_group.sub_level_cache[idx]
                .data
                .iter()
                .filter(|sst| sub_level.table_infos.iter().any(|info| info.id == sst.id))
                .cloned()
                .collect_vec();
            self.sub_level_cache.push(SubLevelCache {
                data,
                sub_level_id: sub_level.sub_level_id,
            });
        }
        for (idx, level) in self.raw_group_meta.levels.iter().enumerate() {
            if level.table_infos.is_empty() {
                continue;
            }
            let data = parent_group.base_level_cache[idx]
                .iter()
                .filter(|sst| level.table_infos.iter().any(|info| info.id == sst.id))
                .cloned()
                .collect_vec();
            self.base_level_cache[idx] = data;
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

    fn init_with_parent_group(
        &mut self,
        parent_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        mut new_group: LocalGroup,
        member_table_ids: &HashSet<StateTableId>,
    ) {
        if parent_group_id == StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId {
            return;
        }
        let parent_group = match self.groups.get_mut(&parent_group_id) {
            None => return,
            Some(group) => group,
        };
        new_group.init_with_parent_group(parent_group, member_table_ids);
        self.groups.insert(group_id, new_group);
    }

    pub fn build_compaction_group_info(&self) -> HashMap<TableId, CompactionGroupId> {
        let mut ret = HashMap::new();
        for (compaction_group_id, levels) in &self.groups {
            levels
                .raw_group_meta
                .build_compaction_group_info(*compaction_group_id, &mut ret);
        }
        ret
    }

    pub fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta) {
        for (compaction_group_id, group_deltas) in &version_delta.group_deltas {
            let summary = summarize_group_deltas(group_deltas);
            if let Some(group_construct) = &summary.group_construct {
                let parent_group_id = group_construct.parent_group_id;
                self.init_with_parent_group(
                    parent_group_id,
                    *compaction_group_id,
                    LocalGroup::new(group_construct.group_config.as_ref().unwrap()),
                    &HashSet::from_iter(group_construct.get_table_ids().iter().cloned()),
                );
            }
            let has_destroy = summary.group_destroy.is_some();
            let group = self
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
                group.add_new_sub_level(
                    insert_sub_level_id,
                    LevelType::Overlapping,
                    insert_table_infos,
                );
            } else {
                // `max_committed_epoch` is not changed. The delta is caused by compaction.
                group.apply_compact_ssts(summary);
            }
            debug_assert_eq!(
                group.sub_level_cache.len(),
                group.raw_group_meta.l0.as_ref().unwrap().sub_levels.len()
            );
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
            let mut sub_level_cache = vec![];
            for sub_level in &levels.l0.as_ref().unwrap().sub_levels {
                sub_level_cache.push(SubLevelCache::new(sub_level.sub_level_id));
            }
            groups.insert(
                group_id,
                LocalGroup {
                    base_level_cache: vec![vec![]; levels.levels.len()],
                    sub_level_cache,
                    raw_group_meta: levels,
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
            levels.insert(*group_id, group.raw_group_meta.clone());
        }
        Self {
            id: version.id,
            levels,
            max_committed_epoch: version.max_committed_epoch,
            safe_epoch: version.safe_epoch,
        }
    }
}
