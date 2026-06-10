// Copyright 2026 RisingWave Labs
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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::level::{InputLevel, Level};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId, HummockSstableId};
use risingwave_pb::hummock::level_handler::RunningCompactTask;
use risingwave_pb::hummock::{
    CompactTask as PbCompactTask, CompactTaskAssignment as PbCompactTaskAssignment,
    InputLevel as PbInputLevel, LevelType,
};

/// A non-overlapping target container that a compaction task writes into.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TargetContainer {
    /// A non-L0 level.
    Level(u32),
    /// A non-overlapping L0 sub-level.
    L0SubLevel(u64),
}

/// The key range that a compaction task will rewrite in a non-overlapping target container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetWriteScope {
    pub target_container: TargetContainer,
    pub key_range: KeyRange,
}

/// Input files from one level selected by a running compaction task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InProgressInputLevel {
    pub level_idx: u32,
    pub sst_ids: Vec<HummockSstableId>,
    pub total_file_size: u64,
}

/// A picker-facing projection of a running compaction task.
///
/// This is derived from the full `CompactTask` stored in `compact_task_assignment`. It should not
/// become a separate source of truth.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InProgressCompactInfo {
    pub task_id: HummockCompactionTaskId,
    pub compaction_group_id: CompactionGroupId,
    /// Input SSTs selected by this running task, grouped by input level.
    pub input_levels: Vec<InProgressInputLevel>,
    pub target_level: u32,
    /// Target write scope if the task writes to a non-overlapping target container.
    pub target_write_scope: Option<TargetWriteScope>,
}

/// Input-side pending index used by pickers to avoid selecting the same SST twice.
///
/// `tasks` is the compact protobuf-compatible representation. `sst_to_task` is derived from it for
/// fast picker checks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PendingInputSstIndex {
    tasks: Vec<RunningCompactTask>,
    sst_to_task: HashMap<HummockSstableId, HummockCompactionTaskId>,
}

impl TargetWriteScope {
    pub fn from_compact_task(task: &CompactTask) -> Option<Self> {
        Self::from_input_levels(
            task.target_level,
            task.target_sub_level_id,
            &task.input_ssts,
        )
    }

    pub fn from_pb_compact_task(task: &PbCompactTask) -> Option<Self> {
        let target_container = pb_target_container(
            task.target_level,
            task.target_sub_level_id,
            &task.input_ssts,
        )?;
        let key_range = pb_input_key_range(&task.input_ssts)?;
        Some(Self {
            target_container,
            key_range,
        })
    }

    pub(crate) fn from_input_levels(
        target_level: u32,
        target_sub_level_id: u64,
        input_levels: &[InputLevel],
    ) -> Option<Self> {
        let target_container = target_container(target_level, target_sub_level_id, input_levels)?;
        let key_range = input_key_range(input_levels)?;
        Some(Self {
            target_container,
            key_range,
        })
    }
}

impl InProgressCompactInfo {
    pub fn from_compact_task(task: &CompactTask) -> Self {
        Self {
            task_id: task.task_id,
            compaction_group_id: task.compaction_group_id,
            input_levels: in_progress_input_levels(&task.input_ssts),
            target_level: task.target_level,
            target_write_scope: TargetWriteScope::from_compact_task(task),
        }
    }

    pub fn from_pb_compact_task(task: &PbCompactTask) -> Self {
        Self {
            task_id: task.task_id,
            compaction_group_id: task.compaction_group_id,
            input_levels: pb_in_progress_input_levels(&task.input_ssts),
            target_level: task.target_level,
            target_write_scope: TargetWriteScope::from_pb_compact_task(task),
        }
    }

    pub fn input_sst_ids(&self) -> Vec<HummockSstableId> {
        self.input_levels
            .iter()
            .flat_map(|level| level.sst_ids.iter().copied())
            .collect_vec()
    }
}

impl PendingInputSstIndex {
    pub fn from_tasks(tasks: impl IntoIterator<Item = RunningCompactTask>) -> Self {
        let mut index = Self::default();
        for task in tasks {
            index.add_task(task);
        }
        index
    }

    pub fn remove_task(&mut self, target_task_id: HummockCompactionTaskId) {
        for task in &self.tasks {
            if task.task_id == target_task_id {
                for sst in &task.ssts {
                    self.sst_to_task.remove(sst);
                }
            }
        }
        self.tasks.retain(|task| task.task_id != target_task_id);
    }

    pub fn contains_sst(&self, sst_id: &HummockSstableId) -> bool {
        self.sst_to_task.contains_key(sst_id)
    }

    pub fn task_id_by_sst(&self, sst_id: &HummockSstableId) -> Option<HummockCompactionTaskId> {
        self.sst_to_task.get(sst_id).cloned()
    }

    pub fn contains_any_sst_in_level(&self, level: &Level) -> bool {
        level
            .table_infos
            .iter()
            .any(|table| self.sst_to_task.contains_key(&table.sst_id))
    }

    pub fn contains_all_ssts_in_level(&self, level: &Level) -> bool {
        if level.table_infos.is_empty() {
            return false;
        }

        level
            .table_infos
            .iter()
            .all(|table| self.sst_to_task.contains_key(&table.sst_id))
    }

    pub fn add_pending_task<'a>(
        &mut self,
        task_id: HummockCompactionTaskId,
        target_level: u32,
        ssts: impl IntoIterator<Item = &'a SstableInfo>,
    ) {
        let mut sst_ids = vec![];
        let mut total_file_size = 0;
        for sst in ssts {
            total_file_size += sst.sst_size;
            sst_ids.push(sst.sst_id);
        }

        self.add_task(RunningCompactTask {
            task_id,
            target_level,
            total_file_size,
            ssts: sst_ids,
        });
    }

    pub fn pending_file_count(&self) -> usize {
        self.sst_to_task.len()
    }

    pub fn pending_file_size(&self) -> u64 {
        self.tasks
            .iter()
            .map(|task| task.total_file_size)
            .sum::<u64>()
    }

    pub fn pending_output_file_size(&self, target_level: u32) -> u64 {
        self.tasks
            .iter()
            .filter(|task| task.target_level == target_level)
            .map(|task| task.total_file_size)
            .sum::<u64>()
    }

    pub fn task_ids(&self) -> Vec<HummockCompactionTaskId> {
        self.tasks.iter().map(|task| task.task_id).collect_vec()
    }

    pub fn tasks(&self) -> &[RunningCompactTask] {
        &self.tasks
    }

    pub fn sst_to_task(&self) -> &HashMap<HummockSstableId, HummockCompactionTaskId> {
        &self.sst_to_task
    }

    #[cfg(test)]
    pub(crate) fn test_add_pending_sst(
        &mut self,
        sst_id: HummockSstableId,
        task_id: HummockCompactionTaskId,
    ) {
        self.sst_to_task.insert(sst_id, task_id);
    }

    fn add_task(&mut self, task: RunningCompactTask) {
        for sst in &task.ssts {
            self.sst_to_task.insert(*sst, task.task_id);
        }
        self.tasks.push(task);
    }
}

pub fn in_progress_compact_infos_for_group<'a>(
    assignments: impl IntoIterator<Item = &'a PbCompactTaskAssignment>,
    compaction_group_id: CompactionGroupId,
) -> Vec<InProgressCompactInfo> {
    assignments
        .into_iter()
        .filter_map(|assignment| assignment.compact_task.as_ref())
        .filter(|task| task.compaction_group_id == compaction_group_id)
        .map(InProgressCompactInfo::from_pb_compact_task)
        .collect_vec()
}

fn target_container(
    target_level: u32,
    target_sub_level_id: u64,
    input_levels: &[InputLevel],
) -> Option<TargetContainer> {
    if target_level > 0 {
        return Some(TargetContainer::Level(target_level));
    }

    let writes_non_overlapping_l0 = input_levels
        .iter()
        .any(|level| !level.table_infos.is_empty())
        && input_levels.iter().all(|level| {
            level.level_idx == 0 && matches!(level.level_type, LevelType::Nonoverlapping)
        });

    writes_non_overlapping_l0.then_some(TargetContainer::L0SubLevel(target_sub_level_id))
}

fn pb_target_container(
    target_level: u32,
    target_sub_level_id: u64,
    input_levels: &[PbInputLevel],
) -> Option<TargetContainer> {
    if target_level > 0 {
        return Some(TargetContainer::Level(target_level));
    }

    let writes_non_overlapping_l0 = input_levels
        .iter()
        .any(|level| !level.table_infos.is_empty())
        && input_levels.iter().all(|level| {
            level.level_idx == 0
                && matches!(
                    LevelType::try_from(level.level_type),
                    Ok(LevelType::Nonoverlapping)
                )
        });

    writes_non_overlapping_l0.then_some(TargetContainer::L0SubLevel(target_sub_level_id))
}

fn input_key_range(input_levels: &[InputLevel]) -> Option<KeyRange> {
    let mut range: Option<KeyRange> = None;
    for sst in input_levels.iter().flat_map(|level| &level.table_infos) {
        match range.as_mut() {
            Some(range) => range.full_key_extend(&sst.key_range),
            None => range = Some(sst.key_range.clone()),
        }
    }
    range
}

fn in_progress_input_levels(input_levels: &[InputLevel]) -> Vec<InProgressInputLevel> {
    input_levels
        .iter()
        .map(|level| InProgressInputLevel {
            level_idx: level.level_idx,
            sst_ids: level.table_infos.iter().map(|sst| sst.sst_id).collect_vec(),
            total_file_size: level.table_infos.iter().map(|sst| sst.sst_size).sum(),
        })
        .collect_vec()
}

fn pb_input_key_range(input_levels: &[PbInputLevel]) -> Option<KeyRange> {
    let mut range: Option<KeyRange> = None;
    for sst in input_levels.iter().flat_map(|level| &level.table_infos) {
        let key_range = sst
            .key_range
            .as_ref()
            .map(|key_range| KeyRange {
                left: key_range.left.clone().into(),
                right: key_range.right.clone().into(),
                right_exclusive: key_range.right_exclusive,
            })
            .unwrap_or_else(KeyRange::inf);
        match range.as_mut() {
            Some(range) => range.full_key_extend(&key_range),
            None => range = Some(key_range),
        }
    }
    range
}

fn pb_in_progress_input_levels(input_levels: &[PbInputLevel]) -> Vec<InProgressInputLevel> {
    input_levels
        .iter()
        .map(|level| InProgressInputLevel {
            level_idx: level.level_idx,
            sst_ids: level.table_infos.iter().map(|sst| sst.sst_id).collect_vec(),
            total_file_size: level.table_infos.iter().map(|sst| sst.sst_size).sum(),
        })
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_hummock_sdk::HummockSstableId;
    use risingwave_hummock_sdk::compact_task::CompactTask;
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::level::InputLevel;
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
    use risingwave_pb::hummock::LevelType;

    use super::{
        InProgressCompactInfo, PendingInputSstIndex, TargetContainer, TargetWriteScope,
        in_progress_compact_infos_for_group,
    };

    fn sst(sst_id: u64, left: usize, right: usize) -> SstableInfo {
        SstableInfoInner {
            object_id: sst_id.into(),
            sst_id: sst_id.into(),
            sst_size: (right - left) as u64,
            key_range: KeyRange {
                left: Bytes::from(crate::hummock::test_utils::iterator_test_key_of_epoch(
                    sst_id, left, 1,
                )),
                right: Bytes::from(crate::hummock::test_utils::iterator_test_key_of_epoch(
                    sst_id, right, 1,
                )),
                right_exclusive: false,
            },
            ..Default::default()
        }
        .into()
    }

    fn test_key(table: u64, idx: usize) -> Bytes {
        Bytes::from(crate::hummock::test_utils::iterator_test_key_of_epoch(
            table, idx, 1,
        ))
    }

    fn input_level(
        level_idx: u32,
        level_type: LevelType,
        table_infos: Vec<SstableInfo>,
    ) -> InputLevel {
        InputLevel {
            level_idx,
            level_type,
            table_infos,
        }
    }

    #[test]
    fn compaction_input_scope_for_non_l0_target() {
        let scope = TargetWriteScope::from_input_levels(
            4,
            0,
            &[
                input_level(0, LevelType::Nonoverlapping, vec![sst(2, 10, 20)]),
                input_level(4, LevelType::Nonoverlapping, vec![sst(1, 1, 5)]),
            ],
        )
        .unwrap();

        assert_eq!(scope.target_container, TargetContainer::Level(4));
        assert_eq!(scope.key_range.left, test_key(1, 1));
        assert_eq!(scope.key_range.right, test_key(2, 20));
    }

    #[test]
    fn compaction_input_scope_for_non_overlapping_l0_target() {
        let scope = TargetWriteScope::from_input_levels(
            0,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(1, 2, 4), sst(2, 6, 8)],
            )],
        )
        .unwrap();

        assert_eq!(scope.target_container, TargetContainer::L0SubLevel(0));
        assert_eq!(scope.key_range.left, test_key(1, 2));
        assert_eq!(scope.key_range.right, test_key(2, 8));
    }

    #[test]
    fn compaction_input_scope_skips_overlapping_l0_target() {
        assert!(
            TargetWriteScope::from_input_levels(
                0,
                42,
                &[input_level(0, LevelType::Overlapping, vec![sst(1, 2, 4)])]
            )
            .is_none()
        );
    }

    #[test]
    fn in_progress_info_uses_full_compact_task() {
        let task = CompactTask {
            task_id: 7,
            compaction_group_id: 9.into(),
            target_level: 3,
            input_ssts: vec![input_level(
                2,
                LevelType::Nonoverlapping,
                vec![sst(1, 3, 5)],
            )],
            ..Default::default()
        };

        let info = InProgressCompactInfo::from_compact_task(&task);

        assert_eq!(info.task_id, 7);
        assert_eq!(info.compaction_group_id, 9);
        assert_eq!(info.target_level, 3);
        assert_eq!(info.input_sst_ids(), vec![HummockSstableId::new(1)]);
        assert_eq!(info.input_levels.len(), 1);
        assert_eq!(info.input_levels[0].level_idx, 2);
        assert_eq!(
            info.target_write_scope,
            Some(TargetWriteScope {
                target_container: TargetContainer::Level(3),
                key_range: KeyRange {
                    left: test_key(1, 3),
                    right: test_key(1, 5),
                    right_exclusive: false,
                },
            })
        );
    }

    #[test]
    fn in_progress_info_keeps_input_ssts_without_target_scope() {
        let task = CompactTask {
            task_id: 7,
            compaction_group_id: 9.into(),
            target_level: 0,
            target_sub_level_id: 42,
            input_ssts: vec![input_level(0, LevelType::Overlapping, vec![sst(1, 3, 5)])],
            ..Default::default()
        };

        let info = InProgressCompactInfo::from_compact_task(&task);

        assert_eq!(info.input_sst_ids(), vec![HummockSstableId::new(1)]);
        assert!(info.target_write_scope.is_none());
    }

    #[test]
    fn assignment_projection_filters_by_group() {
        let group_9_task = CompactTask {
            task_id: 7,
            compaction_group_id: 9.into(),
            target_level: 3,
            input_ssts: vec![input_level(
                2,
                LevelType::Nonoverlapping,
                vec![sst(1, 3, 5)],
            )],
            ..Default::default()
        };
        let group_10_task = CompactTask {
            task_id: 8,
            compaction_group_id: 10.into(),
            target_level: 3,
            input_ssts: vec![input_level(
                2,
                LevelType::Nonoverlapping,
                vec![sst(2, 6, 8)],
            )],
            ..Default::default()
        };
        let assignments = vec![
            risingwave_pb::hummock::CompactTaskAssignment {
                compact_task: Some(group_9_task.clone().into()),
                context_id: 1.into(),
            },
            risingwave_pb::hummock::CompactTaskAssignment {
                compact_task: Some(group_10_task.into()),
                context_id: 1.into(),
            },
        ];

        let infos = in_progress_compact_infos_for_group(&assignments, 9.into());

        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].task_id, group_9_task.task_id);
        assert_eq!(
            infos[0].compaction_group_id,
            group_9_task.compaction_group_id
        );
        assert_eq!(infos[0].input_sst_ids(), vec![HummockSstableId::new(1)]);
        assert!(infos[0].target_write_scope.is_some());
    }

    #[test]
    fn pending_input_sst_index_keeps_task_list_and_fast_index_in_sync() {
        let mut index = PendingInputSstIndex::default();
        index.add_pending_task(11, 4, [&sst(1, 1, 2), &sst(2, 3, 7)]);

        assert!(index.contains_sst(&HummockSstableId::new(1)));
        assert_eq!(index.task_id_by_sst(&HummockSstableId::new(2)), Some(11));
        assert_eq!(index.pending_file_count(), 2);
        assert_eq!(index.pending_file_size(), 5);
        assert_eq!(index.pending_output_file_size(4), 5);
        assert_eq!(index.task_ids(), vec![11]);
        assert_eq!(index.tasks()[0].target_level, 4);

        index.remove_task(11);

        assert!(!index.contains_sst(&HummockSstableId::new(1)));
        assert!(index.tasks().is_empty());
        assert_eq!(index.pending_file_count(), 0);
    }
}
