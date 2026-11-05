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

use std::cmp::Ordering;
use std::collections::HashMap;

use risingwave_hummock_sdk::compact_task::CompactTaskAssignment;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::level::InputLevel;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId};

use crate::hummock::compaction::picker::CompactionInput;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum TargetContainer {
    Level(u32),
    L0SubLevel(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompactionTargetRange {
    target_container: TargetContainer,
    key_range: KeyRange,
}

impl CompactionTargetRange {
    fn from_input_levels(
        target_level: u32,
        target_sub_level_id: u64,
        input_levels: &[InputLevel],
    ) -> Option<Self> {
        let key_range = input_key_range(input_levels)?;
        Some(Self {
            target_container: target_container(target_level, target_sub_level_id),
            key_range,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InProgressTargetRange {
    task_id: HummockCompactionTaskId,
    key_range: KeyRange,
}

#[derive(Default)]
pub struct InProgressCompactionView {
    ranges_by_target_container: HashMap<TargetContainer, Vec<InProgressTargetRange>>,
}

impl InProgressCompactionView {
    pub(crate) fn for_group<'a>(
        assignments: impl IntoIterator<Item = &'a CompactTaskAssignment>,
        compaction_group_id: CompactionGroupId,
    ) -> Self {
        let mut view = Self::default();
        for assignment in assignments {
            let task = &assignment.compact_task;
            if task.compaction_group_id != compaction_group_id {
                continue;
            }

            let Some(target_range) = CompactionTargetRange::from_input_levels(
                task.target_level,
                task.target_sub_level_id,
                &task.input_ssts,
            ) else {
                continue;
            };
            view.ranges_by_target_container
                .entry(target_range.target_container)
                .or_default()
                .push(InProgressTargetRange {
                    task_id: task.task_id,
                    key_range: target_range.key_range,
                });
        }

        // Finalize each target bucket for binary-search based overlap checks.
        for ranges in view.ranges_by_target_container.values_mut() {
            ranges.sort_by(|left, right| left.key_range.cmp(&right.key_range));
            assert!(
                ranges
                    .windows(2)
                    .all(|pair| !pair[0].key_range.sstable_overlap(&pair[1].key_range)),
                "in-progress target ranges must not overlap within the same target container"
            );
        }

        view
    }

    pub(crate) fn has_conflict_with_input(&self, input: &CompactionInput) -> bool {
        let target_range = CompactionTargetRange::from_input_levels(
            input.target_level as u32,
            input.target_sub_level_id,
            &input.input_levels,
        );
        target_range
            .as_ref()
            .and_then(|target_range| self.conflict_target_range(target_range))
            .is_some()
    }

    fn conflict_target_range(
        &self,
        target_range: &CompactionTargetRange,
    ) -> Option<HummockCompactionTaskId> {
        let ranges = self
            .ranges_by_target_container
            .get(&target_range.target_container)?;
        // Follow the range overlap strategy: `compare_right_with` skips ranges that end before the
        // candidate, and `sstable_overlap` performs the final right-exclusive-aware check. Ranges
        // in the same target container are non-overlapping, so the first remaining range is enough.
        let pos = ranges.partition_point(|range| {
            range
                .key_range
                .compare_right_with(&target_range.key_range.left)
                == Ordering::Less
        });

        ranges.get(pos).and_then(|range| {
            target_range
                .key_range
                .sstable_overlap(&range.key_range)
                .then_some(range.task_id)
        })
    }
}

fn target_container(target_level: u32, target_sub_level_id: u64) -> TargetContainer {
    match target_level {
        0 => TargetContainer::L0SubLevel(target_sub_level_id),
        level => TargetContainer::Level(level),
    }
}

fn input_key_range(input_levels: &[InputLevel]) -> Option<KeyRange> {
    let mut range: Option<KeyRange> = None;
    for level in input_levels {
        for sst in &level.table_infos {
            assert!(
                !sst.key_range.inf_key_range(),
                "input SST key range for pending compaction target range must be finite: level_idx={}, sst_id={}",
                level.level_idx,
                sst.sst_id,
            );
            match range.as_mut() {
                Some(range) => range.full_key_extend(&sst.key_range),
                None => range = Some(sst.key_range.clone()),
            }
        }
    }
    range
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_hummock_sdk::compact_task::{CompactTask, CompactTaskAssignment};
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
    use risingwave_pb::hummock::LevelType;

    use super::*;

    fn sst(sst_id: u64, left: usize, right: usize) -> SstableInfo {
        SstableInfoInner {
            object_id: sst_id.into(),
            sst_id: sst_id.into(),
            sst_size: (right - left) as u64,
            key_range: KeyRange {
                left: Bytes::from(crate::hummock::test_utils::iterator_test_key_of_epoch(
                    1, left, 1,
                )),
                right: Bytes::from(crate::hummock::test_utils::iterator_test_key_of_epoch(
                    1, right, 1,
                )),
                right_exclusive: false,
            },
            ..Default::default()
        }
        .into()
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

    fn compact_task(
        task_id: u64,
        compaction_group_id: u64,
        target_level: u32,
        target_sub_level_id: u64,
        input_ssts: Vec<InputLevel>,
    ) -> CompactTask {
        CompactTask {
            task_id,
            compaction_group_id: compaction_group_id.into(),
            target_level,
            target_sub_level_id,
            input_ssts,
            ..Default::default()
        }
    }

    fn compaction_input(
        target_level: usize,
        target_sub_level_id: u64,
        input_levels: Vec<InputLevel>,
    ) -> CompactionInput {
        CompactionInput {
            input_levels,
            target_level,
            target_sub_level_id,
            ..Default::default()
        }
    }

    fn assignment(task: CompactTask) -> CompactTaskAssignment {
        CompactTaskAssignment {
            compact_task: task,
            context_id: 1.into(),
        }
    }

    fn view_for_group(
        tasks: Vec<CompactTask>,
        compaction_group_id: u64,
    ) -> InProgressCompactionView {
        let assignments: Vec<_> = tasks.into_iter().map(assignment).collect();
        InProgressCompactionView::for_group(&assignments, compaction_group_id.into())
    }

    #[test]
    fn target_range_uses_apply_target_container() {
        for (target_level, target_sub_level_id, input_levels, expected_container) in [
            (
                4,
                0,
                vec![
                    input_level(0, LevelType::Nonoverlapping, vec![sst(2, 10, 20)]),
                    input_level(4, LevelType::Nonoverlapping, vec![sst(1, 1, 5)]),
                ],
                TargetContainer::Level(4),
            ),
            (
                0,
                42,
                vec![input_level(
                    0,
                    LevelType::Nonoverlapping,
                    vec![sst(1, 2, 4)],
                )],
                TargetContainer::L0SubLevel(42),
            ),
            (
                0,
                42,
                vec![input_level(0, LevelType::Overlapping, vec![sst(1, 2, 4)])],
                TargetContainer::L0SubLevel(42),
            ),
            (
                0,
                42,
                vec![
                    input_level(0, LevelType::Nonoverlapping, vec![sst(1, 2, 4)]),
                    input_level(0, LevelType::Overlapping, vec![sst(2, 5, 7)]),
                ],
                TargetContainer::L0SubLevel(42),
            ),
        ] {
            let range = CompactionTargetRange::from_input_levels(
                target_level,
                target_sub_level_id,
                &input_levels,
            )
            .unwrap();

            assert_eq!(range.target_container, expected_container);
        }
    }

    #[test]
    #[should_panic(
        expected = "input SST key range for pending compaction target range must be finite"
    )]
    fn target_range_rejects_infinite_input_sst_key_range() {
        let _ = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![
                    SstableInfoInner {
                        object_id: 1.into(),
                        sst_id: 1.into(),
                        key_range: KeyRange::inf(),
                        ..Default::default()
                    }
                    .into(),
                ],
            )],
        );
    }

    #[test]
    fn target_conflict_requires_same_container_and_overlapping_key_range() {
        let in_progress = view_for_group(
            vec![compact_task(
                7,
                9,
                4,
                0,
                vec![input_level(
                    0,
                    LevelType::Nonoverlapping,
                    vec![sst(1, 10, 20)],
                )],
            )],
            9,
        );
        let overlapping = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(2, 15, 25)],
            )],
        );
        let different_level = CompactionTargetRange::from_input_levels(
            5,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(3, 15, 25)],
            )],
        );
        let disjoint = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(4, 30, 40)],
            )],
        );
        let touching_right_boundary = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(5, 20, 30)],
            )],
        );

        assert_eq!(
            in_progress.conflict_target_range(&overlapping.unwrap()),
            Some(7)
        );
        assert_eq!(
            in_progress.conflict_target_range(&different_level.unwrap()),
            None
        );
        assert_eq!(in_progress.conflict_target_range(&disjoint.unwrap()), None);
        assert_eq!(
            in_progress.conflict_target_range(&touching_right_boundary.unwrap()),
            Some(7)
        );
    }

    #[test]
    fn target_conflict_from_input_ignores_skip_policy() {
        let in_progress = view_for_group(
            vec![compact_task(
                7,
                9,
                4,
                0,
                vec![input_level(
                    0,
                    LevelType::Nonoverlapping,
                    vec![sst(1, 10, 20)],
                )],
            )],
            9,
        );
        let mut input = compaction_input(
            4,
            0,
            vec![input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(2, 15, 25)],
            )],
        );

        assert!(in_progress.has_conflict_with_input(&input));
        input.skip_target_range_conflict_check = true;

        assert!(in_progress.has_conflict_with_input(&input));
    }

    #[test]
    fn target_conflict_respects_right_exclusive_boundary() {
        let in_progress = view_for_group(
            vec![compact_task(
                7,
                9,
                4,
                0,
                vec![input_level(
                    0,
                    LevelType::Nonoverlapping,
                    vec![
                        SstableInfoInner {
                            object_id: 1.into(),
                            sst_id: 1.into(),
                            sst_size: 10,
                            key_range: KeyRange {
                                left: Bytes::from(
                                    crate::hummock::test_utils::iterator_test_key_of_epoch(
                                        1, 10, 1,
                                    ),
                                ),
                                right: Bytes::from(
                                    crate::hummock::test_utils::iterator_test_key_of_epoch(
                                        1, 20, 1,
                                    ),
                                ),
                                right_exclusive: true,
                            },
                            ..Default::default()
                        }
                        .into(),
                    ],
                )],
            )],
            9,
        );
        let touching_exclusive_boundary = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(2, 20, 30)],
            )],
        );

        assert_eq!(
            in_progress.conflict_target_range(&touching_exclusive_boundary.unwrap()),
            None
        );
    }

    #[test]
    fn target_conflict_uses_input_key_range_envelope() {
        let in_progress = view_for_group(
            vec![compact_task(
                7,
                9,
                4,
                0,
                vec![input_level(
                    0,
                    LevelType::Nonoverlapping,
                    vec![sst(1, 10, 20), sst(2, 40, 50)],
                )],
            )],
            9,
        );
        let gap_inside_input_envelope = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(3, 25, 35)],
            )],
        );

        assert_eq!(
            in_progress.conflict_target_range(&gap_inside_input_envelope.unwrap()),
            Some(7)
        );
    }

    #[test]
    fn target_conflict_finds_later_range_in_same_container() {
        let in_progress = view_for_group(
            vec![
                compact_task(
                    7,
                    9,
                    4,
                    0,
                    vec![input_level(
                        0,
                        LevelType::Nonoverlapping,
                        vec![sst(1, 10, 20)],
                    )],
                ),
                compact_task(
                    8,
                    9,
                    4,
                    0,
                    vec![input_level(
                        0,
                        LevelType::Nonoverlapping,
                        vec![sst(2, 40, 50)],
                    )],
                ),
                compact_task(
                    9,
                    9,
                    4,
                    0,
                    vec![input_level(
                        0,
                        LevelType::Nonoverlapping,
                        vec![sst(3, 70, 80)],
                    )],
                ),
            ],
            9,
        );
        let overlapping_later = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(4, 45, 46)],
            )],
        );
        let disjoint_gap = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(5, 25, 35)],
            )],
        );

        assert_eq!(
            in_progress.conflict_target_range(&overlapping_later.unwrap()),
            Some(8)
        );
        assert_eq!(
            in_progress.conflict_target_range(&disjoint_gap.unwrap()),
            None
        );
    }

    #[test]
    fn target_conflict_checks_non_overlapping_l0_sub_level_container() {
        let in_progress = view_for_group(
            vec![compact_task(
                7,
                9,
                0,
                42,
                vec![input_level(
                    0,
                    LevelType::Nonoverlapping,
                    vec![sst(1, 10, 20)],
                )],
            )],
            9,
        );
        let same_sub_level = CompactionTargetRange::from_input_levels(
            0,
            42,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(2, 15, 25)],
            )],
        );
        let different_sub_level = CompactionTargetRange::from_input_levels(
            0,
            43,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(3, 15, 25)],
            )],
        );

        assert_eq!(
            in_progress.conflict_target_range(&same_sub_level.unwrap()),
            Some(7)
        );
        assert_eq!(
            in_progress.conflict_target_range(&different_sub_level.unwrap()),
            None
        );
    }

    #[test]
    #[should_panic(expected = "in-progress target ranges must not overlap")]
    fn assignment_projection_rejects_overlapping_target_ranges() {
        let assignments = vec![
            assignment(compact_task(
                7,
                9,
                4,
                0,
                vec![input_level(
                    0,
                    LevelType::Nonoverlapping,
                    vec![sst(1, 10, 20)],
                )],
            )),
            assignment(compact_task(
                8,
                9,
                4,
                0,
                vec![input_level(
                    0,
                    LevelType::Nonoverlapping,
                    vec![sst(2, 15, 25)],
                )],
            )),
        ];

        let _ = InProgressCompactionView::for_group(&assignments, 9.into());
    }

    #[test]
    fn assignment_projection_filters_by_compaction_group_and_target_range() {
        let group_9_task = compact_task(
            7,
            9,
            4,
            0,
            vec![input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(1, 10, 20)],
            )],
        );
        let group_10_task = compact_task(
            8,
            10,
            4,
            0,
            vec![input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(2, 30, 40)],
            )],
        );
        let group_9_l0_task = compact_task(
            9,
            9,
            0,
            0,
            vec![input_level(0, LevelType::Overlapping, vec![sst(3, 50, 60)])],
        );
        let assignments = vec![
            assignment(group_9_task),
            assignment(group_10_task),
            assignment(group_9_l0_task),
        ];

        let projection = InProgressCompactionView::for_group(&assignments, 9.into());
        let group_9_range = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(4, 15, 16)],
            )],
        );
        let group_10_range = CompactionTargetRange::from_input_levels(
            4,
            0,
            &[input_level(
                0,
                LevelType::Nonoverlapping,
                vec![sst(5, 35, 36)],
            )],
        );
        let group_9_l0_range = CompactionTargetRange::from_input_levels(
            0,
            0,
            &[input_level(0, LevelType::Overlapping, vec![sst(6, 55, 56)])],
        );

        assert_eq!(
            projection.conflict_target_range(&group_9_range.unwrap()),
            Some(7)
        );
        assert_eq!(
            projection.conflict_target_range(&group_10_range.unwrap()),
            None
        );
        assert_eq!(
            projection.conflict_target_range(&group_9_l0_range.unwrap()),
            Some(9)
        );
    }
}
