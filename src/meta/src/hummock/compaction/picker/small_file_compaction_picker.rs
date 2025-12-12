// Copyright 2025 RisingWave Labs
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

use bytes::Bytes;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::level::{InputLevel, Levels};
use risingwave_hummock_sdk::sstable_info::SstableInfo;

use crate::hummock::compaction::picker::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct SmallFileCompactionPicker {
    small_file_threshold: u64,
}

#[derive(Default)]
pub struct SmallFilePickerState {
    // record the end_bound that has been scanned
    pub last_select_end_bound: KeyRange,

    // record the end_bound in the current round of scanning tasks
    pub end_bound_in_round: KeyRange,
}

impl SmallFilePickerState {
    pub fn valid(&self) -> bool {
        !self.end_bound_in_round.right.is_empty()
    }

    pub fn init(&mut self, key_range: KeyRange) {
        self.last_select_end_bound = KeyRange {
            left: Bytes::default(),
            right: key_range.left.clone(),
            right_exclusive: true,
        };
        self.end_bound_in_round = key_range;
    }

    pub fn clear(&mut self) {
        self.end_bound_in_round = KeyRange::default();
        self.last_select_end_bound = KeyRange::default();
    }

    /// Skip current SST by updating `end_bound` to its right boundary
    pub fn skip_to_sst_right(&mut self, sst: &SstableInfo) {
        self.last_select_end_bound.full_key_extend(&KeyRange {
            left: Bytes::default(),
            right: sst.key_range.right.clone(),
            right_exclusive: sst.key_range.right_exclusive,
        });
    }

    /// Reset selection and exclude current SST from future selection
    /// Used when encountering interrupting files: pending, large, or cross-vnode files
    pub fn reset_and_exclude_current(
        &mut self,
        sst: &SstableInfo,
        select_input_ssts: &mut Vec<SstableInfo>,
        target_vnode_id: &mut Option<usize>,
    ) {
        select_input_ssts.clear();
        *target_vnode_id = None;
        self.skip_to_sst_right(sst);
    }

    /// Reset selection and restart from current SST as first file of new vnode group
    /// Used when encountering vnode boundary with insufficient files collected
    pub fn reset_and_restart_from_current(
        &mut self,
        sst: &SstableInfo,
        select_input_ssts: &mut Vec<SstableInfo>,
        target_vnode_id: &mut Option<usize>,
        new_vnode: usize,
    ) {
        select_input_ssts.clear();
        *target_vnode_id = Some(new_vnode);
        // Mark boundary before current SST to include it in next iteration
        self.last_select_end_bound.full_key_extend(&KeyRange {
            left: Bytes::default(),
            right: sst.key_range.left.clone(),
            right_exclusive: false,
        });
    }
}

impl SmallFileCompactionPicker {
    pub fn new(small_file_threshold: u64) -> Self {
        Self {
            small_file_threshold,
        }
    }

    pub fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        state: &mut SmallFilePickerState,
    ) -> Option<CompactionInput> {
        assert!(!levels.levels.is_empty());

        // Only scan the last level where 90%+ data resides
        let compaction_level = levels.levels.last().unwrap();
        let level_handler = &level_handlers[compaction_level.level_idx as usize];

        if compaction_level.table_infos.is_empty() {
            state.clear();
            return None;
        }

        if state.valid()
            && state
                .last_select_end_bound
                .compare_right_with(&state.end_bound_in_round.right)
                == std::cmp::Ordering::Greater
        {
            // In round but end_key overflow, turn to next round
            state.clear();
            return None;
        }

        if !state.valid() {
            // New round init key_range bound with table_infos
            let first_sst = compaction_level.table_infos.first().unwrap();
            let last_sst = compaction_level.table_infos.last().unwrap();

            let key_range_this_round = KeyRange {
                left: first_sst.key_range.left.clone(),
                right: last_sst.key_range.right.clone(),
                right_exclusive: last_sst.key_range.right_exclusive,
            };

            state.init(key_range_this_round);
        }

        // Strategy 1: Greedy selection - select all consecutive small files
        // Strategy 2: Minimum 3 files requirement
        // Strategy 3: Don't cross vnode boundaries
        const MIN_FILE_COUNT: usize = 3;
        let mut select_input_ssts = vec![];
        let mut target_vnode_id: Option<usize> = None;

        for sst in &compaction_level.table_infos {
            // Skip already scanned files
            let unmatched_sst = sst.key_range.sstable_overlap(&state.last_select_end_bound);
            if unmatched_sst {
                continue;
            }

            // Check if pending
            if level_handler.is_pending_compact(&sst.sst_id) {
                if select_input_ssts.len() >= MIN_FILE_COUNT {
                    break;
                }
                state.reset_and_exclude_current(sst, &mut select_input_ssts, &mut target_vnode_id);
                continue;
            }

            // Check if small file
            let is_small_file = sst.sst_size < self.small_file_threshold;
            if !is_small_file {
                if select_input_ssts.len() >= MIN_FILE_COUNT {
                    break;
                }
                state.reset_and_exclude_current(sst, &mut select_input_ssts, &mut target_vnode_id);
                continue;
            }

            // Strategy 3: Check vnode boundary
            if !sst.key_range.left.is_empty() && !sst.key_range.right.is_empty() {
                use risingwave_hummock_sdk::key::UserKey;
                let left_vnode = UserKey::decode(&sst.key_range.left).get_vnode_id();
                let right_vnode = UserKey::decode(&sst.key_range.right).get_vnode_id();

                // Skip SSTs that span multiple vnodes
                if left_vnode != right_vnode {
                    if select_input_ssts.len() >= MIN_FILE_COUNT {
                        break;
                    }
                    state.reset_and_exclude_current(
                        sst,
                        &mut select_input_ssts,
                        &mut target_vnode_id,
                    );
                    continue;
                }

                // Check if this SST has a different vnode from our target
                if let Some(target_vnode) = target_vnode_id {
                    if right_vnode != target_vnode {
                        // Found a vnode boundary - finish current selection
                        if select_input_ssts.len() >= MIN_FILE_COUNT {
                            break;
                        }
                        state.reset_and_restart_from_current(
                            sst,
                            &mut select_input_ssts,
                            &mut target_vnode_id,
                            right_vnode,
                        );
                        // Continue with current SST as first file of new vnode
                    }
                } else {
                    target_vnode_id = Some(right_vnode);
                }
            }

            select_input_ssts.push(sst.clone());
        }

        // Strategy 2: Require at least 3 files
        if select_input_ssts.len() < MIN_FILE_COUNT {
            // Turn to next round
            state.clear();
            return None;
        }

        // Successfully selected enough files, update end_bound for next round
        let select_last_sst = select_input_ssts.last().unwrap();
        state.skip_to_sst_right(select_last_sst);

        Some(CompactionInput {
            select_input_size: select_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            total_file_count: select_input_ssts.len() as _,
            input_levels: vec![
                InputLevel {
                    level_idx: compaction_level.level_idx,
                    level_type: compaction_level.level_type,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: compaction_level.level_idx,
                    level_type: compaction_level.level_type,
                    table_infos: vec![],
                },
            ],
            target_level: compaction_level.level_idx as usize,
            ..Default::default()
        })
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_hummock_sdk::level::Level;
    use risingwave_pb::hummock::LevelType;

    use super::*;
    use crate::hummock::compaction::selector::tests::generate_table_impl;

    #[test]
    fn test_greedy_selection_of_consecutive_small_files() {
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };
        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        // Test case 1: 3 small files (minimum requirement met)
        let mut sst1 = generate_table_impl(1, 1, 1, 100, 1);
        sst1.sst_size = 30 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst1.into());

        let mut sst2 = generate_table_impl(2, 1, 101, 200, 1);
        sst2.sst_size = 25 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst2.into());

        let mut sst3 = generate_table_impl(3, 1, 201, 300, 1);
        sst3.sst_size = 20 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst3.into());

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();

        // Should greedily select all 3 consecutive small files
        assert_eq!(2, ret.input_levels.len());
        assert_eq!(3, ret.input_levels[0].table_infos.len());
        assert_eq!(1, ret.input_levels[0].table_infos[0].sst_id);
        assert_eq!(2, ret.input_levels[0].table_infos[1].sst_id);
        assert_eq!(3, ret.input_levels[0].table_infos[2].sst_id);
        assert_eq!(0, ret.input_levels[1].table_infos.len()); // Empty target
        assert_eq!(75 * 1024 * 1024, ret.select_input_size); // Total size
    }

    #[test]
    fn test_minimum_file_count_requirement() {
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Add only 2 small files - should not trigger compaction
        let mut sst1 = generate_table_impl(1, 1, 1, 100, 1);
        sst1.sst_size = 20 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst1.into());

        let mut sst2 = generate_table_impl(2, 1, 101, 200, 1);
        sst2.sst_size = 30 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst2.into());

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        // Should return None because less than 3 files
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(ret.is_none());

        // Verify state is completely cleared (regression test for incorrect order bug)
        assert!(
            !state.valid(),
            "State should be cleared when insufficient files"
        );
        assert!(
            state.last_select_end_bound.right.is_empty(),
            "last_select_end_bound should not be updated when files are insufficient"
        );

        // Second call should start from beginning (not skip any files)
        let ret2 = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(ret2.is_none());
    }

    #[test]
    fn test_skip_pending_compact() {
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Add 4 small files
        let mut sst1 = generate_table_impl(1, 1, 1, 100, 1);
        sst1.sst_size = 20 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst1.into());

        let mut sst2 = generate_table_impl(2, 1, 101, 200, 1);
        sst2.sst_size = 30 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst2.into());

        let mut sst3 = generate_table_impl(3, 1, 201, 300, 1);
        sst3.sst_size = 25 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst3.into());

        let mut sst4 = generate_table_impl(4, 1, 301, 400, 1);
        sst4.sst_size = 15 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst4.into());

        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];

        // Mark first file as pending - should skip it and start from file 2
        levels_handler[6].add_pending_task(999, 6, vec![&levels.levels[0].table_infos[0]]);

        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();

        // Should select files 2, 3, 4 (skipping pending file 1)
        assert_eq!(3, ret.input_levels[0].table_infos.len());
        assert_eq!(2, ret.input_levels[0].table_infos[0].sst_id);
        assert_eq!(3, ret.input_levels[0].table_infos[1].sst_id);
        assert_eq!(4, ret.input_levels[0].table_infos[2].sst_id);
    }

    #[test]
    fn test_pending_file_interrupts_sequence() {
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Add 2 small files, then a pending file, then 3 more small files
        let mut sst1 = generate_table_impl(1, 1, 1, 100, 1);
        sst1.sst_size = 20 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst1.into());

        let mut sst2 = generate_table_impl(2, 1, 101, 200, 1);
        sst2.sst_size = 25 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst2.into());

        let mut sst3 = generate_table_impl(3, 1, 201, 300, 1);
        sst3.sst_size = 30 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst3.into());

        let mut sst4 = generate_table_impl(4, 1, 301, 400, 1);
        sst4.sst_size = 20 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst4.into());

        let mut sst5 = generate_table_impl(5, 1, 401, 500, 1);
        sst5.sst_size = 25 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst5.into());

        let mut sst6 = generate_table_impl(6, 1, 501, 600, 1);
        sst6.sst_size = 30 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst6.into());

        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];

        // Mark file 3 (in the middle) as pending
        levels_handler[6].add_pending_task(999, 6, vec![&levels.levels[0].table_infos[2]]);

        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();

        // Should skip files 1, 2 (insufficient, interrupted by pending file 3)
        // Should select files 4, 5, 6 (after pending file)
        assert_eq!(3, ret.input_levels[0].table_infos.len());
        assert_eq!(4, ret.input_levels[0].table_infos[0].sst_id);
        assert_eq!(5, ret.input_levels[0].table_infos[1].sst_id);
        assert_eq!(6, ret.input_levels[0].table_infos[2].sst_id);
    }

    #[test]
    fn test_large_file_interrupts_and_mixed_patterns() {
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Pattern: small, small, LARGE, small, small, small, LARGE, small
        let sizes = [20, 30, 100, 25, 30, 20, 80, 15];
        for (i, &size) in sizes.iter().enumerate() {
            let mut sst = generate_table_impl((i + 1) as u64, 1, i * 100 + 1, (i + 1) * 100, 1);
            sst.sst_size = size * 1024 * 1024;
            levels.levels[0].table_infos.push(sst.into());
        }

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();

        // Should skip first 2 small files (< MIN_FILE_COUNT before large file #3)
        // Should select files 4, 5, 6 after first large file (before second large file #7)
        assert_eq!(3, ret.input_levels[0].table_infos.len());
        assert_eq!(4, ret.input_levels[0].table_infos[0].sst_id);
        assert_eq!(5, ret.input_levels[0].table_infos[1].sst_id);
        assert_eq!(6, ret.input_levels[0].table_infos[2].sst_id);

        // Second call should skip the large file and find no more valid groups
        let ret2 = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(ret2.is_none()); // Only 1 small file left after large file #7
    }

    #[test]
    fn test_cross_vnode_sst_skipped() {
        use risingwave_common::hash::VirtualNode;
        use risingwave_hummock_sdk::key::{key_with_epoch, prefix_slice_with_vnode};
        use risingwave_hummock_sdk::sstable_info::SstableInfoInner;

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Create an SST that spans multiple vnodes (vnode 10 -> vnode 20)
        let vnode1 = VirtualNode::from_index(10);
        let vnode2 = VirtualNode::from_index(20);
        let table_id = 1u64;

        // Left key with vnode 10
        let left_table_key = prefix_slice_with_vnode(vnode1, b"key_00001");
        let mut left_full_key = vec![];
        left_full_key.extend_from_slice(&(table_id as u32).to_be_bytes());
        left_full_key.extend_from_slice(&left_table_key);
        let left_full_key = key_with_epoch(left_full_key, 1);

        // Right key with vnode 20 (different from left!)
        let right_table_key = prefix_slice_with_vnode(vnode2, b"key_00100");
        let mut right_full_key = vec![];
        right_full_key.extend_from_slice(&(table_id as u32).to_be_bytes());
        right_full_key.extend_from_slice(&right_table_key);
        let right_full_key = key_with_epoch(right_full_key, 1);

        // Files 1-2: normal small files (vnode 10)
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(1, 1, vnode1, 1, 50, 1, 20 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(2, 1, vnode1, 51, 100, 1, 25 * 1024 * 1024).into());

        // File 3: Cross-vnode SST (spans vnode 10 to vnode 20)
        let cross_vnode_sst = SstableInfoInner {
            object_id: 3.into(),
            sst_id: 3.into(),
            key_range: KeyRange {
                left: left_full_key.into(),
                right: right_full_key.into(),
                right_exclusive: false,
            },
            file_size: 30 * 1024 * 1024,
            table_ids: vec![(table_id as u32).into()],
            uncompressed_file_size: 30 * 1024 * 1024,
            total_key_count: 100,
            sst_size: 30 * 1024 * 1024,
            ..Default::default()
        };
        levels.levels[0].table_infos.push(cross_vnode_sst.into());

        // Files 4-6: normal small files (vnode 20)
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(4, 1, vnode2, 101, 200, 1, 20 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(5, 1, vnode2, 201, 300, 1, 25 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(6, 1, vnode2, 301, 400, 1, 30 * 1024 * 1024).into());

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        // First call: should skip files 1-2 (insufficient before cross-vnode SST)
        // Should skip file 3 (cross-vnode)
        // Should select files 4-6 (vnode 20)
        let ret1 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret1.input_levels[0].table_infos.len());
        assert_eq!(4, ret1.input_levels[0].table_infos[0].sst_id);
        assert_eq!(5, ret1.input_levels[0].table_infos[1].sst_id);
        assert_eq!(6, ret1.input_levels[0].table_infos[2].sst_id);

        // Verify the critical branch: cross-vnode SST was correctly skipped
        // This validates the logic: if left_vnode != right_vnode { skip }
    }

    #[test]
    fn test_multi_round_scanning() {
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Pattern: 3 small, 1 large, 3 small - to force true multi-round scanning
        for i in [1, 2, 3] {
            let mut sst =
                generate_table_impl(i, 1, ((i - 1) * 100 + 1) as usize, (i * 100) as usize, 1);
            sst.sst_size = 20 * 1024 * 1024;
            levels.levels[0].table_infos.push(sst.into());
        }

        // Large file interruption
        let mut large_sst = generate_table_impl(4, 1, 301, 400, 1);
        large_sst.sst_size = 100 * 1024 * 1024;
        levels.levels[0].table_infos.push(large_sst.into());

        for i in [5, 6, 7] {
            let mut sst =
                generate_table_impl(i, 1, ((i - 1) * 100 + 1) as usize, (i * 100) as usize, 1);
            sst.sst_size = 20 * 1024 * 1024;
            levels.levels[0].table_infos.push(sst.into());
        }

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        // Round 1: Select first 3 small files (before large file)
        let ret1 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret1.input_levels[0].table_infos.len());
        assert_eq!(1, ret1.input_levels[0].table_infos[0].sst_id);
        assert_eq!(2, ret1.input_levels[0].table_infos[1].sst_id);
        assert_eq!(3, ret1.input_levels[0].table_infos[2].sst_id);

        // Round 2: Skip large file, select next 3 small files
        let ret2 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret2.input_levels[0].table_infos.len());
        assert_eq!(5, ret2.input_levels[0].table_infos[0].sst_id);
        assert_eq!(6, ret2.input_levels[0].table_infos[1].sst_id);
        assert_eq!(7, ret2.input_levels[0].table_infos[2].sst_id);

        // Round 3: No more files in this scanning cycle
        let ret3 = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(ret3.is_none());

        // Round 4: State should reset, pick from beginning again
        let ret4 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret4.input_levels[0].table_infos.len());
        assert_eq!(1, ret4.input_levels[0].table_infos[0].sst_id);
    }

    fn generate_sst_with_vnode(
        sst_id: u64,
        table_id: u64,
        vnode: risingwave_common::hash::VirtualNode,
        left_key: usize,
        right_key: usize,
        epoch: u64,
        sst_size: u64,
    ) -> risingwave_hummock_sdk::sstable_info::SstableInfoInner {
        use risingwave_hummock_sdk::key::{key_with_epoch, prefix_slice_with_vnode};
        use risingwave_hummock_sdk::sstable_info::SstableInfoInner;

        // Create table key with specified vnode
        let left_table_key =
            prefix_slice_with_vnode(vnode, format!("key_{:05}", left_key).as_bytes());
        let right_table_key =
            prefix_slice_with_vnode(vnode, format!("key_{:05}", right_key).as_bytes());

        // Encode as full keys with table_id and epoch
        let mut left_full_key = vec![];
        left_full_key.extend_from_slice(&(table_id as u32).to_be_bytes());
        left_full_key.extend_from_slice(&left_table_key);
        let left_full_key = key_with_epoch(left_full_key, epoch);

        let mut right_full_key = vec![];
        right_full_key.extend_from_slice(&(table_id as u32).to_be_bytes());
        right_full_key.extend_from_slice(&right_table_key);
        let right_full_key = key_with_epoch(right_full_key, epoch);

        SstableInfoInner {
            object_id: sst_id.into(),
            sst_id: sst_id.into(),
            key_range: KeyRange {
                left: left_full_key.into(),
                right: right_full_key.into(),
                right_exclusive: false,
            },
            file_size: sst_size,
            table_ids: vec![(table_id as u32).into()],
            uncompressed_file_size: sst_size,
            total_key_count: (right_key - left_key + 1) as u64,
            sst_size,
            ..Default::default()
        }
    }

    #[test]
    fn test_vnode_boundary_enforcement() {
        use risingwave_common::hash::VirtualNode;
        use risingwave_hummock_sdk::key::UserKey;

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Create small files with different vnodes
        // Files 1-3: vnode 10
        let vnode1 = VirtualNode::from_index(10);
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(1, 1, vnode1, 1, 100, 1, 20 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(2, 1, vnode1, 101, 200, 1, 25 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(3, 1, vnode1, 201, 300, 1, 30 * 1024 * 1024).into());

        // Files 4-6: vnode 20
        let vnode2 = VirtualNode::from_index(20);
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(4, 1, vnode2, 301, 400, 1, 20 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(5, 1, vnode2, 401, 500, 1, 25 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(6, 1, vnode2, 501, 600, 1, 30 * 1024 * 1024).into());

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        // First call: should select files 1-3 (vnode 10)
        let ret1 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret1.input_levels[0].table_infos.len());
        assert_eq!(1, ret1.input_levels[0].table_infos[0].sst_id);
        assert_eq!(2, ret1.input_levels[0].table_infos[1].sst_id);
        assert_eq!(3, ret1.input_levels[0].table_infos[2].sst_id);

        // Verify all selected files have the same vnode
        let first_vnode =
            UserKey::decode(&ret1.input_levels[0].table_infos[0].key_range.right).get_vnode_id();

        for sst in &ret1.input_levels[0].table_infos {
            let left_vnode = UserKey::decode(&sst.key_range.left).get_vnode_id();
            let right_vnode = UserKey::decode(&sst.key_range.right).get_vnode_id();

            // Each SST should not span vnodes
            assert_eq!(
                left_vnode, right_vnode,
                "SST {} spans multiple vnodes",
                sst.sst_id
            );

            // All SSTs in the task should have the same vnode
            assert_eq!(
                right_vnode, first_vnode,
                "SST {} has different vnode from first SST",
                sst.sst_id
            );
        }

        // Second call: should select files 4-6 (same vnode from table_prefix=2)
        let ret2 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret2.input_levels[0].table_infos.len());
        assert_eq!(4, ret2.input_levels[0].table_infos[0].sst_id);
        assert_eq!(5, ret2.input_levels[0].table_infos[1].sst_id);
        assert_eq!(6, ret2.input_levels[0].table_infos[2].sst_id);

        // Verify all selected files have the same vnode
        let second_vnode =
            UserKey::decode(&ret2.input_levels[0].table_infos[0].key_range.right).get_vnode_id();

        for sst in &ret2.input_levels[0].table_infos {
            let left_vnode = UserKey::decode(&sst.key_range.left).get_vnode_id();
            let right_vnode = UserKey::decode(&sst.key_range.right).get_vnode_id();

            assert_eq!(
                left_vnode, right_vnode,
                "SST {} spans multiple vnodes",
                sst.sst_id
            );
            assert_eq!(
                right_vnode, second_vnode,
                "SST {} has different vnode from first SST",
                sst.sst_id
            );
        }

        // Verify the two groups have different vnodes
        assert_ne!(
            first_vnode, second_vnode,
            "The two compaction groups should have different vnodes"
        );
    }

    #[test]
    fn test_skip_insufficient_vnode_group() {
        use risingwave_common::hash::VirtualNode;
        use risingwave_hummock_sdk::key::UserKey;

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Files 1-2: vnode 10 (only 2 files - insufficient)
        let vnode1 = VirtualNode::from_index(10);
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(1, 1, vnode1, 1, 100, 1, 20 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(2, 1, vnode1, 101, 200, 1, 25 * 1024 * 1024).into());

        // Files 3-5: vnode 20 (3 files - sufficient)
        let vnode2 = VirtualNode::from_index(20);
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(3, 1, vnode2, 201, 300, 1, 20 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(4, 1, vnode2, 301, 400, 1, 25 * 1024 * 1024).into());
        levels.levels[0]
            .table_infos
            .push(generate_sst_with_vnode(5, 1, vnode2, 401, 500, 1, 30 * 1024 * 1024).into());

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        // First call: should skip files 1-2 (insufficient) and select files 3-5 (vnode 20)
        let ret1 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret1.input_levels[0].table_infos.len());
        assert_eq!(3, ret1.input_levels[0].table_infos[0].sst_id);
        assert_eq!(4, ret1.input_levels[0].table_infos[1].sst_id);
        assert_eq!(5, ret1.input_levels[0].table_infos[2].sst_id);

        // Verify all selected files belong to vnode 20
        for sst in &ret1.input_levels[0].table_infos {
            let vnode = UserKey::decode(&sst.key_range.right).get_vnode_id();
            assert_eq!(
                vnode, 20,
                "All selected files should be from vnode 20, but SST {} has vnode {}",
                sst.sst_id, vnode
            );
        }

        // Second call: no more sufficient groups
        let ret2 = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(ret2.is_none());
    }

    #[test]
    fn test_no_repeated_compaction_with_simulated_output() {
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Initial state: 3 small files (20MB + 15MB + 10MB = 45MB total)
        let mut sst1 = generate_table_impl(1, 1, 1, 100, 1);
        sst1.sst_size = 20 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst1.into());

        let mut sst2 = generate_table_impl(2, 1, 101, 200, 1);
        sst2.sst_size = 15 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst2.into());

        let mut sst3 = generate_table_impl(3, 1, 201, 300, 1);
        sst3.sst_size = 10 * 1024 * 1024;
        levels.levels[0].table_infos.push(sst3.into());

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        // Round 1: Should select all 3 small files
        let ret1 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret1.input_levels[0].table_infos.len());
        assert_eq!(45 * 1024 * 1024, ret1.select_input_size);

        // Simulate compaction execution: 3 files merged into 1 file (45MB)
        // This simulates the normal case where target_file_size >> threshold
        levels.levels[0].table_infos.clear();
        let mut merged_sst = generate_table_impl(4, 1, 1, 300, 1);
        merged_sst.sst_size = 45 * 1024 * 1024; // Still below threshold (50MB)
        levels.levels[0].table_infos.push(merged_sst.into());

        // Reset state for next round
        state.clear();

        // Round 2: Should NOT select the merged file
        // Because there's only 1 file, doesn't meet MIN_FILE_COUNT requirement
        let ret2 = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(
            ret2.is_none(),
            "Should not select single file even if it's below threshold - Strategy 2 protects us"
        );
    }

    #[test]
    fn test_no_repeated_compaction_even_with_small_target_file_size() {
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 6,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        // Initial state: 3 small files (15MB each = 45MB total)
        for i in 1..=3 {
            let mut sst =
                generate_table_impl(i, 1, ((i - 1) * 100 + 1) as usize, (i * 100) as usize, 1);
            sst.sst_size = 15 * 1024 * 1024;
            levels.levels[0].table_infos.push(sst.into());
        }

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
            LevelHandler::new(5),
            LevelHandler::new(6),
        ];
        let mut state = SmallFilePickerState::default();
        let picker = SmallFileCompactionPicker::new(50 * 1024 * 1024);

        // Round 1: Select 3 files
        let ret1 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(3, ret1.input_levels[0].table_infos.len());

        // Simulate worst case: target_file_size is misconfigured to be very small (10MB)
        // Compaction outputs 4 small files instead of 1 large file
        levels.levels[0].table_infos.clear();
        for i in 4..=7u64 {
            let mut sst = generate_table_impl(
                i,
                1,
                ((i - 4) * 75 + 1) as usize,
                ((i - 3) * 75) as usize,
                1,
            );
            sst.sst_size = 11 * 1024 * 1024; // Each file is 11MB (below 50MB threshold)
            levels.levels[0].table_infos.push(sst.into());
        }

        state.clear();

        // Round 2: Should select these 4 files (all small, meets MIN_FILE_COUNT)
        let ret2 = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(4, ret2.input_levels[0].table_infos.len());

        // Simulate another round: 4 files -> 3 files (still misconfigured)
        levels.levels[0].table_infos.clear();
        for i in 8..=10u64 {
            let mut sst = generate_table_impl(
                i,
                1,
                ((i - 8) * 100 + 1) as usize,
                ((i - 7) * 100) as usize,
                1,
            );
            sst.sst_size = 15 * 1024 * 1024;
            levels.levels[0].table_infos.push(sst.into());
        }

        state.clear();

        // Round 3: Can still select (this demonstrates the issue with misconfiguration)
        let ret3 = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(
            ret3.is_some(),
            "With misconfigured target_file_size, compaction can repeat - but this is a config issue, not a code bug"
        );
        assert_eq!(3, ret3.unwrap().input_levels[0].table_infos.len());

        // Simulate final merge: 3 files -> 1 file (finally below MIN_FILE_COUNT)
        levels.levels[0].table_infos.clear();
        let mut final_sst = generate_table_impl(11, 1, 1, 300, 1);
        final_sst.sst_size = 45 * 1024 * 1024;
        levels.levels[0].table_infos.push(final_sst.into());

        state.clear();

        // Round 4: Eventually stops when only 1 file remains
        let ret4 = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(
            ret4.is_none(),
            "Eventually stops due to MIN_FILE_COUNT requirement"
        );
    }
}
