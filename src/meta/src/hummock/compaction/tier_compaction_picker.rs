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

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_hummock_sdk::key::{user_key, FullKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::{Level, LevelType, SstableInfo};

use super::SearchResult;
use crate::hummock::compaction::compaction_picker::CompactionPicker;
use crate::hummock::compaction::overlap_strategy::{OverlapStrategy, RangeOverlapStrategy};
use crate::hummock::compaction::CompactionConfig;
use crate::hummock::level_handler::LevelHandler;

pub struct TierCompactionPicker {
    compact_task_id: u64,
    target_level: usize,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    config: Arc<CompactionConfig>,
}

impl Default for TierCompactionPicker {
    fn default() -> Self {
        Self {
            compact_task_id: 0,
            target_level: 1,
            overlap_strategy: Arc::new(RangeOverlapStrategy {}),
            config: Arc::new(CompactionConfig::default()),
        }
    }
}

#[derive(Default)]
pub struct TargetFilesInfo {
    tables: Vec<SstableInfo>,
    table_ids: HashSet<u64>,
    compaction_bytes: u64,
}

impl CompactionPicker for TierCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let select_level = 0;
        let target_level = self.target_level;

        let next_task_id = self.compact_task_id;
        if levels[select_level].table_infos.is_empty() {
            return None;
        }
        let (select_level_inputs, target_level_inputs) = self.select_input_files(
            &levels[select_level],
            &levels[target_level],
            &level_handlers[select_level],
            &level_handlers[target_level],
        );
        if select_level_inputs.is_empty() {
            return self.pick_intra_l0_compaction(
                &levels[select_level],
                &mut level_handlers[select_level],
            );
        }

        level_handlers[select_level].add_pending_task(next_task_id, &select_level_inputs);
        level_handlers[target_level].add_pending_task(next_task_id, &target_level_inputs);
        // Here, we have known that `select_level_input` is valid
        let mut splits = Vec::with_capacity(target_level_inputs.len());
        splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
        if target_level_inputs.len() > 1 {
            for table in &target_level_inputs[1..] {
                let key_before_last: Bytes = FullKey::from_user_key_slice(
                    user_key(&table.key_range.as_ref().unwrap().left),
                    HummockEpoch::MAX,
                )
                .into_inner()
                .into();
                splits.last_mut().unwrap().right = key_before_last.clone();
                splits.push(KeyRange::new(key_before_last, Bytes::new()));
            }
        }

        Some(SearchResult {
            select_level: Level {
                level_idx: select_level as u32,
                level_type: LevelType::Overlapping as i32,
                table_infos: select_level_inputs,
            },
            target_level: Level {
                level_idx: target_level as u32,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: target_level_inputs,
            },
            split_ranges: splits,
        })
    }
}

impl TierCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        target_level: usize,
        config: Arc<CompactionConfig>,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> TierCompactionPicker {
        TierCompactionPicker {
            compact_task_id,
            target_level,
            overlap_strategy,
            config,
        }
    }

    fn pick_target_level_overlap_files(
        &self,
        select_tables: &[SstableInfo],
        level: &Level,
        level_handlers: &LevelHandler,
        input_ssts: &mut TargetFilesInfo,
    ) -> bool {
        if level.table_infos.is_empty() {
            return true;
        }
        // pick up files in L1 which are overlap with L0 to target level input.
        let new_add_tables = self
            .overlap_strategy
            .check_multiple_overlap(select_tables, &level.table_infos);
        if new_add_tables
            .iter()
            .any(|table| level_handlers.is_pending_compact(&table.id))
        {
            return false;
        }
        for table in new_add_tables {
            if input_ssts.table_ids.contains(&table.id) {
                continue;
            }
            input_ssts.table_ids.insert(table.id);
            input_ssts.compaction_bytes += table.file_size;
            input_ssts.tables.push(table);
        }
        true
    }

    fn pick_intra_l0_compaction(
        &self,
        level0: &Level,
        level0_handler: &mut LevelHandler,
    ) -> Option<SearchResult> {
        if level0.table_infos.len() < self.config.level0_max_file_number {
            return None;
        }

        let mut sst_idx = 0;
        while sst_idx < level0.table_infos.len() {
            if !level0_handler.is_pending_compact(&level0.table_infos[sst_idx].id) {
                break;
            }
            sst_idx += 1;
        }

        if sst_idx >= level0.table_infos.len() {
            return None;
        }

        let mut compaction_bytes = level0.table_infos[sst_idx].file_size;
        let mut select_level_inputs = vec![level0.table_infos[sst_idx].clone()];
        for table in level0.table_infos[sst_idx + 1..].iter() {
            if level0_handler.is_pending_compact(&table.id) {
                break;
            }
            if compaction_bytes >= self.config.max_compaction_bytes {
                break;
            }
            compaction_bytes += table.file_size;
            select_level_inputs.push(table.clone());
        }
        if select_level_inputs.len() < self.config.level0_trigger_number {
            return None;
        }
        level0_handler.add_pending_task(self.compact_task_id, &select_level_inputs);
        Some(SearchResult {
            select_level: Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: select_level_inputs,
            },
            target_level: Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![],
            },
            split_ranges: vec![],
        })
    }

    fn select_input_files(
        &self,
        select_level: &Level,
        target_level: &Level,
        select_level_handler: &LevelHandler,
        target_level_handler: &LevelHandler,
    ) -> (Vec<SstableInfo>, Vec<SstableInfo>) {
        let mut info = self.overlap_strategy.create_overlap_info();
        for idx in 0..select_level.table_infos.len() {
            let select_table = select_level.table_infos[idx].clone();
            if select_level_handler.is_pending_compact(&select_table.id) {
                info.update(&select_table);
                continue;
            }
            if info.check_overlap(&select_table) {
                info.update(&select_table);
                continue;
            }

            let mut target_level_ssts = TargetFilesInfo::default();
            let mut select_compaction_bytes = select_table.file_size;
            let mut select_level_ssts = vec![select_table];
            if !self.pick_target_level_overlap_files(
                &select_level_ssts,
                target_level,
                target_level_handler,
                &mut target_level_ssts,
            ) {
                break;
            }
            // try expand more L0 files if the currenct compaction job is too small.
            for other in &select_level.table_infos[idx + 1..] {
                if select_level_handler.is_pending_compact(&other.id) {
                    break;
                }
                if select_compaction_bytes >= self.config.max_compaction_bytes {
                    break;
                }

                if info.check_overlap(other) {
                    break;
                }

                select_level_ssts.push(other.clone());
                if !self.pick_target_level_overlap_files(
                    &select_level_ssts,
                    target_level,
                    target_level_handler,
                    &mut target_level_ssts,
                ) {
                    select_level_ssts.pop().unwrap();
                    break;
                }
                select_compaction_bytes += other.file_size;
            }
            target_level_ssts.tables.sort_by(|a, b| {
                let r1 = KeyRange::from(a.key_range.as_ref().unwrap());
                let r2 = KeyRange::from(b.key_range.as_ref().unwrap());
                r1.cmp(&r2)
            });
            return (select_level_ssts, target_level_ssts.tables);
        }
        (vec![], vec![])
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_pb::hummock::KeyRange as RawKeyRange;

    use super::*;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    fn generate_table(
        id: u64,
        table_prefix: u64,
        left: usize,
        right: usize,
        epoch: u64,
    ) -> SstableInfo {
        SstableInfo {
            id,
            key_range: Some(RawKeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch),
                inf: false,
            }),
            file_size: 1,
        }
    }

    #[test]
    fn test_compact_l0_to_l1() {
        let picker = TierCompactionPicker::new(
            0,
            1,
            Arc::new(CompactionConfig::default()),
            Arc::new(RangeOverlapStrategy::default()),
        );
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(5, 1, 201, 300, 2),
                    generate_table(4, 1, 112, 200, 2),
                ],
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(3, 1, 0, 100, 1),
                    generate_table(2, 1, 111, 200, 1),
                    generate_table(1, 1, 222, 300, 1),
                    generate_table(0, 1, 301, 400, 1),
                ],
            },
        ];
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 2);
        assert_eq!(ret.target_level.table_infos[0].id, 2);
        assert_eq!(ret.target_level.table_infos[1].id, 1);

        // no conflict with the last job
        levels[0]
            .table_infos
            .push(generate_table(6, 1, 301, 333, 4));
        levels[0]
            .table_infos
            .push(generate_table(7, 1, 100, 200, 2));
        // pick table 5 and 0. but skip table 6 because [0_key_test_000100, 1_key_test_000333] will
        // be conflict with the previous job.
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 3);
        assert_eq!(levels_handler[1].get_pending_file_count(), 3);
        assert_eq!(ret.target_level.table_infos[0].id, 0);
        assert_eq!(ret.select_level.table_infos[0].id, 6);

        // the first idle table in L0 is table 6 and its confict with the last job so we can not
        // pick table 7.
        let picker = TierCompactionPicker::new(
            1,
            1,
            Arc::new(CompactionConfig::default()),
            Arc::new(RangeOverlapStrategy::default()),
        );
        levels[0]
            .table_infos
            .push(generate_table(8, 1, 222, 233, 3));
        let ret = picker.pick_compaction(&levels, &mut levels_handler);
        assert!(ret.is_none());

        // compact L0 to L0
        let config = CompactionConfig {
            level0_max_file_number: 2,
            level0_trigger_number: 2,
            ..Default::default()
        };
        let picker = TierCompactionPicker::new(
            1,
            1,
            Arc::new(config),
            Arc::new(RangeOverlapStrategy::default()),
        );
        levels[0]
            .table_infos
            .push(generate_table(9, 1, 100, 200, 3));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.table_infos[0].id, 7);
        assert_eq!(ret.select_level.table_infos[1].id, 8);
        assert_eq!(ret.select_level.table_infos[2].id, 9);
        assert!(ret.target_level.table_infos.is_empty());
    }

    #[test]
    fn test_selecting_key_range_overlap() {
        // When picking L0->L1, all L1 files overlapped with selecting_key_range should be picked.

        let picker = TierCompactionPicker::default();

        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 2),
                    generate_table(2, 1, 400, 500, 2),
                ],
            },
            Level {
                level_idx: 1,

                level_type: LevelType::Nonoverlapping as i32,

                table_infos: vec![
                    generate_table(3, 1, 0, 50, 1),
                    generate_table(4, 1, 150, 200, 1),
                    generate_table(5, 1, 250, 300, 1),
                    generate_table(6, 1, 1000, 200, 1),
                ],
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 2);

        assert_eq!(levels_handler[1].get_pending_file_count(), 2);

        assert_eq!(
            ret.select_level
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![1, 2]
        );

        assert_eq!(
            ret.target_level
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![4, 5]
        );
    }

    #[test]
    fn test_compacting_key_range_overlap_l0() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with L0's

        // compacting_key_range.
        let picker = TierCompactionPicker::default();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 2),
                    generate_table(2, 1, 450, 500, 2),
                ],
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![],
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 0);

        levels[0]
            .table_infos
            .push(generate_table(3, 1, 250, 300, 3));

        assert!(picker
            .pick_compaction(&levels, &mut levels_handler)
            .is_none());
    }

    #[test]
    fn test_compacting_key_range_overlap_l1() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with any L1 files
        // under compaction.

        let picker = TierCompactionPicker::default();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![generate_table(1, 1, 200, 250, 2)],
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(2, 1, 150, 300, 2)],
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);

        levels[0]
            .table_infos
            .push(generate_table(3, 1, 100, 110, 3));

        levels[0]
            .table_infos
            .push(generate_table(4, 1, 400, 500, 3));

        // Will be intra L0
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert!(ret.target_level.table_infos.is_empty());
        assert_eq!(
            ret.select_level
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![3]
        );
    }

    #[test]
    fn test_compacting_key_range_overlap_intra_l0() {
        // When picking L0->L0, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let picker = TierCompactionPicker::default();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 160, 2),
                    generate_table(2, 1, 190, 250, 2),
                ],
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(3, 1, 200, 300, 2)],
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);

        levels[0]
            .table_infos
            .push(generate_table(4, 1, 170, 180, 3));

        assert!(picker
            .pick_compaction(&levels, &mut levels_handler)
            .is_none());
    }
}
