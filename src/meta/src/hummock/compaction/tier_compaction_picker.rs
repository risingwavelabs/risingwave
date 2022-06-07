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

use risingwave_hummock_sdk::key::{user_key, FullKey};
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::{KeyRange, Level, LevelType, SstableInfo};

use super::SearchResult;
use crate::hummock::compaction::compaction_picker::CompactionPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::CompactionConfig;
use crate::hummock::level_handler::LevelHandler;

const MIN_COMPACTION_BYTES: u64 = 2 * 1024 * 1024; // 1MB

pub struct TierCompactionPicker {
    compact_task_id: u64,
    config: Arc<CompactionConfig>,
}

impl TierCompactionPicker {
    pub fn new(compact_task_id: u64, config: Arc<CompactionConfig>) -> TierCompactionPicker {
        TierCompactionPicker {
            compact_task_id,
            config,
        }
    }
}

impl CompactionPicker for TierCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let mut idx = 0;
        while idx < levels[0].table_infos.len() {
            let table = &levels[0].table_infos[idx];
            if level_handlers[0].is_pending_compact(&table.id) {
                idx += 1;
                continue;
            }
            let mut compaction_bytes = table.file_size;
            let mut select_level_inputs = vec![table.clone()];
            if table.file_size > self.config.min_compaction_bytes {
                // only merge small file until we support sub-level.
                idx += 1;
                continue;
            }

            let mut next_offset = idx + 1;

            for other in &levels[0].table_infos[idx + 1..] {
                if level_handlers[0].is_pending_compact(&other.id) {
                    break;
                }
                // no need to trigger a bigger compaction
                if compaction_bytes >= self.config.min_compaction_bytes {
                    break;
                }
                compaction_bytes += other.file_size;
                select_level_inputs.push(other.clone());
                next_offset += 1;
            }

            // to make compaction tree balance, we do not need to merge a large file and a few small
            // files.
            while let Some(first) = select_level_inputs.first() {
                if first.file_size * 2 > compaction_bytes
                    && select_level_inputs.len() >= self.config.level0_tier_compact_file_number
                    && compaction_bytes > MIN_COMPACTION_BYTES
                {
                    compaction_bytes -= first.file_size;
                    select_level_inputs.remove(0);
                } else {
                    break;
                }
            }

            if select_level_inputs.len() < self.config.level0_tier_compact_file_number {
                idx = next_offset;
                continue;
            }

            level_handlers[0].add_pending_task(self.compact_task_id, &select_level_inputs);

            return Some(SearchResult {
                select_level: Level {
                    level_idx: 0,
                    level_type: LevelType::Overlapping as i32,
                    table_infos: select_level_inputs,
                    total_file_size: 0,
                },
                target_level: Level {
                    level_idx: 0,
                    level_type: LevelType::Overlapping as i32,
                    table_infos: vec![],
                    total_file_size: 0,
                },
                split_ranges: vec![],
            });
        }
        None
    }
}

pub struct LevelCompactionPicker {
    compact_task_id: u64,
    target_level: usize,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    config: Arc<CompactionConfig>,
}

#[derive(Default)]
pub struct TargetFilesInfo {
    tables: Vec<SstableInfo>,
    table_ids: HashSet<u64>,
    compaction_bytes: u64,
}

impl TargetFilesInfo {
    fn add_tables(&mut self, new_add_tables: Vec<SstableInfo>) {
        for table in new_add_tables {
            if self.table_ids.contains(&table.id) {
                continue;
            }
            self.table_ids.insert(table.id);
            self.compaction_bytes += table.file_size;
            self.tables.push(table);
        }
    }

    fn calc_inc_compaction_size(&self, new_add_tables: &[SstableInfo]) -> u64 {
        new_add_tables
            .iter()
            .filter(|table| !self.table_ids.contains(&table.id))
            .map(|table| table.file_size)
            .sum()
    }
}

impl CompactionPicker for LevelCompactionPicker {
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
            return None;
        }

        level_handlers[select_level].add_pending_task(next_task_id, &select_level_inputs);
        level_handlers[target_level].add_pending_task(next_task_id, &target_level_inputs);
        // Here, we have known that `select_level_input` is valid
        let mut splits = Vec::with_capacity(target_level_inputs.len());
        splits.push(KeyRange::new(vec![], vec![]));
        if target_level_inputs.len() > 1 {
            for table in &target_level_inputs[1..] {
                let key_before_last = FullKey::from_user_key_slice(
                    user_key(&table.key_range.as_ref().unwrap().left),
                    HummockEpoch::MAX,
                )
                .into_inner();
                splits.last_mut().unwrap().right = key_before_last.clone();
                splits.push(KeyRange::new(key_before_last, vec![]));
            }
        }

        Some(SearchResult {
            select_level: Level {
                level_idx: select_level as u32,
                level_type: LevelType::Overlapping as i32,
                table_infos: select_level_inputs,
                // no use
                total_file_size: 0,
            },
            target_level: Level {
                level_idx: target_level as u32,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: target_level_inputs,
                total_file_size: 0,
            },
            split_ranges: splits,
        })
    }
}

impl LevelCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        target_level: usize,
        config: Arc<CompactionConfig>,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> LevelCompactionPicker {
        LevelCompactionPicker {
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
    ) -> Option<Vec<SstableInfo>> {
        if level.table_infos.is_empty() {
            return Some(vec![]);
        }
        // pick up files in L1 which are overlap with L0 to target level input.
        let new_add_tables = self
            .overlap_strategy
            .check_base_level_overlap(select_tables, &level.table_infos);
        if new_add_tables
            .iter()
            .any(|table| level_handlers.is_pending_compact(&table.id))
        {
            return None;
        }
        Some(new_add_tables)
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
            let mut select_info = self.overlap_strategy.create_overlap_info();
            let mut select_compaction_bytes = select_table.file_size;
            select_info.update(&select_table);
            let mut select_level_ssts = vec![select_table];
            match self.pick_target_level_overlap_files(
                &select_level_ssts,
                target_level,
                target_level_handler,
            ) {
                None => continue,
                Some(tables) => {
                    target_level_ssts.add_tables(tables);
                }
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
                select_info.update(other);
                if select_level.table_infos[0..idx]
                    .iter()
                    .any(|table| select_info.check_overlap(table))
                {
                    select_level_ssts.pop().unwrap();
                    break;
                }

                match self.pick_target_level_overlap_files(
                    &select_level_ssts,
                    target_level,
                    target_level_handler,
                ) {
                    None => {
                        select_level_ssts.pop().unwrap();
                        break;
                    }
                    Some(tables) => {
                        // we only extend L0 files when write-amplification does not increase.
                        let inc_compaction_size =
                            target_level_ssts.calc_inc_compaction_size(&tables);
                        if select_compaction_bytes > 0
                            && (target_level_ssts.compaction_bytes + inc_compaction_size)
                                / (select_compaction_bytes + other.file_size)
                                > target_level_ssts.compaction_bytes / select_compaction_bytes
                        {
                            select_level_ssts.pop().unwrap();
                            break;
                        }
                        target_level_ssts.add_tables(tables);
                    }
                }
                select_compaction_bytes += other.file_size;
            }

            // do not schedule tasks with big write-amplification
            if select_compaction_bytes < self.config.min_compaction_bytes
                && select_compaction_bytes * 4 < target_level_ssts.compaction_bytes
            {
                continue;
            }

            target_level_ssts.tables.sort_by(|a, b| {
                let r1 = a.key_range.as_ref().unwrap();
                let r2 = b.key_range.as_ref().unwrap();
                r1.compare(r2)
            });
            return (select_level_ssts, target_level_ssts.tables);
        }
        (vec![], vec![])
    }
}

#[cfg(test)]
pub mod tests {
    use itertools::Itertools;
    use risingwave_pb::hummock::KeyRange;

    use super::*;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::CompactionMode;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    pub fn generate_table(
        id: u64,
        table_prefix: u64,
        left: usize,
        right: usize,
        epoch: u64,
    ) -> SstableInfo {
        SstableInfo {
            id,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch),
                inf: false,
            }),
            file_size: (right - left + 1) as u64,
            vnode_bitmaps: vec![],
        }
    }

    fn create_compaction_picker_for_test() -> LevelCompactionPicker {
        let config = Arc::new(CompactionConfig {
            level0_tier_compact_file_number: 2,
            min_compaction_bytes: 0,
            ..Default::default()
        });
        LevelCompactionPicker::new(0, 1, config, Arc::new(RangeOverlapStrategy::default()))
    }

    #[test]
    fn test_compact_l0_to_l1() {
        let picker = create_compaction_picker_for_test();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(5, 1, 201, 300, 2),
                    generate_table(4, 1, 112, 200, 2),
                ],
                total_file_size: 0,
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
                total_file_size: 0,
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
        let picker = LevelCompactionPicker::new(
            1,
            1,
            Arc::new(CompactionConfig::default()),
            Arc::new(RangeOverlapStrategy::default()),
        );
        levels[0]
            .table_infos
            .push(generate_table(8, 1, 199, 233, 3));
        let ret = picker.pick_compaction(&levels, &mut levels_handler);
        assert!(ret.is_none());

        // compact L0 to L0
        let config = CompactionConfig {
            level0_tier_compact_file_number: 2,
            ..Default::default()
        };
        let picker = TierCompactionPicker::new(2, Arc::new(config));
        levels[0]
            .table_infos
            .push(generate_table(9, 1, 100, 400, 3));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.table_infos[0].id, 7);
        assert_eq!(ret.select_level.table_infos[1].id, 8);
        assert_eq!(ret.select_level.table_infos[2].id, 9);
        assert!(ret.target_level.table_infos.is_empty());
        levels_handler[0].remove_task(1);
        levels[0].table_infos.retain(|table| table.id < 7);
        levels[0]
            .table_infos
            .push(generate_table(10, 1, 100, 200, 3));
        levels[0]
            .table_infos
            .push(generate_table(11, 1, 201, 300, 3));
        levels[0]
            .table_infos
            .push(generate_table(12, 1, 301, 400, 3));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.table_infos.len(), 3);
    }

    #[test]
    fn test_selecting_key_range_overlap() {
        // When picking L0->L1, all L1 files overlapped with selecting_key_range should be picked.

        let config = Arc::new(CompactionConfig {
            level0_tier_compact_file_number: 2,
            min_compaction_bytes: 0,
            compaction_mode: CompactionMode::RangeMode,
            ..Default::default()
        });
        let picker =
            LevelCompactionPicker::new(0, 1, config, Arc::new(RangeOverlapStrategy::default()));

        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 2),
                    generate_table(2, 1, 400, 500, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,

                level_type: LevelType::Nonoverlapping as i32,

                table_infos: vec![
                    generate_table(3, 1, 0, 50, 1),
                    generate_table(4, 1, 150, 200, 1),
                    generate_table(5, 1, 250, 300, 1),
                    generate_table(6, 1, 1000, 2000, 1),
                ],
                total_file_size: 0,
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
        let picker = create_compaction_picker_for_test();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 2),
                    generate_table(2, 1, 450, 500, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
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
        let picker = create_compaction_picker_for_test();

        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![generate_table(1, 1, 200, 250, 2)],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(2, 1, 150, 300, 2)],
                total_file_size: 0,
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

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        // Will be trival move. The second file can not be picked up because the range of files
        // [3,4] would be overlap with file [0]
        assert!(ret.target_level.table_infos.is_empty());
        assert_eq!(ret.target_level.level_idx, 1);
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
        let picker = create_compaction_picker_for_test();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 160, 2),
                    generate_table(2, 1, 190, 250, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(3, 1, 200, 300, 2)],
                total_file_size: 0,
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

    #[test]
    fn test_compact_with_write_amplification_limit() {
        let picker = create_compaction_picker_for_test();
        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 160, 2),
                    generate_table(2, 1, 190, 250, 2),
                    generate_table(3, 1, 200, 300, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 100, 199, 2),
                    generate_table(5, 1, 200, 260, 2),
                    generate_table(6, 1, 300, 600, 2),
                ],
                total_file_size: 0,
            },
        ];
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 2);
        assert_eq!(ret.select_level.table_infos[0].id, 1);
        assert_eq!(ret.select_level.table_infos[1].id, 2);
        assert_eq!(ret.target_level.table_infos[0].id, 4);
        assert_eq!(ret.target_level.table_infos[1].id, 5);
    }
}
