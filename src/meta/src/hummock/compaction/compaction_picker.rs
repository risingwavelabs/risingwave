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

use risingwave_pb::hummock::{Level, SstableInfo};

use super::overlap_strategy::OverlapInfo;
use crate::hummock::compaction::overlap_strategy::{OverlapStrategy, RangeOverlapInfo};
use crate::hummock::compaction::{ManualCompactionOption, SearchResult};
use crate::hummock::level_handler::LevelHandler;

pub trait CompactionPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult>;
}

pub struct MinOverlappingPicker {
    compact_task_id: u64,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    level: usize,
}

impl MinOverlappingPicker {
    pub fn new(
        compact_task_id: u64,
        level: usize,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> MinOverlappingPicker {
        MinOverlappingPicker {
            compact_task_id,
            overlap_strategy,
            level,
        }
    }
}

impl MinOverlappingPicker {
    // For example:
    //  L1:      [(k0, k4),  (k5, k7), (k8, k9)]
    //  L2:      [(k0, k1), (k2, k6)]
    // If we only choose file (k0, k4), (k0, k1), (k2, k6), the file (k5, k7) will compact with the
    // result of their compaction task again. So a better strategy is choosing (k0, k4), (k0, k1),
    // (k2, k6), (k5, k7) all in one task.
    fn try_expand_input(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
        select_input_ssts: Vec<SstableInfo>,
        target_input_ssts: &[SstableInfo],
    ) -> Vec<SstableInfo> {
        assert_eq!(select_input_ssts.len(), 1);
        let select_overlap_files = self
            .overlap_strategy
            .check_base_level_overlap(target_input_ssts, &levels[self.level].table_infos);
        if select_overlap_files
            .iter()
            .any(|table| level_handlers[self.level].is_pending_compact(&table.id))
        {
            return select_input_ssts;
        }
        if select_overlap_files.len() == select_input_ssts.len() {
            return select_input_ssts;
        }
        let mut target_input_ids = HashSet::with_capacity(target_input_ssts.len());
        for table in target_input_ssts {
            target_input_ids.insert(table.id);
        }
        let select_table_id = select_input_ssts[0].id;
        let target_level = self.level + 1;
        let mut select_overlap_results = vec![];
        for table in select_overlap_files {
            if table.id == select_table_id {
                select_overlap_results.push(table);
                continue;
            }

            let mut info = self.overlap_strategy.create_overlap_info();
            info.update(&table);
            let target_overlap_files =
                info.check_multiple_overlap(&levels[target_level].table_infos);
            if target_overlap_files
                .iter()
                .any(|other| !target_input_ids.contains(&other.id))
            {
                continue;
            }
            select_overlap_results.push(table);
        }

        // Check again because there may be a file does not overlap with any other files in target
        // level but the range between it and another file would overlap more files in
        // target level.
        if select_overlap_results.len() > select_input_ssts.len() {
            let target_overlap_files = self.overlap_strategy.check_base_level_overlap(
                &select_overlap_results,
                &levels[target_level].table_infos,
            );
            if target_overlap_files
                .iter()
                .all(|table| target_input_ids.contains(&table.id))
            {
                return select_overlap_results;
            }
        }
        select_input_ssts
    }
}

impl CompactionPicker for MinOverlappingPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let target_level = self.level + 1;
        let mut scores = vec![];
        for table in &levels[self.level].table_infos {
            if level_handlers[self.level].is_pending_compact(&table.id) {
                continue;
            }
            let mut total_file_size = 0;
            let mut pending_campct = false;
            let overlap_files = self
                .overlap_strategy
                .check_base_level_overlap(&[table.clone()], &levels[target_level].table_infos);
            for other in overlap_files {
                if level_handlers[target_level].is_pending_compact(&other.id) {
                    pending_campct = true;
                    break;
                }
                total_file_size += other.file_size;
            }
            if pending_campct {
                continue;
            }
            scores.push((total_file_size * 100 / (table.file_size + 1), table.clone()));
        }
        if scores.is_empty() {
            return None;
        }
        let (_, table) = scores.iter().min_by(|x, y| x.0.cmp(&y.0)).unwrap();
        let mut select_input_ssts = vec![table.clone()];
        let target_input_ssts = self
            .overlap_strategy
            .check_base_level_overlap(&select_input_ssts, &levels[target_level].table_infos);
        if !target_input_ssts.is_empty() {
            let expand_select_ssts = self.try_expand_input(
                levels,
                level_handlers,
                select_input_ssts,
                &target_input_ssts,
            );
            select_input_ssts = expand_select_ssts;
        }
        level_handlers[self.level].add_pending_task(self.compact_task_id, &select_input_ssts);
        if !target_input_ssts.is_empty() {
            level_handlers[target_level].add_pending_task(self.compact_task_id, &target_input_ssts);
        }
        Some(SearchResult {
            select_level: Level {
                level_idx: self.level as u32,
                level_type: levels[self.level].level_type,
                table_infos: select_input_ssts,
                total_file_size: 0,
            },
            target_level: Level {
                level_idx: target_level as u32,
                level_type: levels[target_level].level_type,
                table_infos: target_input_ssts,
                total_file_size: 0,
            },
            split_ranges: vec![],
        })
    }
}

pub struct ManualCompactionPicker {
    compact_task_id: u64,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    option: ManualCompactionOption,
}

impl ManualCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        option: ManualCompactionOption,
    ) -> Self {
        Self {
            compact_task_id,
            overlap_strategy,
            option,
        }
    }
}

impl CompactionPicker for ManualCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let level = self.option.level;
        let target_level = level + 1;

        let mut select_input_ssts = vec![];
        let mut tmp_sst_info = SstableInfo::default();
        let mut range_overlap_info = RangeOverlapInfo::default();
        tmp_sst_info.key_range = Some(self.option.key_range.clone());
        range_overlap_info.update(&tmp_sst_info);

        let level_table_infos: Vec<SstableInfo> = levels[level]
            .table_infos
            .iter()
            .filter(|sst_info| range_overlap_info.check_overlap(sst_info))
            .filter(|sst_info| {
                if self.option.internal_table_id.is_empty() {
                    return true;
                }

                // to collect internal_table_id from sst_info
                let table_id_in_sst: Vec<u32> = sst_info
                    .vnode_bitmaps
                    .iter()
                    .map(|vmap| vmap.table_id)
                    .collect();

                // to filter sst_file by table_id
                for table_id in &table_id_in_sst {
                    if self.option.internal_table_id.contains(table_id) {
                        return true;
                    }
                }

                false
            })
            .cloned()
            .collect();

        for table in &level_table_infos {
            if level_handlers[level].is_pending_compact(&table.id) {
                continue;
            }

            let mut pending_campct = false;
            let overlap_files = self
                .overlap_strategy
                .check_base_level_overlap(&[table.clone()], &levels[target_level].table_infos);
            for other in overlap_files {
                if level_handlers[target_level].is_pending_compact(&other.id) {
                    pending_campct = true;
                    break;
                }
            }
            if pending_campct {
                continue;
            }

            select_input_ssts.push(table.clone());
        }

        if select_input_ssts.is_empty() {
            return None;
        }

        let target_input_ssts = self
            .overlap_strategy
            .check_base_level_overlap(&select_input_ssts, &levels[target_level].table_infos);

        level_handlers[level].add_pending_task(self.compact_task_id, &select_input_ssts);
        if !target_input_ssts.is_empty() {
            level_handlers[target_level].add_pending_task(self.compact_task_id, &target_input_ssts);
        }

        Some(SearchResult {
            select_level: Level {
                level_idx: level as u32,
                level_type: levels[level].level_type,
                table_infos: select_input_ssts,
                total_file_size: 0,
            },
            target_level: Level {
                level_idx: target_level as u32,
                level_type: levels[target_level].level_type,
                table_infos: target_input_ssts,
                total_file_size: 0,
            },
            split_ranges: vec![],
        })
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_pb::common::VNodeBitmap;
    pub use risingwave_pb::hummock::{KeyRange, LevelType};

    use super::*;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::tier_compaction_picker::tests::generate_table;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    #[test]
    fn test_compact_l1() {
        let picker = MinOverlappingPicker::new(0, 1, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 0, 100, 1),
                    generate_table(5, 1, 101, 150, 1),
                    generate_table(6, 1, 151, 201, 1),
                    generate_table(7, 1, 501, 800, 1),
                    generate_table(8, 2, 301, 400, 1),
                ],
                total_file_size: 0,
            },
        ];
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trival move to next level.
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.level_idx, 1);
        assert_eq!(ret.target_level.level_idx, 2);
        assert_eq!(ret.select_level.table_infos.len(), 1);
        assert_eq!(ret.select_level.table_infos[0].id, 2);
        assert_eq!(ret.target_level.table_infos.len(), 0);

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.level_idx, 1);
        assert_eq!(ret.target_level.level_idx, 2);
        assert_eq!(ret.select_level.table_infos.len(), 1);
        assert_eq!(ret.target_level.table_infos.len(), 1);
        assert_eq!(ret.select_level.table_infos[0].id, 0);
        assert_eq!(ret.target_level.table_infos[0].id, 4);

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.level_idx, 1);
        assert_eq!(ret.target_level.level_idx, 2);
        assert_eq!(ret.select_level.table_infos.len(), 1);
        assert_eq!(ret.target_level.table_infos.len(), 2);
        assert_eq!(ret.select_level.table_infos[0].id, 1);
        assert_eq!(ret.target_level.table_infos[0].id, 5);
        assert_eq!(ret.target_level.table_infos[1].id, 6);
    }

    #[test]
    fn test_expand_l1_files() {
        let picker = MinOverlappingPicker::new(0, 1, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 50, 199, 1),
                    generate_table(5, 1, 200, 399, 1),
                ],
                total_file_size: 0,
            },
        ];
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trival move to next level.
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.level_idx, 1);
        assert_eq!(ret.target_level.level_idx, 2);

        assert_eq!(ret.select_level.table_infos.len(), 2);
        assert_eq!(ret.select_level.table_infos[0].id, 0);
        assert_eq!(ret.select_level.table_infos[1].id, 1);

        assert_eq!(ret.target_level.table_infos.len(), 1);
        assert_eq!(ret.target_level.table_infos[0].id, 4);
    }

    #[test]
    fn test_manaul_compaction_picker() {
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 0, 100, 1),
                    generate_table(5, 1, 101, 150, 1),
                    generate_table(6, 1, 151, 201, 1),
                    generate_table(7, 1, 501, 800, 1),
                    generate_table(8, 2, 301, 400, 1),
                ],
                total_file_size: 0,
            },
        ];
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        let clean_task_state = |level_handler: &mut LevelHandler| {
            for pending_task_id in &level_handler.pending_tasks_ids() {
                level_handler.remove_task(*pending_task_id);
            }
        };

        {
            // test key_range option
            let option = ManualCompactionOption {
                level: 1,
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 0, 1),
                    right: iterator_test_key_of_epoch(1, 201, 1),
                    inf: false,
                },
                ..Default::default()
            };
            let picker =
                ManualCompactionPicker::new(0, Arc::new(RangeOverlapStrategy::default()), option);
            let result = picker
                .pick_compaction(&levels, &mut levels_handler)
                .unwrap();

            assert_eq!(2, result.select_level.table_infos.len());
            assert_eq!(3, result.target_level.table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            // test all key range
            let option = ManualCompactionOption::default();
            let picker =
                ManualCompactionPicker::new(0, Arc::new(RangeOverlapStrategy::default()), option);
            let result = picker
                .pick_compaction(&levels, &mut levels_handler)
                .unwrap();

            assert_eq!(3, result.select_level.table_infos.len());
            assert_eq!(3, result.target_level.table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            let level_table_info = &mut levels[1].table_infos;
            let table_info_1 = &mut level_table_info[1];
            table_info_1.vnode_bitmaps.resize(2, VNodeBitmap::default());
            table_info_1.vnode_bitmaps[0].table_id = 1;
            table_info_1.vnode_bitmaps[1].table_id = 2;

            // test internal_table_id
            let option = ManualCompactionOption {
                level: 1,
                internal_table_id: HashSet::from([2]),
                ..Default::default()
            };

            let picker =
                ManualCompactionPicker::new(0, Arc::new(RangeOverlapStrategy::default()), option);

            let result = picker
                .pick_compaction(&levels, &mut levels_handler)
                .unwrap();

            assert_eq!(1, result.select_level.table_infos.len());
            assert_eq!(2, result.target_level.table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            // include all table_info
            let level_table_info = &mut levels[1].table_infos;
            for table_info in level_table_info {
                table_info.vnode_bitmaps.resize(2, VNodeBitmap::default());
                table_info.vnode_bitmaps[0].table_id = 1;
                table_info.vnode_bitmaps[1].table_id = 2;
            }

            // test key range filter first
            let option = ManualCompactionOption {
                level: 1,
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 101, 1),
                    right: iterator_test_key_of_epoch(1, 199, 1),
                    inf: false,
                },
                internal_table_id: HashSet::from([2]),
            };

            let picker =
                ManualCompactionPicker::new(0, Arc::new(RangeOverlapStrategy::default()), option);

            let result = picker
                .pick_compaction(&levels, &mut levels_handler)
                .unwrap();

            assert_eq!(1, result.select_level.table_infos.len());
            assert_eq!(2, result.target_level.table_infos.len());
        }
    }
}
