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

use bytes::Bytes;
use risingwave_hummock_sdk::key::{user_key, FullKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::{Level, LevelType, SstableInfo};

use super::SearchResult;
use crate::hummock::level_handler::LevelHandler;

const DEFAULT_MAX_COMPACTION_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2GB
const DEFAULT_LEVEL0_MAX_FILE_NUMBER: usize = 16;
const DEFAULT_LEVEL0_TRIGGER_NUMBER: usize = 4;

#[derive(Debug)]
pub struct TierCompactionPicker {
    compact_task_id: u64,
    max_compaction_bytes: u64,
    level0_max_file_number: usize,
    level0_trigger_number: usize,
}

#[derive(Default)]
pub struct ConflictKeyRange {
    key_range: Option<KeyRange>,
}

impl ConflictKeyRange {
    fn check_pending_compact(&mut self, level_handler: &LevelHandler, table: &SstableInfo) -> bool {
        let key_range = KeyRange::from(table.key_range.as_ref().unwrap());
        if level_handler.is_pending_compact(&table.id) {
            if let Some(range) = self.key_range.as_mut() {
                range.full_key_extend(&key_range);
            } else {
                self.key_range = Some(key_range);
            }
            return true;
        }
        false
    }

    fn check_overlap(&self, table: &SstableInfo) -> bool {
        let key_range = KeyRange::from(table.key_range.as_ref().unwrap());
        if let Some(range) = self.key_range.as_ref() {
            // This file is conflict with previous files in L0.
            if key_range.full_key_overlap(range) {
                return true;
            }
        }
        false
    }
}

impl TierCompactionPicker {
    pub fn new(compact_task_id: u64) -> TierCompactionPicker {
        TierCompactionPicker {
            compact_task_id,
            // TODO: set it by cluster configure or vertical group configure.
            max_compaction_bytes: DEFAULT_MAX_COMPACTION_BYTES,
            level0_max_file_number: DEFAULT_LEVEL0_MAX_FILE_NUMBER,
            level0_trigger_number: DEFAULT_LEVEL0_TRIGGER_NUMBER,
        }
    }

    pub fn pick_compaction(
        &self,
        levels: Vec<Level>,
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let select_level = 0;
        // TODO: After support dynamic-leveled-compaction, use base-level as target-level.
        let target_level = 1;

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

    fn pick_target_level_overlap_files(
        &self,
        tier_key_range: &KeyRange,
        level: &Level,
        level_handlers: &LevelHandler,
        input_ssts: &mut Vec<SstableInfo>,
    ) -> bool {
        let overlap_begin = level.table_infos.partition_point(|table_status| {
            user_key(&table_status.key_range.as_ref().unwrap().right)
                < user_key(&tier_key_range.left)
        });
        if overlap_begin >= level.table_infos.len() {
            return true;
        }
        // pick up files in L1 which are overlap with L0 to target level input.
        for table in &level.table_infos[overlap_begin..] {
            let end_range = KeyRange::from(table.key_range.as_ref().unwrap());
            if user_key(&end_range.left) > user_key(&tier_key_range.right) {
                break;
            }
            if level_handlers.is_pending_compact(&table.id) {
                return false;
            }
            input_ssts.push(table.clone());
        }
        true
    }

    fn pick_intra_l0_compaction(
        &self,
        level0: &Level,
        level0_handler: &mut LevelHandler,
    ) -> Option<SearchResult> {
        if level0.table_infos.len() < self.level0_max_file_number {
            return None;
        }

        let mut sst_idx = 0;
        while sst_idx < level0.table_infos.len() {
            if !level0_handler.is_pending_compact(&level0.table_infos[sst_idx].id) {
                break;
            }
            sst_idx += 1;
        }

        if level0.table_infos.len() - sst_idx < self.level0_trigger_number {
            return None;
        }

        let mut compaction_bytes = level0.table_infos[sst_idx].file_size;
        let mut select_level_inputs = vec![level0.table_infos[sst_idx].clone()];
        for table in level0.table_infos[sst_idx + 1..].iter() {
            if level0_handler.is_pending_compact(&table.id) {
                break;
            }
            if compaction_bytes >= self.max_compaction_bytes {
                break;
            }
            compaction_bytes += table.file_size;
            select_level_inputs.push(table.clone());
        }
        if select_level_inputs.len() < self.level0_trigger_number {
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
        let mut conflict_range = ConflictKeyRange::default();
        for idx in 0..select_level.table_infos.len() {
            if conflict_range
                .check_pending_compact(select_level_handler, &select_level.table_infos[idx])
            {
                continue;
            }

            if conflict_range.check_overlap(&select_level.table_infos[idx]) {
                break;
            }

            let mut tiered_key_range =
                KeyRange::from(select_level.table_infos[idx].key_range.as_ref().unwrap());
            let mut target_level_ssts = vec![];
            if !self.pick_target_level_overlap_files(
                &tiered_key_range,
                target_level,
                target_level_handler,
                &mut target_level_ssts,
            ) {
                break;
            }
            let mut select_level_ssts = vec![select_level.table_infos[idx].clone()];
            let mut compaction_bytes = target_level_ssts
                .iter()
                .map(|table| table.file_size)
                .sum::<u64>()
                + select_level_ssts[0].file_size;
            // try expand more L0 files if the currenct compaction job is too small.
            for other in &select_level.table_infos[idx + 1..] {
                if compaction_bytes >= self.max_compaction_bytes
                    || select_level_handler.is_pending_compact(&other.id)
                {
                    break;
                }
                if conflict_range.check_overlap(other) {
                    break;
                }
                // expand select range in L0.
                tiered_key_range
                    .full_key_extend(&KeyRange::from(other.key_range.as_ref().unwrap()));
                let mut new_target_level_ssts = vec![];
                if !self.pick_target_level_overlap_files(
                    &tiered_key_range,
                    target_level,
                    target_level_handler,
                    &mut new_target_level_ssts,
                ) {
                    return (select_level_ssts, target_level_ssts);
                }
                compaction_bytes += other.file_size;
                select_level_ssts.push(other.clone());
                target_level_ssts = new_target_level_ssts;
            }
            return (select_level_ssts, target_level_ssts);
        }
        (vec![], vec![])
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::hummock::KeyRange as RawKeyRange;

    use super::*;
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
        let picker = TierCompactionPicker::new(0);
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                // table_5_[1_10],
                // table_6_[2_20],
                table_infos: vec![generate_table(4, 1, 101, 300, 2)],
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
            .pick_compaction(levels.clone(), &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(levels_handler[1].get_pending_file_count(), 2);
        assert_eq!(ret.target_level.table_infos[0].id, 2);
        assert_eq!(ret.target_level.table_infos[1].id, 1);

        // no conflict with the last job
        levels[0]
            .table_infos
            .push(generate_table(5, 1, 301, 333, 4));
        let ret = picker
            .pick_compaction(levels.clone(), &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 3);
        assert_eq!(ret.target_level.table_infos[0].id, 0);

        // confict with the last job
        let mut picker = TierCompactionPicker::new(1);
        levels[0]
            .table_infos
            .push(generate_table(6, 1, 222, 233, 3));
        let ret = picker.pick_compaction(levels.clone(), &mut levels_handler);
        assert!(ret.is_none());

        // compact L0 to L0
        picker.level0_trigger_number = 2;
        picker.level0_max_file_number = 2;
        levels[0]
            .table_infos
            .push(generate_table(7, 1, 100, 200, 3));
        let ret = picker
            .pick_compaction(levels.clone(), &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.table_infos[0].id, 6);
        assert_eq!(ret.select_level.table_infos[1].id, 7);
        assert!(ret.target_level.table_infos.is_empty());
    }
}
