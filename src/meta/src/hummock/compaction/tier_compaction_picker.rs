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
        let next_task_id = self.compact_task_id;
        if levels[0].table_infos.is_empty() {
            return None;
        }

        let select_sst_id = match self.select_first_l0_sst(&levels, level_handlers) {
            None => return self.pick_intra_l0_compaction(&levels[0], level_handlers),
            Some(idx) => idx,
        };

        let (prior, posterior) = level_handlers.split_at_mut(select_level as usize + 1);
        let (prior, posterior) = (prior.last_mut().unwrap(), posterior.first_mut().unwrap());
        let target_level = posterior.get_level();

        let (tier_key_range, select_level_inputs) =
            self.pickup_l0_files(&levels[select_level], prior, select_sst_id);
        let mut target_level_inputs = vec![];
        if !self.pick_target_level_overlap_files(
            &tier_key_range,
            &levels[target_level as usize],
            posterior,
            &mut target_level_inputs,
        ) {
            return self.pick_intra_l0_compaction(&levels[0], level_handlers);
        }

        prior.add_pending_task(next_task_id, &select_level_inputs);
        // Here, we have known that `select_level_input` is valid
        let mut splits = Vec::with_capacity(target_level_inputs.len());
        splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
        posterior.add_pending_task(next_task_id, &target_level_inputs);
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
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        if level0.table_infos.len() < self.level0_max_file_number {
            return None;
        }

        let mut sst_idx = 0;
        while sst_idx < level0.table_infos.len() {
            if !level_handlers[0].is_pending_compact(&level0.table_infos[sst_idx].id) {
                break;
            }
            sst_idx += 1;
        }
        if level0.table_infos.len() - sst_idx < self.level0_trigger_number {
            return None;
        }

        let (_, select_level_inputs) =
            self.pickup_l0_files(level0, &mut level_handlers[0], sst_idx);
        level_handlers[0].add_pending_task(self.compact_task_id, &select_level_inputs);
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

    fn pickup_l0_files(
        &self,
        level0: &Level,
        level0_handler: &mut LevelHandler,
        sst_idx: usize,
    ) -> (KeyRange, Vec<SstableInfo>) {
        let mut tier_key_range =
            KeyRange::from(level0.table_infos[sst_idx].key_range.as_ref().unwrap());
        let mut compaction_bytes = level0.table_infos[sst_idx].file_size;
        let mut select_tables = vec![level0.table_infos[sst_idx].clone()];
        for table in level0.table_infos[sst_idx + 1..].iter() {
            assert!(!level0_handler.is_pending_compact(&table.id));
            if compaction_bytes >= self.max_compaction_bytes {
                break;
            }
            compaction_bytes += table.file_size;
            let other_key_range = KeyRange::from(table.key_range.as_ref().unwrap());
            tier_key_range.full_key_extend(&other_key_range);
            select_tables.push(table.clone());
        }
        (tier_key_range, select_tables)
    }

    fn select_first_l0_sst(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<usize> {
        assert_eq!(levels[0].level_idx, 0);

        let mut sst_idx = None;
        for (idx, table) in levels[0].table_infos.iter().enumerate() {
            if !level_handlers[0].is_pending_compact(&table.id) {
                sst_idx = Some(idx);
                break;
            }
        }
        let first_idle_idx = match sst_idx {
            Some(idx) => idx,
            None => return None,
        };
        let tier_key_range = KeyRange::from(
            levels[0].table_infos[first_idle_idx]
                .key_range
                .as_ref()
                .unwrap(),
        );
        for i in 0..first_idle_idx {
            let table = &levels[0].table_infos[i];
            if tier_key_range.full_key_overlap(&KeyRange::from(table.key_range.as_ref().unwrap())) {
                return None;
            }
        }

        for table in &levels[1].table_infos {
            if tier_key_range.full_key_overlap(&KeyRange::from(table.key_range.as_ref().unwrap()))
                && level_handlers[1].is_pending_compact(&table.id)
            {
                return None;
            }
        }
        Some(first_idle_idx)
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
            .push(generate_table(7, 2, 100, 200, 3));
        let ret = picker
            .pick_compaction(levels.clone(), &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.table_infos[0].id, 6);
        assert_eq!(ret.select_level.table_infos[1].id, 7);
        assert!(ret.target_level.table_infos.is_empty());
    }
}
