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

use risingwave_pb::hummock::{HummockVersion, Level, SstableInfo};

use crate::prost_key_range::KeyRangeExt;
use crate::CompactionGroupId;

pub trait HummockVersionExt {
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Vec<Level>;
    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Vec<Level>;
    fn level_delete_ssts(
        operand: &mut Level,
        delete_sst_ids_superset: &HashSet<u64>,
        l0_remove_position: &mut Option<usize>,
    );
    fn level_insert_ssts(
        operand: &mut Level,
        new_table_infos: Vec<SstableInfo>,
        l0_remove_position: &Option<usize>,
    );
}

impl HummockVersionExt for HummockVersion {
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Vec<Level> {
        &self
            .levels
            .get(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id))
            .levels
    }

    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Vec<Level> {
        &mut self
            .levels
            .get_mut(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id))
            .levels
    }

    fn level_delete_ssts(
        operand: &mut Level,
        delete_sst_ids_superset: &HashSet<u64>,
        l0_remove_position: &mut Option<usize>,
    ) {
        let mut new_table_infos = Vec::with_capacity(operand.table_infos.len());
        let mut new_total_file_size = 0;
        for table_info in &operand.table_infos {
            if delete_sst_ids_superset.contains(&table_info.id) {
                if operand.level_idx == 0 && l0_remove_position.is_none() {
                    *l0_remove_position = Some(new_table_infos.len());
                }
            } else {
                new_total_file_size += table_info.file_size;
                new_table_infos.push(table_info.clone());
            }
        }
        operand.table_infos = new_table_infos;
        operand.total_file_size = new_total_file_size;
    }

    fn level_insert_ssts(
        operand: &mut Level,
        insert_table_infos: Vec<SstableInfo>,
        l0_remove_position: &Option<usize>,
    ) {
        operand.total_file_size += insert_table_infos
            .iter()
            .map(|sst| sst.file_size)
            .sum::<u64>();
        let mut l0_remove_position = *l0_remove_position;
        if operand.level_idx != 0 {
            l0_remove_position = None;
        }
        if let Some(l0_remove_pos) = l0_remove_position {
            let (l, r) = operand.table_infos.split_at_mut(l0_remove_pos);
            let mut new_table_infos = l.to_vec();
            new_table_infos.extend(insert_table_infos);
            new_table_infos.extend_from_slice(r);
            operand.table_infos = new_table_infos;
        } else {
            operand.table_infos.extend(insert_table_infos);
            if operand.level_idx != 0 {
                operand.table_infos.sort_by(|sst1, sst2| {
                    let a = sst1.key_range.as_ref().unwrap();
                    let b = sst2.key_range.as_ref().unwrap();
                    a.compare(b)
                });
            }
        }
    }
}
