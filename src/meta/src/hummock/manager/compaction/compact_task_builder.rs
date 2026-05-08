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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::table_watermark::{TableWatermarks, WatermarkSerdeType};
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::{CompactionConfig, TableOption, TableSchema};

use crate::hummock::compaction::CompactionTask as PickedCompactionTask;

pub(super) struct CompactTaskBuildContext {
    pub(super) task_id: HummockCompactionTaskId,
    pub(super) compaction_group_id: CompactionGroupId,
    pub(super) compaction_group_version_id: u64,
    pub(super) existing_table_ids: Vec<TableId>,
    pub(super) table_options: BTreeMap<TableId, TableOption>,
    pub(super) is_target_level_last: bool,
    pub(super) compaction_config: Arc<CompactionConfig>,
    pub(super) current_epoch_time: u64,
}

pub(super) fn build_base_compact_task(
    picked_task: PickedCompactionTask,
    context: CompactTaskBuildContext,
) -> (CompactTask, Vec<TableId>) {
    let target_level_id = picked_task.input.target_level as u32;
    let input_ssts = picked_task.input.input_levels;
    let compact_table_ids = input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .flat_map(|sst| sst.table_ids.iter().copied())
        .sorted()
        .dedup()
        .collect_vec();
    let compact_table_id_set: HashSet<_> = compact_table_ids.iter().copied().collect();
    let mut table_options = context.table_options;
    retain_table_options(&mut table_options, &compact_table_id_set);
    let compression_algorithm = match picked_task.compression_algorithm.as_str() {
        "Lz4" => 1,
        "Zstd" => 2,
        _ => 0,
    };
    let compaction_config = context.compaction_config.as_ref();

    (
        CompactTask {
            input_ssts,
            splits: vec![KeyRange::inf()],
            sorted_output_ssts: vec![],
            task_id: context.task_id,
            target_level: target_level_id,
            // Only gc delete keys in last level because there may be older versions in lower levels.
            gc_delete_keys: context.is_target_level_last,
            base_level: picked_task.base_level as u32,
            task_status: TaskStatus::Pending,
            compaction_group_id: context.compaction_group_id,
            compaction_group_version_id: context.compaction_group_version_id,
            existing_table_ids: context.existing_table_ids,
            compression_algorithm,
            target_file_size: picked_task.target_file_size,
            table_options,
            current_epoch_time: context.current_epoch_time,
            compaction_filter_mask: compaction_config.compaction_filter_mask,
            target_sub_level_id: picked_task.input.target_sub_level_id,
            task_type: picked_task.compaction_task_type,
            split_weight_by_vnode: picked_task.input.vnode_partition_count,
            max_sub_compaction: compaction_config.max_sub_compaction,
            max_kv_count_for_xor16: compaction_config.max_kv_count_for_xor16,
            max_vnode_key_range_bytes: compaction_config.max_vnode_key_range_bytes,
            ..Default::default()
        },
        compact_table_ids,
    )
}

pub(super) fn attach_compact_task_table_metadata(
    compact_task: &mut CompactTask,
    compact_table_ids: &[TableId],
    table_watermarks: BTreeMap<TableId, TableWatermarks>,
    all_versioned_table_schemas: &HashMap<TableId, Vec<i32>>,
) {
    let mut pk_prefix_table_watermarks = BTreeMap::default();
    let mut non_pk_prefix_table_watermarks = BTreeMap::default();
    let mut value_table_watermarks = BTreeMap::default();
    for (table_id, watermark) in table_watermarks {
        match watermark.watermark_type {
            WatermarkSerdeType::PkPrefix => {
                pk_prefix_table_watermarks.insert(table_id, watermark);
            }
            WatermarkSerdeType::NonPkPrefix => {
                non_pk_prefix_table_watermarks.insert(table_id, watermark);
            }
            WatermarkSerdeType::Value => {
                value_table_watermarks.insert(table_id, watermark);
            }
        }
    }

    compact_task.pk_prefix_table_watermarks = pk_prefix_table_watermarks;
    compact_task.non_pk_prefix_table_watermarks = non_pk_prefix_table_watermarks;
    compact_task.value_table_watermarks = value_table_watermarks;
    compact_task.table_schemas = compact_table_ids
        .iter()
        .filter_map(|table_id| {
            all_versioned_table_schemas.get(table_id).map(|column_ids| {
                (
                    *table_id,
                    TableSchema {
                        column_ids: column_ids.clone(),
                    },
                )
            })
        })
        .collect();
}

fn retain_table_options(
    table_options: &mut BTreeMap<TableId, TableOption>,
    table_id_set: &HashSet<TableId>,
) {
    table_options.retain(|table_id, _| table_id_set.contains(table_id));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retain_table_options() {
        let mut table_options = BTreeMap::from([
            (TableId::new(1), TableOption::default()),
            (TableId::new(2), TableOption::default()),
            (TableId::new(3), TableOption::default()),
        ]);
        let compact_table_id_set = HashSet::from_iter([TableId::new(1), TableId::new(3)]);

        retain_table_options(&mut table_options, &compact_table_id_set);

        assert_eq!(
            table_options.keys().copied().collect::<Vec<_>>(),
            vec![TableId::new(1), TableId::new(3)]
        );
    }
}
