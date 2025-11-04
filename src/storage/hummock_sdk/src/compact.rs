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

use std::collections::HashSet;

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::LevelType;

use crate::compact_task::CompactTask;
use crate::sstable_info::SstableInfo;

pub fn compact_task_output_to_string(compact_task: &CompactTask) -> String {
    use std::fmt::Write;

    let mut s = String::default();
    writeln!(
        s,
        "Compaction task id: {:?}, group-id: {:?}, type: {:?}, target level: {:?}, target sub level: {:?} target_file_size: {:?}, splits: {:?}, status: {:?}",
        compact_task.task_id,
        compact_task.compaction_group_id,
        compact_task.task_type,
        compact_task.target_level,
        compact_task.target_sub_level_id,
        compact_task.target_file_size,
        compact_task.splits.len(),
        compact_task.task_status
    )
    .unwrap();
    s.push_str("Output: \n");
    for sst in &compact_task.sorted_output_ssts {
        append_sstable_info_to_string(&mut s, sst);
    }
    s
}

pub fn compact_task_to_string(compact_task: &CompactTask) -> String {
    use std::fmt::Write;

    let mut s = String::new();
    writeln!(
        s,
        "Compaction task id: {:?}, group-id: {:?}, type: {:?}, target level: {:?}, target sub level: {:?} target_file_size: {:?}, splits: {:?}",
        compact_task.task_id,
        compact_task.compaction_group_id,
        compact_task.task_type,
        compact_task.target_level,
        compact_task.target_sub_level_id,
        compact_task.target_file_size,
        compact_task.splits.len(),
    )
    .unwrap();
    s.push_str("Input: \n");
    let existing_table_ids: HashSet<TableId> =
        compact_task.existing_table_ids.iter().copied().collect();
    let mut input_sst_table_ids: HashSet<TableId> = HashSet::new();
    let mut dropped_table_ids = HashSet::new();
    for level_entry in &compact_task.input_ssts {
        let tables: Vec<String> = level_entry
            .table_infos
            .iter()
            .map(|table| {
                for tid in &table.table_ids {
                    if !existing_table_ids.contains(tid) {
                        dropped_table_ids.insert(tid);
                    } else {
                        input_sst_table_ids.insert(*tid);
                    }
                }
                if table.total_key_count != 0 {
                    format!(
                        "[id: {}, obj_id: {} object_size {}KB sst_size {}KB stale_ratio {}]",
                        table.sst_id,
                        table.object_id,
                        table.file_size / 1024,
                        table.sst_size / 1024,
                        (table.stale_key_count * 100 / table.total_key_count),
                    )
                } else {
                    format!(
                        "[id: {}, obj_id: {} object_size {}KB sst_size {}KB]",
                        table.sst_id,
                        table.object_id,
                        table.file_size / 1024,
                        table.sst_size / 1024,
                    )
                }
            })
            .collect();
        writeln!(s, "Level {:?} {:?} ", level_entry.level_idx, tables).unwrap();
    }
    if !compact_task.table_vnode_partition.is_empty() {
        writeln!(s, "Table vnode partition info:").unwrap();
        compact_task
            .table_vnode_partition
            .iter()
            .filter(|t| input_sst_table_ids.contains(t.0))
            .for_each(|(tid, partition)| {
                writeln!(s, " [{:?}, {:?}]", tid, partition).unwrap();
            });
    }

    if !dropped_table_ids.is_empty() {
        writeln!(s, "Dropped table_ids: {:?} ", dropped_table_ids).unwrap();
    }
    s
}

pub fn append_sstable_info_to_string(s: &mut String, sstable_info: &SstableInfo) {
    use std::fmt::Write;

    let key_range = &sstable_info.key_range;
    let left_str = if key_range.left.is_empty() {
        "-inf".to_owned()
    } else {
        hex::encode(&key_range.left)
    };
    let right_str = if key_range.right.is_empty() {
        "+inf".to_owned()
    } else {
        hex::encode(&key_range.right)
    };

    let stale_ratio = (sstable_info.stale_key_count * 100)
        .checked_div(sstable_info.total_key_count)
        .unwrap_or(0);
    writeln!(
        s,
        "SstableInfo: object id={}, SST id={}, KeyRange=[{:?},{:?}], table_ids: {:?}, object_size={}KB, sst_size={}KB stale_ratio={}%, bloom_filter_kind {:?}",
        sstable_info.object_id,
        sstable_info.sst_id,
        left_str,
        right_str,
        sstable_info.table_ids,
        sstable_info.file_size / 1024,
        sstable_info.sst_size / 1024,
        stale_ratio,
        sstable_info.bloom_filter_kind,
    )
    .unwrap();
}

pub fn statistics_compact_task(task: &CompactTask) -> CompactTaskStatistics {
    let mut total_key_count = 0;
    let mut total_file_count: u64 = 0;
    let mut total_file_size = 0;
    let mut total_uncompressed_file_size = 0;

    for level in &task.input_ssts {
        total_file_count += level.table_infos.len() as u64;

        level.table_infos.iter().for_each(|sst| {
            total_file_size += sst.file_size;
            total_uncompressed_file_size += sst.uncompressed_file_size;
            total_key_count += sst.total_key_count;
        });
    }

    CompactTaskStatistics {
        total_file_count,
        total_key_count,
        total_file_size,
        total_uncompressed_file_size,
    }
}

#[derive(Debug)]
pub struct CompactTaskStatistics {
    pub total_file_count: u64,
    pub total_key_count: u64,
    pub total_file_size: u64,
    pub total_uncompressed_file_size: u64,
}

pub fn estimate_memory_for_compact_task(
    task: &CompactTask,
    block_size: u64,
    recv_buffer_size: u64,
    sst_capacity: u64,
) -> u64 {
    let mut result = 0;
    // When building the SstableStreamIterator, sstable_syncable will fetch the SstableMeta and seek
    // to the specified block and build the iterator. Since this operation is concurrent, the memory
    // usage will need to take into account the size of the SstableMeta.
    // The common size of SstableMeta in tests is no more than 1m (mainly from xor filters).
    let mut task_max_sst_meta_ratio = 0;

    // The memory usage of the SstableStreamIterator comes from SstableInfo with some state
    // information (use ESTIMATED_META_SIZE to estimate it), the BlockStream being read (one block),
    // and tcp recv_buffer_size.
    let max_input_stream_estimated_memory = block_size + recv_buffer_size;

    // input
    for level in &task.input_ssts {
        if level.level_type == LevelType::Nonoverlapping {
            let mut cur_level_max_sst_meta_size = 0;
            for sst in &level.table_infos {
                let meta_size = sst.file_size - sst.meta_offset;
                task_max_sst_meta_ratio =
                    std::cmp::max(task_max_sst_meta_ratio, meta_size * 100 / sst.file_size);
                cur_level_max_sst_meta_size = std::cmp::max(meta_size, cur_level_max_sst_meta_size);
            }
            result += max_input_stream_estimated_memory + cur_level_max_sst_meta_size;
        } else {
            for sst in &level.table_infos {
                let meta_size = sst.file_size - sst.meta_offset;
                result += max_input_stream_estimated_memory + meta_size;
                task_max_sst_meta_ratio =
                    std::cmp::max(task_max_sst_meta_ratio, meta_size * 100 / sst.file_size);
            }
        }
    }

    // output
    // builder will maintain SstableInfo + block_builder(block) + writer (block to vec)
    let estimated_meta_size = sst_capacity * task_max_sst_meta_ratio / 100;

    // FIXME: sst_capacity is the upper bound of the memory usage of the streaming sstable uploader
    // A more reasonable memory limit method needs to be adopted, this is just a temporary fix.
    result += estimated_meta_size + sst_capacity;

    result
}
