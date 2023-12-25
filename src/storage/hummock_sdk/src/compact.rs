// Copyright 2023 RisingWave Labs
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

use risingwave_pb::hummock::{CompactTask, LevelType, SstableInfo};

pub fn compact_task_to_string(compact_task: &CompactTask) -> String {
    use std::fmt::Write;

    let mut s = String::new();
    writeln!(
        s,
        "Compaction task id: {:?}, group-id: {:?}, target level: {:?}, target sub level: {:?}",
        compact_task.task_id,
        compact_task.compaction_group_id,
        compact_task.target_level,
        compact_task.target_sub_level_id
    )
    .unwrap();
    writeln!(s, "Compaction watermark: {:?} ", compact_task.watermark).unwrap();
    writeln!(
        s,
        "Compaction target_file_size: {:?} ",
        compact_task.target_file_size
    )
    .unwrap();
    writeln!(s, "Compaction # splits: {:?} ", compact_task.splits.len()).unwrap();
    writeln!(
        s,
        "Compaction task status: {:?} ",
        compact_task.task_status()
    )
    .unwrap();
    writeln!(
        s,
        "Compaction task table_ids: {:?} ",
        compact_task.existing_table_ids,
    )
    .unwrap();
    s.push_str("Compaction Sstables structure: \n");
    for level_entry in &compact_task.input_ssts {
        let tables: Vec<String> = level_entry
            .table_infos
            .iter()
            .map(|table| {
                if table.total_key_count != 0 {
                    format!(
                        "[id: {}, obj_id: {} {}KB stale_ratio {} delete_range_ratio {}]",
                        table.get_sst_id(),
                        table.object_id,
                        table.file_size / 1024,
                        (table.stale_key_count * 100 / table.total_key_count),
                        (table.range_tombstone_count * 100 / table.total_key_count),
                    )
                } else {
                    format!(
                        "[id: {}, obj_id: {} {}KB]",
                        table.get_sst_id(),
                        table.object_id,
                        table.file_size / 1024,
                    )
                }
            })
            .collect();
        writeln!(s, "Level {:?} {:?} ", level_entry.level_idx, tables).unwrap();
    }
    s.push_str("Compaction task output: \n");
    for sst in &compact_task.sorted_output_ssts {
        append_sstable_info_to_string(&mut s, sst);
    }
    s
}

pub fn append_sstable_info_to_string(s: &mut String, sstable_info: &SstableInfo) {
    use std::fmt::Write;

    let key_range = sstable_info.key_range.as_ref().unwrap();
    let left_str = if key_range.left.is_empty() {
        "-inf".to_string()
    } else {
        hex::encode(key_range.left.as_slice())
    };
    let right_str = if key_range.right.is_empty() {
        "+inf".to_string()
    } else {
        hex::encode(key_range.right.as_slice())
    };

    let stale_ratio = (sstable_info.stale_key_count * 100)
        .checked_div(sstable_info.total_key_count)
        .unwrap_or(0);
    let range_tombstone_ratio = (sstable_info.range_tombstone_count * 100)
        .checked_div(sstable_info.total_key_count)
        .unwrap_or(0);
    writeln!(
        s,
        "SstableInfo: object id={}, SST id={}, KeyRange=[{:?},{:?}], table_ids: {:?}, size={}KB, stale_ratio={}%, range_tombstone_count={} range_tombstone_ratio={}% bloom_filter_kind {:?}",
        sstable_info.get_object_id(),
        sstable_info.get_sst_id(),
        left_str,
        right_str,
        sstable_info.table_ids,
        sstable_info.file_size / 1024,
        stale_ratio,
        sstable_info.range_tombstone_count,
        range_tombstone_ratio,
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
            total_file_size += sst.get_file_size();
            total_uncompressed_file_size += sst.get_uncompressed_file_size();
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
    support_streaming_upload: bool,
) -> u64 {
    let mut result = 0;
    // When building the SstableStreamIterator, sstable_syncable will fetch the SstableMeta and seek
    // to the specified block and build the iterator. Since this operation is concurrent, the memory
    // usage will need to take into account the size of the SstableMeta.
    // The common size of SstableMeta in tests is no more than 1m (mainly from xor filters).
    let mut max_meta_ratio = 0;

    // The memory usage of the SstableStreamIterator comes from SstableInfo with some state
    // information (use ESTIMATED_META_SIZE to estimate it), the BlockStream being read (one block),
    // and tcp recv_buffer_size.
    let max_input_stream_estimated_memory = block_size + recv_buffer_size;

    // input
    for level in &task.input_ssts {
        if level.level_type() == LevelType::Nonoverlapping {
            let mut meta_size = 0;
            for sst in &level.table_infos {
                meta_size = std::cmp::max(meta_size, sst.file_size - sst.meta_offset);
                max_meta_ratio = std::cmp::max(max_meta_ratio, meta_size * 100 / sst.file_size);
            }
            result += max_input_stream_estimated_memory + meta_size;
        } else {
            for sst in &level.table_infos {
                let meta_size = sst.file_size - sst.meta_offset;
                result += max_input_stream_estimated_memory + meta_size;
                max_meta_ratio = std::cmp::max(max_meta_ratio, meta_size * 100 / sst.file_size);
            }
        }
    }

    // output
    // builder will maintain SstableInfo + block_builder(block) + writer (block to vec)
    let estimated_meta_size = sst_capacity * max_meta_ratio / 100;
    if support_streaming_upload {
        result += estimated_meta_size + 2 * block_size
    } else {
        result += estimated_meta_size + sst_capacity; // Use sst_capacity to avoid BatchUploader
                                                      // memory bursts.
    }

    result
}
