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

use risingwave_pb::hummock::{CompactTask, SstableInfo};

pub fn compact_task_to_string(compact_task: &CompactTask) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "Compaction task id: {:?}, target level: {:?}\n",
        compact_task.task_id, compact_task.target_level
    ));
    s.push_str(&format!(
        "Compaction watermark: {:?} \n",
        compact_task.watermark
    ));
    s.push_str(&format!(
        "Compaction # splits: {:?} \n",
        compact_task.splits.len()
    ));
    s.push_str(&format!(
        "Compaction task status: {:?} \n",
        compact_task.task_status
    ));
    s.push_str("Compaction SSTables structure: \n");
    for level_entry in &compact_task.input_ssts {
        let tables: Vec<u64> = level_entry
            .table_infos
            .iter()
            .map(|table| table.id)
            .collect();
        s.push_str(&format!(
            "Level {:?}: {:?} \n",
            level_entry.level_idx, tables
        ));
    }
    s.push_str("Compaction task output: \n");
    for sst in &compact_task.sorted_output_ssts {
        append_sstable_info_to_string(&mut s, sst);
    }
    s
}

pub fn append_sstable_info_to_string(s: &mut String, sstable_info: &SstableInfo) {
    let key_range = sstable_info.key_range.as_ref().unwrap();
    let key_range_str = if key_range.inf {
        "(-inf, +inf)".to_owned()
    } else {
        format!(
            "[{}, {}]",
            hex::encode(key_range.left.as_slice()),
            hex::encode(key_range.right.as_slice())
        )
    };
    s.push_str(&format!(
        "SstableInfo: id={:?}, KeyRange={:?}, size={:?}\n",
        sstable_info.id, key_range_str, sstable_info.file_size
    ));
}
