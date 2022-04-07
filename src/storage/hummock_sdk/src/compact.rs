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

use risingwave_pb::hummock::CompactTask;

pub fn compact_task_to_string(compact_task: CompactTask) -> String {
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
        s.push_str(&format!(
            "Level {:?}: {:?} \n",
            level_entry.level_idx, level_entry.level
        ));
    }
    s.push_str(&format!(
        "Compaction task output: {:?} \n",
        compact_task.sorted_output_ssts
    ));

    s
}
