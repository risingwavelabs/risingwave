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

use itertools::Itertools;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_pb::hummock::level_handler::KeyRangeTaskId;
use risingwave_pb::hummock::SstableInfo;

#[derive(Clone, Debug, PartialEq)]
pub struct SSTableStat {
    pub key_range: KeyRange,
    pub table_id: u64,
    pub compact_task: Option<u64>,
}

impl From<&SstableInfo> for SSTableStat {
    fn from(info: &SstableInfo) -> Self {
        SSTableStat {
            key_range: info.key_range.as_ref().unwrap().into(),
            table_id: info.id,
            compact_task: None,
        }
    }
}

impl From<&SSTableStat> for risingwave_pb::hummock::SstableStat {
    fn from(stat: &SSTableStat) -> Self {
        risingwave_pb::hummock::SstableStat {
            key_range: Some(stat.key_range.clone().into()),
            table_id: stat.table_id,
            compact_task: stat
                .compact_task
                .map(|it| risingwave_pb::hummock::sstable_stat::CompactTaskId { id: it }),
        }
    }
}

impl From<&risingwave_pb::hummock::SstableStat> for SSTableStat {
    fn from(stat: &risingwave_pb::hummock::SstableStat) -> Self {
        SSTableStat {
            key_range: stat.key_range.as_ref().unwrap().into(),
            table_id: stat.table_id,
            compact_task: stat.compact_task.as_ref().map(|it| it.id),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum LevelHandler {
    /// * `Vec<SSTableStat>` - existing SSTs in this level, arranged in order no matter Tiering or
    ///   Leveling
    /// * `Vec<(KeyRange, u64, u64)>` - key ranges (and corresponding compaction task id, #SSTs) to
    ///   be merged to bottom level in order
    Nonoverlapping(Vec<SSTableStat>, Vec<(KeyRange, u64, u64)>),
    Overlapping(Vec<SSTableStat>, Vec<(KeyRange, u64, u64)>),
}

impl LevelHandler {
    fn clear_compacting_range(&mut self, clear_task_id: u64) {
        match self {
            LevelHandler::Overlapping(_, compacting_key_ranges)
            | LevelHandler::Nonoverlapping(_, compacting_key_ranges) => {
                compacting_key_ranges.retain(|(_, task_id, _)| *task_id != clear_task_id);
            }
        }
    }

    pub fn unassign_task(&mut self, unassign_task_id: u64) -> bool {
        self.clear_compacting_range(unassign_task_id);
        let mut changed = false;
        match self {
            LevelHandler::Overlapping(l_n, _) | LevelHandler::Nonoverlapping(l_n, _) => {
                for SSTableStat { compact_task, .. } in l_n {
                    if *compact_task == Some(unassign_task_id) {
                        *compact_task = None;
                        changed = true;
                    }
                }
            }
        }
        changed
    }

    pub fn pop_task_input(&mut self, finished_task_id: u64) -> Vec<u64> {
        self.clear_compacting_range(finished_task_id);

        let mut deleted_table_ids = vec![];
        let deleted_table_ids_ref = &mut deleted_table_ids;
        match self {
            LevelHandler::Overlapping(l_n, _) | LevelHandler::Nonoverlapping(l_n, _) => {
                l_n.retain(
                    |SSTableStat {
                         table_id,
                         compact_task,
                         ..
                     }| {
                        if *compact_task != Some(finished_task_id) {
                            true
                        } else {
                            deleted_table_ids_ref.push(*table_id);
                            false
                        }
                    },
                );
            }
        }
        deleted_table_ids
    }
}

impl From<&LevelHandler> for risingwave_pb::hummock::LevelHandler {
    fn from(lh: &LevelHandler) -> Self {
        let level_type = match lh {
            LevelHandler::Nonoverlapping(_, _) => risingwave_pb::hummock::LevelType::Nonoverlapping,
            LevelHandler::Overlapping(_, _) => risingwave_pb::hummock::LevelType::Overlapping,
        };
        match lh {
            LevelHandler::Nonoverlapping(ssts, key_ranges)
            | LevelHandler::Overlapping(ssts, key_ranges) => risingwave_pb::hummock::LevelHandler {
                level_type: level_type as i32,
                ssts: ssts
                    .iter()
                    .map_into::<risingwave_pb::hummock::SstableStat>()
                    .collect_vec(),
                key_ranges: key_ranges
                    .iter()
                    .map(|it| KeyRangeTaskId {
                        key_range: Some(it.0.clone().into()),
                        task_id: it.1,
                        ssts: it.2,
                    })
                    .collect(),
            },
        }
    }
}

impl From<&risingwave_pb::hummock::LevelHandler> for LevelHandler {
    fn from(lh: &risingwave_pb::hummock::LevelHandler) -> Self {
        let ssts = lh.ssts.iter().map_into::<SSTableStat>().collect();
        let key_ranges = lh
            .key_ranges
            .iter()
            .map(|it| (it.key_range.as_ref().unwrap().into(), it.task_id, it.ssts))
            .collect_vec();
        match lh.level_type() {
            risingwave_pb::hummock::LevelType::Nonoverlapping => {
                LevelHandler::Nonoverlapping(ssts, key_ranges)
            }
            risingwave_pb::hummock::LevelType::Overlapping => {
                LevelHandler::Overlapping(ssts, key_ranges)
            }
        }
    }
}
