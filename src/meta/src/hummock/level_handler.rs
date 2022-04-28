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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockSSTableId;
use risingwave_pb::hummock::level_handler::KeyRangeTaskId;
use risingwave_pb::hummock::SstableInfo;

#[derive(Clone, Debug, PartialEq)]
pub struct SSTableInfo {
    pub key_range: KeyRange,
    pub table_id: u64,
}

impl From<&SstableInfo> for SSTableInfo {
    fn from(sst: &SstableInfo) -> Self {
        Self {
            key_range: sst.key_range.as_ref().unwrap().into(),
            table_id: sst.id,
        }
    }
}

impl From<SSTableInfo> for SstableInfo {
    fn from(info: SSTableInfo) -> Self {
        SstableInfo {
            key_range: Some(info.key_range.into()),
            id: info.table_id,
        }
    }
}

impl From<&SSTableInfo> for SstableInfo {
    fn from(info: &SSTableInfo) -> Self {
        SstableInfo::from(info.clone())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum LevelHandler {
    /// * `HashMap<HummockSSTableId, u64>` - compaction task id of a SST.
    /// * `Vec<(KeyRange, u64, u64)>` - key ranges (and corresponding compaction task id, number of
    ///   SSTs) to be merged to bottom level in order
    Nonoverlapping(HashMap<HummockSSTableId, u64>, Vec<(KeyRange, u64, u64)>),
    Overlapping(HashMap<HummockSSTableId, u64>, Vec<(KeyRange, u64, u64)>),
}

impl LevelHandler {
    pub fn remove_task(&mut self, target_task_id: u64) {
        match self {
            LevelHandler::Overlapping(compacting_ssts, compacting_key_ranges)
            | LevelHandler::Nonoverlapping(compacting_ssts, compacting_key_ranges) => {
                compacting_ssts.retain(|_, task_id| *task_id != target_task_id);
                compacting_key_ranges.retain(|(_, task_id, _)| *task_id != target_task_id);
            }
        }
    }
}

impl From<&LevelHandler> for risingwave_pb::hummock::LevelHandler {
    fn from(lh: &LevelHandler) -> Self {
        let level_type = match lh {
            LevelHandler::Nonoverlapping(_, _) => risingwave_pb::hummock::LevelType::Nonoverlapping,
            LevelHandler::Overlapping(_, _) => risingwave_pb::hummock::LevelType::Overlapping,
        };
        match lh {
            LevelHandler::Nonoverlapping(compacting_ssts, key_ranges)
            | LevelHandler::Overlapping(compacting_ssts, key_ranges) => {
                risingwave_pb::hummock::LevelHandler {
                    level_type: level_type as i32,
                    compacting_ssts: compacting_ssts.clone(),
                    key_ranges: key_ranges
                        .iter()
                        .map(|it| KeyRangeTaskId {
                            key_range: Some(it.0.clone().into()),
                            task_id: it.1,
                            ssts: it.2,
                        })
                        .collect(),
                }
            }
        }
    }
}

impl From<&risingwave_pb::hummock::LevelHandler> for LevelHandler {
    fn from(lh: &risingwave_pb::hummock::LevelHandler) -> Self {
        let key_ranges = lh
            .key_ranges
            .iter()
            .map(|it| (it.key_range.as_ref().unwrap().into(), it.task_id, it.ssts))
            .collect_vec();
        match lh.level_type() {
            risingwave_pb::hummock::LevelType::Nonoverlapping => {
                LevelHandler::Nonoverlapping(lh.compacting_ssts.clone(), key_ranges)
            }
            risingwave_pb::hummock::LevelType::Overlapping => {
                LevelHandler::Overlapping(lh.compacting_ssts.clone(), key_ranges)
            }
        }
    }
}
