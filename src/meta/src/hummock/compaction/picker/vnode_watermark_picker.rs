// Copyright 2024 RisingWave Labs
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

use std::collections::BTreeMap;

use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::table_watermark::ReadTableWatermark;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{InputLevel, SstableInfo};

use crate::hummock::compaction::picker::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct VnodeWatermarkCompactionPicker {}

impl VnodeWatermarkCompactionPicker {
    pub fn new() -> Self {
        Self {}
    }

    pub fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        table_watermarks: &BTreeMap<TableId, ReadTableWatermark>,
    ) -> Option<CompactionInput> {
        let level = levels.levels.last()?;
        let mut select_input_ssts = vec![];
        for sst_info in &level.table_infos {
            if !level_handlers[level.level_idx as usize].is_pending_compact(&sst_info.sst_id)
                && should_delete_sst_by_watermark(sst_info, table_watermarks)
            {
                select_input_ssts.push(sst_info.clone());
            }
        }
        if select_input_ssts.is_empty() {
            return None;
        }
        Some(CompactionInput {
            select_input_size: select_input_ssts.iter().map(|sst| sst.file_size).sum(),
            total_file_count: select_input_ssts.len() as u64,
            input_levels: vec![
                InputLevel {
                    level_idx: level.level_idx,
                    level_type: level.level_type,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: level.level_idx,
                    level_type: level.level_type,
                    table_infos: vec![],
                },
            ],
            target_level: level.level_idx as usize,
            target_sub_level_id: level.sub_level_id,
            ..Default::default()
        })
    }
}

fn should_delete_sst_by_watermark(
    sst_info: &SstableInfo,
    table_watermarks: &BTreeMap<TableId, ReadTableWatermark>,
) -> bool {
    let left_key = FullKey::decode(&sst_info.key_range.as_ref().unwrap().left);
    let right_key = FullKey::decode(&sst_info.key_range.as_ref().unwrap().right);
    if left_key.user_key.table_id != right_key.user_key.table_id {
        return false;
    }
    let Some(watermarks) = table_watermarks.get(&left_key.user_key.table_id) else {
        return false;
    };
    should_delete_key_by_watermark(
        left_key.user_key.table_key.vnode_part(),
        left_key.user_key.table_key.key_part(),
        watermarks,
    ) && should_delete_key_by_watermark(
        right_key.user_key.table_key.vnode_part(),
        right_key.user_key.table_key.key_part(),
        watermarks,
    )
}

fn should_delete_key_by_watermark(
    key_vnode: VirtualNode,
    key: &[u8],
    watermark: &ReadTableWatermark,
) -> bool {
    let Some(w) = watermark.vnode_watermarks.get(&key_vnode) else {
        return false;
    };
    watermark.direction.filter_by_watermark(key, w)
}
